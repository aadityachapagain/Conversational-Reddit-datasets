"""A Dataflow script for creating datasets from reddit.

For usage see README.md.
"""


import argparse
import hashlib
import json
import logging
import os
import re
import uuid
import random
from collections import defaultdict, namedtuple
from functools import partial

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import BigQuerySource, Read
from apache_beam.io.textio import WriteToText
from apache_beam.io.tfrecordio import WriteToTFRecord
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

_JSON_FORMAT = "JSON"

def _parse_args(argv=None):
    """Parse command line arguments."""

    def _positive_int(value):
        """Define a positive integer ArgumentParser type."""
        value = int(value)
        if value <= 0:
            raise argparse.ArgumentTypeError(
                "Value must be positive, {} was passed.".format(value))
        return value

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--reddit_table",
        required=True,
        help="The BigQuery table to read comments from, in "
             "project:table format.",
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        help="Google cloud storage output directory to write the dataset.",
    )
    parser.add_argument(
        "--dataset_format",
        choices={'TF', _JSON_FORMAT},
        default="TF",
        help="The dataset format to write. 'TF' for serialized tensorflow "
             "examples in TFRecords. 'JSON' for text files with one JSON "
             "object per line."
    )
    parser.add_argument(
        "--parent_depth",
        type=_positive_int,
        default=10,
        help="How many parent comments to consider.",
    )
    parser.add_argument(
        "--max_length",
        type=_positive_int,
        default=127,
        help="Maximum length of comments to include.",
    )
    parser.add_argument(
        "--min_length",
        type=_positive_int,
        default=9,
        help="Minimum length of comments to include.",
    )
    parser.add_argument(
        "--train_split",
        default=0.9, type=float,
        help="The proportion of data to put in the training set.",
    )
    parser.add_argument(
        "--num_shards_test",
        default=100,
        type=_positive_int,
        help="The number of shards for the test set.",
    )
    parser.add_argument(
        "--num_shards_train",
        default=1000,
        type=_positive_int,
        help="The number of shards for the train set.",
    )
    return parser.parse_known_args(argv)


# Represent a reddit comment.
Comment = namedtuple(
    "Comment",
    [
        "id",
        "thread_id",
        "parent_id",
        "body",
        "body_is_trimmed",
        "author",
        "subreddit",
    ]
)


def normalise_comment(comment, max_length):
    """Create a _Comment object from a row in the BigQuery table."""
    return Comment(
        id=comment['id'],
        thread_id=_normalise_id(comment['link_id']),
        parent_id=_normalise_id(comment['parent_id']),
        body=trim(comment['body'], max_length),
        body_is_trimmed=len(comment['body']) > max_length,
        author=comment['author'],
        subreddit=comment['subreddit'],
    )


def _normalise_id(raw_id):
    import re
    """Reddit IDs start with t1_, t2_, etc. which need to be stripped."""
    return re.sub("^t[0-9]_", "", raw_id)


def trim(text, max_length):
    """Trims text to be at most `max_length`, without splitting apart words."""
    if len(text) <= max_length:
        return text

    text = text[:max_length + 1]

    # Trim until the last two characters are the boundary between an
    # alphanumeric character, and a non-alphanumeric character.
    while len(text) > 1 and (text[-1].isalnum() == text[-2].isalnum()):
        text = text[:-1]

    return text[:-1]


def _should_skip(comment, min_length):
    if comment.body_is_trimmed:
        return True
    if comment.body in {"[deleted]", "[removed]"}:
        return True
    if len(comment.body) < min_length:
        return True
    return False


def create_examples(thread, parent_depth, min_length, format):
    """Creates serialized tensorflow examples from a reddit thread."""
    id_to_comment = {comment.id: comment for comment in list(thread)}

    for linear_path in linear_paths(id_to_comment, parent_depth):
        response = id_to_comment[linear_path[-1]]
        context = id_to_comment[linear_path[-2]]  # guaranteed to exist.

        if (_should_skip(response, min_length)
                or _should_skip(context, min_length)):
            continue

        example = {}
        example['subreddit'] = response.subreddit
        example['thread_id'] = response.thread_id
        example['context_author'] = context.author
        example['response_author'] = response.author
        example['context'] = context.body
        example['response'] = response.body

        for i in range(parent_depth - 1):
            # Extra contexts start at index -3.
            index = -3 - i
            try:
                context_i = linear_path[index]
            except IndexError:
                break

            example['context/{}'.format(i)] = id_to_comment[context_i].body

        yield example


def linear_paths(id_to_comment, parent_depth):
    """Gets all linear paths of comments and replies from the thread.

    Each linear path is guaranteed to have at least two comments in it.
    """
    paths = []
    seen_ids = set()
    id_to_children = defaultdict(list)
    for comment_id, comment in id_to_comment.items():
        id_to_children[comment.parent_id].append(comment_id)
        if comment.parent_id not in id_to_comment:
            paths.append([comment_id])
            seen_ids.add(comment_id)

    while paths:
        new_paths = []
        for path in paths:
            last_id = path[-1]
            for child_id in id_to_children[last_id]:
                if child_id in seen_ids:
                    # Prevent infinite loops.
                    continue
                seen_ids.add(child_id)
                new_path = path[-parent_depth:] + [child_id]
                new_paths.append(new_path)
                yield new_path
        paths = new_paths


def _shuffle(pcollection):
    """Shuffles the input pcollection."""
    pcollection |= "add random key" >> beam.Map(
        lambda value: (uuid.uuid4(), value))
    pcollection |= "group by key" >> beam.GroupByKey()
    pcollection |= "get shuffled values" >> beam.FlatMap(lambda t: t[1])
    return pcollection

def run(argv=None, comments=None):
    """Run the beam pipeline.

    Args:
        argv: (optional) the command line flags to parse.
        comments_collection: (optional) a list of comment JSON objects to
            process. Used in unit-tests to avoid requiring a BigQuery source.
    """
    args, pipeline_args = _parse_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    if comments is not None:
        comments = p | ("Read in-memory comments") >> beam.Create(comments)
    else:
        comments = p | ("Read " + args.reddit_table) >> Read(
            BigQuerySource(args.reddit_table))

    comments |= (
        "Normalise comments" >> beam.Map(
            partial(normalise_comment, max_length=args.max_length)))

    thread_id_to_comments = comments | (
        "Key by thread id" >> beam.Map(
            lambda comment: (comment.thread_id, comment)))
    threads = thread_id_to_comments | (
        "Group comments by thread ID" >> beam.GroupByKey())
    threads = threads | ("Get threads" >> beam.Map(lambda t: t[1]))

    examples = threads | (
        "Create {} examples".format(args.dataset_format) >> beam.FlatMap(
            partial(create_examples,
                    parent_depth=args.parent_depth,
                    min_length=args.min_length,
                    format=args.dataset_format,
                    )))
    examples = _shuffle(examples)

    # [START dataflow_molecules_split_to_train_and_eval_datasets]
    # Split the dataset into a training set and an evaluation set
    assert 0 < (100 - args.train_split*100) < 100, 'eval_percent must in the range (0-100)'
    eval_percent = 100 - args.train_split*100
    train_dataset, eval_dataset = (
        examples
        | 'Split dataset' >> beam.Partition(
            lambda elem, _: int(random.uniform(0, 100) < eval_percent), 2))
    # [END dataflow_molecules_split_to_train_and_eval_datasets]

    if args.dataset_format == _JSON_FORMAT:
        write_sink = WriteToText
        file_name_suffix = ".json"
        serialize_fn = json.dumps

    serialized_train_examples = train_dataset | (
        "serialize {} examples".format('train') >> beam.Map(serialize_fn))
    (
        serialized_train_examples | ("write " + 'train')
        >> write_sink(
            os.path.join(args.output_dir, 'train'),
            file_name_suffix=file_name_suffix,
            num_shards=args.num_shards_train,
        )
    )

    serialized_test_examples = eval_dataset | (
        "serialize {} examples".format('valid') >> beam.Map(serialize_fn))
    (
        serialized_test_examples | ("write " + 'valid')
        >> write_sink(
            os.path.join(args.output_dir, 'valid'),
            file_name_suffix=file_name_suffix,
            num_shards=args.num_shards_train,
        )
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
