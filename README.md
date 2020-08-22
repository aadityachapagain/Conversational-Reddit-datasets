# Create the conversational dataset
Below are instructions for how to generate the reddit dataset.

## Getting Started

Conversational datasets are created using [Apache Beam pipeline](https://beam.apache.org/) scripts, run on [Google Dataflow](https://cloud.google.com/dataflow/). This parallelises the data processing pipeline across many worker machines. Apache Beam requires python >= 3.6, so you will need to set up a python => 3.6 virtual environment:

The Dataflow scripts write conversational datasets to Google cloud storage, so you will need to [create a bucket](https://cloud.google.com/storage/docs/creating-buckets) to save the dataset to.

Dataflow will run workers on multiple Compute Engine instances, so make sure you have a sufficient [quota](https://cloud.google.com/dataflow/quotas) of `n1-standard-1` machines. The READMEs for individual datasets give an idea of how many workers are required, and how long each dataflow job should take.

And you will need to [set up authentication](https://cloud.google.com/docs/authentication/getting-started) by creating a service account with access to Dataflow and Cloud Storage, and set `GOOGLE_APPLICATION_CREDENTIALS`:

```bash
export GOOGLE_APPLICATION_CREDENTIALS={{json file key location}}
```

## Create the BigQuery input table

Reddit comment data is stored as a public BigQuery dataset, partitioned into months: [`fh-bigquery:reddit_comments.YYYY_MM`](https://console.cloud.google.com/bigquery?p=fh-bigquery&d=reddit_comments&page=dataset). The first step in creating the dataset is to create a single table that contains all the comment data to include.

First, [install the bq command-line tool](https://cloud.google.com/bigquery/docs/bq-command-line-tool).

Ensure you have a BigQuery dataset to write the table to:

```bash
DATASET="data"
bq mk --dataset ${DATASET?}
```

Write a new table by querying the public reddit data:

```bash
TABLE=reddit

# For all data up to 2019.
TABLE_REGEX="^201[5678]_[01][0-9]$"

QUERY="SELECT * \
  FROM TABLE_QUERY(\
  [fh-bigquery:reddit_comments], \
  \"REGEXP_MATCH(table_id, '${TABLE_REGEX?}')\" )"

# Run the query.
echo "${QUERY?}" | bq query \
  --n 0 \
  --batch --allow_large_results \
  --destination_table ${DATASET?}.${TABLE?} \
  --use_legacy_sql=true
```

## Run the dataflow script

[`create_data.py`](create_data.py) is a [Google Dataflow](https://cloud.google.com/dataflow/) script that reads the input BigQuery table and saves the dataset to Google Cloud Storage.


Now you can run the Dataflow script:

```bash
PROJECT="your-google-cloud-project"
BUCKET="your-bucket"

DATADIR="gs://${BUCKET?}/reddit/$(date +"%Y%m%d")"

# The below uses values of $DATASET and $TABLE set
# in the previous section.

python reddit/create_data.py \
  --output_dir ${DATADIR?} \
  --reddit_table ${PROJECT?}:${DATASET?}.${TABLE?} \
  --runner DataflowRunner \
  --temp_location ${DATADIR?}/temp \
  --staging_location ${DATADIR?}/staging \
  --project ${PROJECT?} \
  --dataset_format JSON
```
Once the above is running, you can continue to monitor it in the terminal, or quit the process and follow the running job on the
[dataflow admin page](https://console.cloud.google.com/dataflow).

The dataset will be saved in the `$DATADIR` directory, as sharded train and test sets- `gs://your-bucket/reddit/YYYYMMDD/train-*-of-01000.json` and
`gs://your-bucket/reddit/YYYYMMDD/test-*-of-00100.json`.

## Using your own machine to download datasets

Incase if you don't have any gcp projects then you may download reddit datasets from [PushShift](https://reddit.pushshit.io/) website by just running following command but it takes forever to download and preprocess. So, I wouldn't suggest you do this.

```bash
python build.py --dpath <download_path> --reddit-link <"pushshit.io link which contains all the datasets"> --hash-link <"link for hash file.txt">