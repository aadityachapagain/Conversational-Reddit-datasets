"""
Download and preprocess reddit datasets from pushshit.io
and store dataset in gcp
"""

import requests
from bs4 import BeautifulSoup
import os
from collections import defaultdict
import hashlib
import tqdm
import time
import logging
import shutil
import argparse
import bz2
import json
import lzma
import zstandard as zstd
import io
from langdetect import detect
from unmark import unmark
import re
import traceback
from gcp.gcs_service import GCP_Service
import multiprocessing
import random
import pandas as pd
from dask import dataframe as  dd
import swifter

logger = logging.getLogger("main")
FORMAT = '%(asctime)-15s %(name)s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT)
logger.setLevel("INFO")

data_ext = ['.bz2', '.xz','.zst']
reddit_link = "https://files.pushshift.io/reddit/submissions/"
datasets_link = defaultdict(lambda : {})
hash_link = 'https://files.pushshift.io/reddit/submissions/sha256sums.txt'


parser = argparse.ArgumentParser(description='download reddit datasets from pushshift.io')
parser.add_argument('--dpath', type=str,required= True,
                    help= 'destination path to download datasets')
parser.add_argument('--gcs-path', type=str,
                    help= 'destination path of gcp to store preprocessed data.')
parser.add_argument('--reddit-link', type=str,
                    default=reddit_link,
                    help= 'destination path of gcp to store preprocessed data.')
parser.add_argument('--hash-link', type=str, default=hash_link,
                    help= 'destination path of gcp to store preprocessed data.')

args = parser.parse_args()

"""
data preprocesss
* convert markup to plain text checked
* Remove comments/posts from Bots checked
* Remove comments/posts from non-English checked
* remove comments/posts marked as delete or removed checked
* remove comments/posts longer than 128 BPE tokens. will do this during loading data in model using tokenizer
* remove longer than 2040 characters and doesnot contain spaces. checked
* remove Shorter than 5 character. checked
* remove comments/posts with contains a URL. checked
* remove comments/posts starts with a non-ASCII. checked 
* remove comments further than depth 7 in the thread. since we are not pretraining we might not need this
* remove unsafe  posts and comments. checked

"""

def preprocess_handler(dpath: str):
    logger.info(f"pre-processing {dpath}")
    if dpath.lower().endswith('.bz2'):
        read_bz2_dataset(dpath)
    elif dpath.lower().endswith('.xz'):
        read_lzma_dataset(dpath)
    elif dpath.lower().endswith('.zst'):
        read_zstandered_data(dpath)
    else:
        logger.info("File not supported ... ")

    out_file = ''.join(dpath.split('.')[:-1]) +'.txt'
    logger.info(f"Done preprocessing {dpath} to {out_file}")
    return out_file

def find_url(string): 
  
    # findall() has been used  
    # with valid conditions for urls in string 
    regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
    url = re.findall(regex,string)       
    return [x[0] for x in url]

def preprocess_text(text):
    # remove mulitple spaces into single space
    text = re.sub('\s+',' ',text)
    # check if there is any url or not
    # check if text start with non-ASCII character
    cond = len(find_url(text)) > 0 or text.strip() == '' \
        or len(text.strip()) <= 5 or text.strip().lower() == '[deleted]' \
        or text.strip().lower() == '[removed]' or ord(text[0]) > 128 \
        or detect(text) != 'en'
    if cond:
        return False
    if ' ' not in text and len(text) > 2040:
        return False
    return text

def preprocess_data(data: str):
    try:
        data = json.loads(data)
        # check if sumbission is over 18 or not
        if data['over_18']:
            return False
        # convert markdown to plain text
        text_body = preprocess_text(unmark(data['selftext'].strip()))
        text_title = preprocess_text(unmark(data['title'].strip()))
        if text_body and text_title:
            return text_title + '\n' + text_body
        elif text_body:
            return text_body
        elif text_title:
            return text_title
        return False
    except:
        return False
    

def read_bz2_dataset(path):
    new_path = ''.join(path.split('.')[:-1]) +'.txt'
    with open(new_path, 'w') as fw:
        with bz2.open(path) as fp:
            text_stream = io.TextIOWrapper(fp, encoding='utf-8')
            while True:
                tmp = text_stream.readlines(10000000)
                if not tmp:
                    break
                df = pd.DataFrame()
                df['text'] = tmp
                ddf = dd.from_pandas(df, npartitions=2*multiprocessing.cpu_count())
                df['text'] =  ddf.map_partitions(lambda df:(df.apply(lambda x: preprocess_data(x['text']), axis=1))).compute(scheduler='processes')
                df['text'].swifter.apply(lambda x: fw.write(x + '\n') if x else None)
            text_stream.close()
                    

def read_lzma_dataset(path):
    new_path = ''.join(path.split('.')[:-1]) +'.txt'
    with open(new_path, 'w') as fw:
        with lzma.open(path) as fp:
            text_stream = io.TextIOWrapper(fp, encoding='utf-8')
            while True:
                tmp = text_stream.readlines(50000000)
                if not tmp:
                    break
                df = pd.DataFrame()
                df['text'] = tmp
                ddf = dd.from_pandas(df, npartitions=2*multiprocessing.cpu_count())
                df['text'] =  ddf.map_partitions(lambda df:(df.apply(lambda x: preprocess_data(x['text']), axis=1))).compute(scheduler='processes')
                df['text'].swifter.apply(lambda x: fw.write(x + '\n') if x else None)
            text_stream.close()

def read_zstandered_data(path):
    new_path = ''.join(path.split('.')[:-1]) +'.txt'
    with open(new_path, 'w') as fw:
        with open(path, 'rb') as fp:
            dctx = zstd.ZstdDecompressor()
            stream_reader = dctx.stream_reader(fp)
            text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')
            while True:
                tmp = text_stream.readlines(50000000)
                if not tmp:
                    break
                df = pd.DataFrame()
                df['text'] = tmp
                ddf = dd.from_pandas(df, npartitions=2*multiprocessing.cpu_count())
                df['text'] =  ddf.map_partitions(lambda df:(df.apply(lambda x: preprocess_data(x['text']), axis=1))).compute(scheduler='processes')
                df['text'].swifter.apply(lambda x: fw.write(x + '\n') if x else None)
            text_stream.close()
            stream_reader.close()
            del dctx

def download(url, path, fname, redownload=False, num_retries=5):
    """
    Download file using `requests`.

    If ``redownload`` is set to false, then will not download tar file again if it is
    present (default ``False``).
    """
    outfile = os.path.join(path, fname)
    if not os.path.isdir(os.path.dirname(outfile)):
        os.makedirs(os.path.dirname(outfile))
    download = not os.path.isfile(outfile) or redownload
    logger.info(f"Downloading {url} to {outfile}")
    retry = num_retries
    exp_backoff = [2 ** r for r in reversed(range(retry))]

    pbar = tqdm.tqdm(unit='B', unit_scale=True, desc='Downloading {}'.format(fname))

    while download and retry > 0:
        resume_file = outfile + '.part'
        resume = os.path.isfile(resume_file)
        if resume:
            resume_pos = os.path.getsize(resume_file)
            mode = 'ab'
        else:
            resume_pos = 0
            mode = 'wb'
        response = None

        with requests.Session() as session:
            try:
                header = (
                    {'Range': 'bytes=%d-' % resume_pos, 'Accept-Encoding': 'identity'}
                    if resume
                    else {}
                )
                response = session.get(url, stream=True, timeout=5, headers=header)

                # negative reply could be 'none' or just missing
                if resume and response.headers.get('Accept-Ranges', 'none') == 'none':
                    resume_pos = 0
                    mode = 'wb'

                CHUNK_SIZE = 32768
                total_size = int(response.headers.get('Content-Length', -1))
                # server returns remaining size if resuming, so adjust total
                total_size += resume_pos
                pbar.total = total_size
                done = resume_pos

                with open(resume_file, mode) as f:
                    for chunk in response.iter_content(CHUNK_SIZE):
                        if chunk:  # filter out keep-alive new chunks
                            f.write(chunk)
                        if total_size > 0:
                            done += len(chunk)
                            if total_size < done:
                                # don't freak out if content-length was too small
                                total_size = done
                                pbar.total = total_size
                            pbar.update(len(chunk))
                    break
            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout,
            ):
                retry -= 1
                pbar.clear()
                if retry > 0:
                    pl = 'y' if retry == 1 else 'ies'
                    logger.debug(
                        f'Connection error, retrying. ({retry} retr{pl} left)'
                    )
                    time.sleep(exp_backoff[retry])
                else:
                    logger.error('Retried too many times, stopped retrying.')
            finally:
                if response:
                    response.close()
    if retry <= 0:
        raise RuntimeError('Connection broken too many times. Stopped retrying.')

    if download and retry > 0:
        pbar.update(done - pbar.n)
        if done < total_size:
            raise RuntimeError(
                f'Received less data than specified in Content-Length header for '
                f'{url}. There may be a download problem.'
            )
        move(resume_file, outfile)

    pbar.close()
    return outfile

def move(path1, path2):
    """
    Rename the given file.
    """
    shutil.move(path1, path2)


class DownloadableFile:
    """
    A class used to abstract any file that has to be downloaded online.
    """

    def __init__(self, url, file_name, hashcode, zipped=True, from_google=False):
        self.url = url
        self.file_name = file_name
        self.hashcode = hashcode
        self.compressed = zipped
        self.from_google = from_google

    def checksum(self, dpath):
        """
        Checksum on a given file.

        :param dpath: path to the downloaded file.
        """
        sha256_hash = hashlib.sha256()
        with open(os.path.join(dpath, self.file_name), "rb") as f:
            for byte_block in iter(lambda: f.read(65536), b""):
                sha256_hash.update(byte_block)
            if sha256_hash.hexdigest() != self.hashcode:
                # remove_dir(dpath)
                raise AssertionError(
                    f"[ Checksum for {self.file_name} from \n{self.url}\n"
                    "does not match the expected checksum. Please try again. ]"
                )
            else:
                logger.debug("Checksum Successful")

    def download_file(self, dpath):
        out_file = download(self.url, dpath, self.file_name)

        if self.hashcode:
            self.checksum(dpath)
        
        return out_file

def collect_hash():
    res = requests.get(hash_link)
    hashes = res.content.decode("utf-8").strip()
    for hash_to_file in hashes.split('\n'):
        hash_to_file = hash_to_file.strip().split()
        datasets_link[hash_to_file[1]]['hash'] = hash_to_file[0]

def is_recommended_link(link):
    for ext in data_ext:
        if link.endswith(ext):
            return link
    return False

def get_all_downloadable_links():
    res = requests.get(reddit_link)
    content = BeautifulSoup(res.content, 'html5lib')
    for link in content.find_all('a'):
        _link = link.get('href')
        _link = is_recommended_link(_link)
        if _link:
            _link = os.path.split(_link)[-1]
            datasets_link[_link]['link'] = os.path.join(reddit_link, _link)


def distributed_download(download_batch: dict):
    for k in random.sample(list(download_batch.keys()), k=len(list(download_batch.keys()))):
        v = download_batch[k]
        if v.get('link', False):
            fd = DownloadableFile(
                v['link'], k, None
            )
            if args.gcs_path:
                gcs_files = gcp.list_files(args.gcs_path)
            target_gcs_file = os.path.join(args.gcs_path, ''.join(k.split('.')[:-1]) + '.txt')
            if target_gcs_file in gcs_files:
                logger.info(f'{k} file is already preprocessed !')
                continue
            outfile = fd.download_file(args.dpath)
            outfile = preprocess_handler(outfile)
            file_name = os.path.split(outfile)[-1]
            if args.gcs_path:
                gcs_path = os.path.join(args.gcs_path, file_name)
                gcp.upload_from_filename(outfile, gcs_path)

if __name__ == "__main__":
    reddit_link = args.reddit_link
    hash_link = args.hash_link
    gcp = GCP_Service()
    collect_hash()
    download_path = args.dpath
    get_all_downloadable_links()
    distributed_download(datasets_link)