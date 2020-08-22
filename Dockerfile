FROM ubuntu:20.04
RUN apt-get update
RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y python3 python3-pip curl

COPY build.py build.sh unmark.py ./
COPY gcp gcp

RUN PIP=pip3 ./build.sh

CMD ["python3", "build.py", "--gcs-path", "reddit_datasets", "--dpath", "/tmp/reddit_datasets/"]