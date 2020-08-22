#! /bin/sh

set -e -x

: ${PIP:=pip}

$PIP install requirements.txt