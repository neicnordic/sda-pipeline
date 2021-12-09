#!/bin/bash

pip3 install s3cmd

cd dev_utils || exit 1

# Make buckets if they don't exist already 
s3cmd -c s3cmd-notls.conf mb s3://inbox || true
s3cmd -c s3cmd-notls.conf mb s3://archive || true

# Upload test file
s3cmd -c s3cmd-notls.conf put dummy_data.c4gh s3://inbox/dummy_data.c4gh
s3cmd -c s3cmd-notls.conf put largefile.c4gh s3://inbox/largefile.c4gh
s3cmd -c s3cmd-notls.conf put empty.c4gh s3://inbox/empty.c4gh
s3cmd -c s3cmd-notls.conf put truncated1.c4gh s3://inbox/truncated1.c4gh
s3cmd -c s3cmd-notls.conf put truncated2.c4gh s3://inbox/truncated2.c4gh
