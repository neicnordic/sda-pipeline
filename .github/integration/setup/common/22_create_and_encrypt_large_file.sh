#!/bin/bash

cd dev_utils || exit 1

touch largefile.raw
shred -s 4G largefile.raw

md5sum /largefile.raw > largefile.raw.md5
sha256sum largefile.raw > largefile.raw.sha256

crypt4gh  --recipient_pk dev_utils/c4gh.pub.pem < largefile.raw > largefile.c4gh

rm largefile.raw


