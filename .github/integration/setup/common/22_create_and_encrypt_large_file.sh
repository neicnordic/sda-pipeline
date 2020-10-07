#!/bin/bash

cd dev_utils || exit 1

touch largefile.raw
shred -s 4G largefile.raw

md5sum largefile.raw > largefile.raw.md5
sha256sum largefile.raw > largefile.raw.sha256

crypt4gh encrypt --recipient_pk c4gh.pub.pem < largefile.raw > largefile.c4gh

rm largefile.raw

touch empty.c4gh

dd if=largefile.c4gh bs=1 count="$((15000+RANDOM))" of=truncated1.c4gh
dd if=largefile.c4gh bs=1 count=10 of=truncated2.c4gh
