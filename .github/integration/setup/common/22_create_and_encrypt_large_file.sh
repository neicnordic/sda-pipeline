#!/bin/bash

cd dev_utils || exit 1

touch largefile.raw
size=$(echo "$RANDOM" '*' "$RANDOM" '*' 5 + "$RANDOM" | bc)
shred -n 1 -s "$size" largefile.raw

md5sum largefile.raw > largefile.raw.md5
sha256sum largefile.raw > largefile.raw.sha256

crypt4gh encrypt --recipient_pk c4gh.pub.pem < largefile.raw > largefile.c4gh

rm largefile.raw

touch empty.c4gh

dd if=largefile.c4gh bs=1 count="$((15000+RANDOM))" of=truncated1.c4gh
dd if=largefile.c4gh bs=1 count=10 of=truncated2.c4gh

dd if=/dev/random of=wrongly_encrypted.raw count=1 bs=$(( 1024 * 1024 *  1 )) iflag=fullblock

cat > wrong_key.pub <<EOF
-----BEGIN CRYPT4GH PUBLIC KEY-----
fGi6kvZlQN37PLmumygbNaSLf0NA+kj4KB1b70HFFCU=
-----END CRYPT4GH PUBLIC KEY-----
EOF

crypt4gh encrypt --recipient_pk wrong_key.pub < wrongly_encrypted.raw > wrongly_encrypted.c4gh
rm wrongly_encrypted.raw wrong_key.pub