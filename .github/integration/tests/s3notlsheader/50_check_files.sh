#!/bin/bash

cd dev_utils || exit 1

chmod 600 certs/client-key.pem

echo "Checking archive files in s3"

# Earlier tests verify that the file is in the database correctly

accessids=$(docker run --rm --name client --network dev_utils_default \
	neicnordic/pg-client:latest postgresql://lega_in:lega_in@db:5432/lega \
	-t -A -c "SELECT stable_id FROM local_ega.files where status='READY';")

if [ -z "$accessids" ]; then
	echo "Failed to get accession ids"
	exit 1
fi

for aid in $accessids; do
	echo "Checking accesssionid $aid"

	apath=$(docker run --rm --name client --network dev_utils_default \
		neicnordic/pg-client:latest postgresql://lega_in:lega_in@db:5432/lega \
		-t -A -c "SELECT archive_path FROM local_ega.files where stable_id='$aid';")

	acheck=$(docker run --rm --name client --network dev_utils_default \
		neicnordic/pg-client:latest postgresql://lega_in:lega_in@db:5432/lega \
		-t -A -c "SELECT archive_file_checksum FROM local_ega.files where stable_id='$aid';")

	achecktype=$(docker run --rm --name client --network dev_utils_default \
		neicnordic/pg-client:latest postgresql://lega_in:lega_in@db:5432/lega \
		-t -A -c "SELECT archive_file_checksum_type FROM local_ega.files where stable_id='$aid';")

	decrcheck=$(docker run --rm --name client --network dev_utils_default \
		neicnordic/pg-client:latest postgresql://lega_in:lega_in@db:5432/lega \
		-t -A -c "SELECT decrypted_file_checksum FROM local_ega.files where stable_id='$aid';")

	s3cmd -c s3cmd-notls.conf get "s3://archive/$apath" "tmp/$apath"

	if [ "${achecktype,,*}" = 'md5sum' ]; then
		filecheck=$(md5sum "tmp/$apath" | cut -d' ' -f1)
	else
		filecheck=$(sha256sum "tmp/$apath" | cut -d' ' -f1)
	fi

	if [ "$filecheck" != "$acheck" ]; then
		echo "Checksum archive failure for file with stable id $aid ($filecheck vs $acheck)"
		exit 1
	fi

	rm -f "tmp/$apath"

	# Check checksum for backuped copy as well
	s3cmd -c s3cmd-notls.conf get "s3://backup/$apath" "tmp/$apath"

	# Decrypt the file
	crypt4gh decrypt --sk c4gh-new.sec.pem < "tmp/$apath" > "tmp/$apath-decr"

	if [ "${achecktype,,*}" = 'md5sum' ]; then
		filecheck=$(md5sum "tmp/$apath-decr" | cut -d' ' -f1)
	else
		filecheck=$(sha256sum "tmp/$apath-decr" | cut -d' ' -f1)
	fi

	if [ "$filecheck" != "$decrcheck" ]; then
		echo "Checksum backup failure for file with stable id $aid ($filecheck vs $decrcheck)"
		exit 1
	fi

	rm -f "tmp/$apath" "tmp/$apath-decr"
done

echo "Passed check for archive and backup (s3 with header, no tls)"
