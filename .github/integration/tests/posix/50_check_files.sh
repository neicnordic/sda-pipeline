#!/bin/bash

cd dev_utils || exit 1

echo "Checking archive files in backup"

# Earlier tests verify that the file is in the database correctly

accessids=$(docker run --rm --name client --network dev_utils_default \
	    neicnordic/pg-client:latest postgresql://lega_in:lega_in@db:5432/lega \
            -t -A -c "SELECT stable_id FROM local_ega.files where status='READY';")

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

    mkdir -p tmp/
    docker cp "ingest:/tmp/$apath" tmp/

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
    docker cp "sync:/backup/$apath" tmp/

    if [ "${achecktype,,*}" = 'md5sum' ]; then 
	filecheck=$(md5sum "tmp/$apath" | cut -d' ' -f1)
    else
	filecheck=$(sha256sum "tmp/$apath" | cut -d' ' -f1)
    fi

    if [ "$filecheck" != "$acheck" ]; then
	echo "Checksum backup failure for file with stable id $aid ($filecheck vs $acheck)"
	exit 1
    fi

    rm -f "tmp/$apath"
done

echo "Passed check for archive and backup (posix)"
