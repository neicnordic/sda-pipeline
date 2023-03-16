#!/bin/sh

for cmd in s3cmd jq openssl
do
    if ! command -v "$cmd" >/dev/null 2>&1
    then
        printf '%s could not be found\n' "$cmd" >&2
        exit 1
    fi
done

# we need this certificate to be of 600 to work with db connection
chmod 600 certs/client-key.pem

FILE="dummy_data.c4gh"
SHA="5e9c767958cc3f6e8d16512b8b8dcab855ad1e04e05798b86f50ef600e137578"
MD5="b60fa2486b121bed8d566bacec987e0d"

if [ "$1" != "" ]; then
    SHA=$(sha256sum "$1" | awk '{print $1;}')
    MD5=$(md5sum "$1" | awk '{print $1;}')
    FILE="$(realpath "$1")"
fi

file=test/$(basename "$FILE")

cd "$(dirname "$0")" || exit

s3cmd -c s3cmd.conf put "$FILE" s3://inbox/"$file"


# invoke ingestion for standalone (orchestrator)
curl --cacert certs/ca.pem -vvv -u test:test 'https://localhost:15672/api/exchanges/test/sda/publish' \
-H 'Content-Type: application/json;charset=UTF-8' \
--data-binary '{"vhost":"test","name":"sda","properties":{"delivery_mode":2,"correlation_id":"1","content_encoding":"UTF-8","content_type":"application/json"},"routing_key":"inbox","payload_encoding":"string","payload":"{\"operation\":\"upload\",\"user\":\"test\",\"filepath\":\"'"$file"'\",\"encrypted_checksums\":[{\"type\":\"sha256\",\"value\":\"'"$SHA"'\",\"type\":\"md5\",\"value\":\"'"$MD5"'\"}]}"}'


RETRY_TIMES=0
until docker logs mapper 2>&1 | grep "Mapped file to dataset"
do
    echo "waiting for mapper to complete"
    RETRY_TIMES=$((RETRY_TIMES+1));
    if [ $RETRY_TIMES -eq 30 ]; then
        echo "Mapping failed"
        exit 1
    fi
    sleep 10;
done

# check that the file's dataset appeared in the db.
dataset=$(docker run --rm --name client --network dev_utils_default -v "$PWD/certs:/certs" \
	-e PGSSLCERT=/certs/client.pem -e PGSSLKEY=/certs/client-key.pem -e PGSSLROOTCERT=/certs/ca.pem \
	neicnordic/pg-client:latest postgresql://lega_out:lega_out@db:5432/lega \
-c "SELECT * from local_ega_ebi.file_dataset" | grep urn:neic:test)
if [ ${#dataset} -eq 0 ]; then
    echo "Failure"
    exit 1
else
    echo "Success"
fi
