#!/bin/sh

for cmd in s3cmd jq openssl
do
    if ! command -v "$cmd" >/dev/null 2>&1
    then
        printf '%s could not be found\n' "$cmd" >&2
        exit 1
    fi
done

infile=${1:-$(dirname "$0")/dummy_data.c4gh}
if [ ! -f "$infile" ]; then
    printf 'Unable to find regular file "%s"\n' "$infile" >&2
    exit 1
fi
infile=$( realpath "$infile" )

cd "$(dirname "$0")" || exit

# we need this certificate to be of 600 to work with db connection
chmod 600 certs/client-key.pem

SHA=$(openssl sha256 "$infile" | sed 's/.* //')
MD5=$(openssl md5    "$infile" | sed 's/.* //')

s3object=test/$(basename "$infile")
s3cmd -c s3cmd.conf put "$infile" "s3://inbox/$s3object"

encrypted_checksums=$( jq -c -n \
    --arg sha256 "$SHA" \
    --arg md5 "$MD5" \
    '$ARGS.named|to_entries|map(with_entries(select(.key=="key").key="type"))'
)

payload_string=$( jq -c -n \
    --arg operation upload \
    --arg user test \
    --arg filepath "$s3object" \
    --argjson encrypted_checksums "$encrypted_checksums" \
    '$ARGS.named|tostring'
)

properties=$( jq -c -n \
    --argjson delivery_mode 2 \
    --arg correlation_id 1 \
    --arg content_encoding UTF-8 \
    --arg content_type application/json \
    '$ARGS.named'
)

request_body=$( jq -c -n \
    --arg vhost test \
    --arg name sda \
    --argjson properties "$properties" \
    --arg routing_key inbox \
    --arg payload_encoding string \
    --argjson payload "$payload_string" \
    '$ARGS.named'
)

# invoke ingestion for standalone (orchestrator)
curl -vvv --cacert certs/ca.pem --user test:test \
    --header 'Content-Type: application/json;charset=UTF-8' \
    --data-binary "$request_body" \
    'https://localhost:15672/api/exchanges/test/sda/publish'

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
