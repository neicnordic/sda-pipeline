#!/bin/bash

pip3 install s3cmd

cd dev_utils || exit 1

chmod 600 certs/client-key.pem

now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
C4GH_PASSPHRASE=$(grep -F passphrase config.yaml | sed -e 's/.* //' -e 's/"//g')
export C4GH_PASSPHRASE

md5sum=$(md5sum largefile.c4gh  | cut -d' ' -f 1)
sha256sum=$(sha256sum largefile.c4gh | cut -d' ' -f 1)

dcf=$(mktemp)

decsha256sum=$(crypt4gh decrypt --sk c4gh.sec.pem <largefile.c4gh  | LANG=C dd bs=4M 2>"$dcf" | sha256sum | cut -d' ' -f 1)
decmd5sum=$(crypt4gh decrypt --sk c4gh.sec.pem <largefile.c4gh  | md5sum | cut -d' ' -f 1)

rm -f "$dcf"

## case cancel message arrives before ingestion has started

docker pause ingest

curl --cacert certs/ca.pem -v -u test:test 'https://localhost:15672/api/exchanges/test/sda/publish' \
	-H 'Content-Type: application/json;charset=UTF-8' \
	--data-binary "$(echo '{"vhost":"test", "name":"sda",
							"properties":{
								"delivery_mode":2,
								"correlation_id":"1001",
								"content_encoding":"UTF-8",
								"content_type":"application/json"
							},
							"routing_key":"files",
							"payload_encoding":"string",
							"payload":"{\"type\":\"ingest\",\"user\":\"test1\",\"filepath\":\"largefile.c4gh\",\"encrypted_checksums\":[
								{\"type\":\"sha256\",\"value\":\"SHA256SUM\"},
								{\"type\":\"md5\",\"value\":\"MD5SUM\"}
							]}"
							}' | sed -e "s/MD5SUM/${md5sum}/" -e "s/SHA256SUM/${sha256sum}/")"

curl --cacert certs/ca.pem -v -u test:test 'https://localhost:15672/api/exchanges/test/sda/publish' \
	-H 'Content-Type: application/json;charset=UTF-8' \
	--data-binary "$(echo '{"vhost":"test", "name":"sda",
							"properties":{
								"delivery_mode":2,
								"correlation_id":"1002",
								"content_encoding":"UTF-8",
								"content_type":"application/json"
							},
							"routing_key":"files",
							"payload_encoding":"string",
							"payload":"{\"type\":\"cancel\",\"user\":\"test1\",\"filepath\":\"largefile.c4gh\",\"encrypted_checksums\":[
								{\"type\":\"sha256\",\"value\":\"SHA256SUM\"},
								{\"type\":\"md5\",\"value\":\"MD5SUM\"}
							]}"
							}' | sed -e "s/MD5SUM/${md5sum}/" -e "s/SHA256SUM/${sha256sum}/")"

status=$(docker run --rm --name client --network dev_utils_default -v "$PWD/certs:/certs" \
	-e PGSSLCERT=/certs/client.pem -e PGSSLKEY=/certs/client-key.pem -e PGSSLROOTCERT=/certs/ca.pem \
	neicnordic/pg-client:latest postgresql://lega_in:lega_in@db:5432/lega \
	-t -A -c "SELECT status from local_ega.files where inbox_path='largefile.c4gh' and elixir_id='test1';")

if [ "$status" != "DISABLED" ]; then
	echo "cancel before ingestion failed, expected DISABLED got: $status"
	docker logs intercept --since="$now"
	exit 1
fi

docker unpause ingest

RETRY_TIMES=0
until docker logs ingest --since="$now" 2>&1 | grep "File is DISABLED"; do
	echo "waiting for ingestion to be canceled"
	RETRY_TIMES=$((RETRY_TIMES + 1))
	if [ "$RETRY_TIMES" -eq 60 ]; then
		echo "::error::Time out while waiting for ingest to be canceled, logs:"
		docker logs --since="$now" ingest
		exit 1
	fi
	sleep 10
done

## case cancel message arrives after ingestion has started

s3cmd -c s3cmd.conf put largefile.c4gh s3://inbox/largefile.c4gh

now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
curl --cacert certs/ca.pem -v -u test:test 'https://localhost:15672/api/exchanges/test/sda/publish' \
	-H 'Content-Type: application/json;charset=UTF-8' \
	--data-binary "$(echo '{"vhost":"test", "name":"sda",
							"properties":{
								"delivery_mode":2,
								"correlation_id":"2001",
								"content_encoding":"UTF-8",
								"content_type":"application/json"
							},
							"routing_key":"files",
							"payload_encoding":"string",
							"payload":"{\"type\":\"ingest\",\"user\":\"test2\",\"filepath\":\"largefile.c4gh\",\"encrypted_checksums\":[
								{\"type\":\"sha256\",\"value\":\"SHA256SUM\"},
								{\"type\":\"md5\",\"value\":\"MD5SUM\"}
							]}"
							}' | sed -e "s/MD5SUM/${md5sum}/" -e "s/SHA256SUM/${sha256sum}/")"

RETRY_TIMES=0
until docker logs ingest --since="$now" 2>&1 | grep "Received work (corr-id: 2001"; do
	echo "waiting for ingestion to start"
	RETRY_TIMES=$((RETRY_TIMES + 1))
	if [ "$RETRY_TIMES" -eq 60 ]; then
		echo "::error::Time out while waiting for ingest to start, logs:"
		docker logs --since="$now" ingest
		exit 1
	fi
	sleep 1
done

curl --cacert certs/ca.pem -v -u test:test 'https://localhost:15672/api/exchanges/test/sda/publish' \
	-H 'Content-Type: application/json;charset=UTF-8' \
	--data-binary "$(echo '{"vhost":"test", "name":"sda",
							"properties":{
								"delivery_mode":2,
								"correlation_id":"2002",
								"content_encoding":"UTF-8",
								"content_type":"application/json"
							},
							"routing_key":"files",
							"payload_encoding":"string",
							"payload":"{\"type\":\"cancel\",\"user\":\"test2\",\"filepath\":\"largefile.c4gh\",\"encrypted_checksums\":[
								{\"type\":\"sha256\",\"value\":\"SHA256SUM\"},
								{\"type\":\"md5\",\"value\":\"MD5SUM\"}
							]}"
							}' | sed -e "s/MD5SUM/${md5sum}/" -e "s/SHA256SUM/${sha256sum}/")"

RETRY_TIMES=0
until docker logs ingest --since="$now" 2>&1 | grep "file is DISABLED, reverting changes"; do
	echo "waiting for ingestion to abort"
	RETRY_TIMES=$((RETRY_TIMES + 1))
	if [ "$RETRY_TIMES" -eq 60 ]; then
		echo "::error::Time out while waiting for ingest to abort, logs:"
		docker logs --since="$now" ingest
		exit 1
	fi
	sleep 10
done
