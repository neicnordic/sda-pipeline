#!/bin/bash

cd dev_utils || exit 1
exit 0
#
# Submit some messages that will trigger various failures. Do this
# before the "real" work to verify that these failures are not top of
# queue and handled again and again.
#

# Helper function that checks if msg is moved to error queue
# admits a string arg that is part of the output error msg

function check_move_to_error_queue() {
	now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
	echo $now
	RETRY_TIMES=0
	echo
	echo "Waiting for msg containing \""$1"\" to move to error queue."
	until curl --cacert certs/ca.pem  -u test:test 'https://localhost:15672/api/queues/test/error/get' \
		-H 'Content-Type: application/json;charset=UTF-8' \
		-d '{"count":1,"ackmode":"ack_requeue_true","encoding":"auto","truncate":50000}' 2>&1 | grep -q "$1"; do
		printf '%s' "."
		RETRY_TIMES=$((RETRY_TIMES + 1))
		if [ $RETRY_TIMES -eq 61 ]; then
			echo "::error::Time out while waiting for msg to move to error queue, logs:"
			echo
			echo ingest
			echo
			docker logs --since="$now" ingest
			exit 1
		fi
		sleep 2
	done
	echo
	echo "Message with \""$1"\" moved to error queue."
}
# Submit a file encrypted with the wrong key

md5sum=$(md5sum wrongly_encrypted.c4gh | cut -d' ' -f 1)
sha256sum=$(sha256sum wrongly_encrypted.c4gh | cut -d' ' -f 1)

curl --cacert certs/ca.pem  -vvv -u test:test 'https://localhost:15672/api/exchanges/test/sda/publish' \
     -H 'Content-Type: application/json;charset=UTF-8' \
     --data-binary "$( echo '{
    	                       "vhost":"test",
                               "name":"sda",
    	                       "properties":{
    	                                     "delivery_mode":2,
    	                                     "correlation_id":"1",
    	                                     "content_encoding":"UTF-8",
    	                                     "content_type":"application/json"
    	                                    },
    	                       "routing_key":"files",
    	                       "payload_encoding":"string",
    	                       "payload":"{
    	                                   \"type\":\"ingest\",
    	                                   \"user\":\"test\",
    	                                   \"filepath\":\"/wrongly_encrypted.c4gh\",
    	                                   \"encrypted_checksums\":[{
    	                                                             \"type\":\"sha256\",
    	                                                             \"value\":\"SHA256SUM\"},
    	                                                            {
    	                                                             \"type\":\"md5\",
    	                                                             \"value\":\"MD5SUM\"
    	                                                            }
    	                                                           ]
    	                                  }"
    	                      }' | sed -e "s/SHA256SUM/${sha256sum}/" -e "s/MD5SUM/${md5sum}/" )"

# Verify that message is moved to the error queue

check_move_to_error_queue "decryption failed"

# Submit a non-existent file

curl --cacert certs/ca.pem  -vvv -u test:test 'https://localhost:15672/api/exchanges/test/sda/publish' \
     -H 'Content-Type: application/json;charset=UTF-8' \
     --data-binary '{
    	                       "vhost":"test",
                               "name":"sda",
    	                       "properties":{
    	                                     "delivery_mode":2,
    	                                     "correlation_id":"1",
    	                                     "content_encoding":"UTF-8",
    	                                     "content_type":"application/json"
    	                                    },
    	                       "routing_key":"files",
    	                       "payload_encoding":"string",
    	                       "payload":"{
    	                                   \"type\":\"ingest\",
    	                                   \"user\":\"test\",
    	                                   \"filepath\":\"/THISFILEDOESNOTEXIST\",
    	                                   \"encrypted_checksums\":[{
    	                                                             \"type\":\"sha256\",
    	                                                             \"value\":\"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\"},
    	                                                            {
    	                                                             \"type\":\"md5\",
    	                                                             \"value\":\"d41d8cd98f00b204e9800998ecf8427e\"
    	                                                            }
    	                                                           ]
    	                                  }"
    	                      }'


# Verify message put in error here once https://github.com/neicnordic/sda-pipeline/issues/130 is resolved.

curl --cacert certs/ca.pem  -u test:test 'https://localhost:15672/api/queues/test/error/get' \
		   -H 'Content-Type: application/json;charset=UTF-8' \
		   -d '{"count":1,"ackmode":"ack_requeue_true","encoding":"auto","truncate":50000}'

# Submit an existing file but incorrect checksum

curl --cacert certs/ca.pem  -vvv -u test:test 'https://localhost:15672/api/exchanges/test/sda/publish' \
     -H 'Content-Type: application/json;charset=UTF-8' \
     --data-binary '{
    	                       "vhost":"test",
                               "name":"sda",
    	                       "properties":{
    	                                     "delivery_mode":2,
    	                                     "correlation_id":"1",
    	                                     "content_encoding":"UTF-8",
    	                                     "content_type":"application/json"
    	                                    },
    	                       "routing_key":"files",
    	                       "payload_encoding":"string",
    	                       "payload":"{
    	                                   \"type\":\"ingest\",
    	                                   \"user\":\"test\",
    	                                   \"filepath\":\"/dummy_data.c4gh\",
    	                                   \"encrypted_checksums\":[{
    	                                                             \"type\":\"sha256\",
    	                                                             \"value\":\"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\"},
    	                                                            {
    	                                                             \"type\":\"md5\",
    	                                                             \"value\":\"d41d8cd98f00b204e9800998ecf8427e\"
    	                                                            }
    	                                                           ]
    	                                  }"
    	                      }'


# Verify message put in error here once https://github.com/neicnordic/sda-pipeline/issues/130 is resolved.

curl --cacert certs/ca.pem  -u test:test 'https://localhost:15672/api/queues/test/error/get' \
		   -H 'Content-Type: application/json;charset=UTF-8' \
		   -d '{"count":1,"ackmode":"ack_requeue_true","encoding":"auto","truncate":50000}'


# Submit a truncated file (with correct checksum)

md5sum=$(md5sum truncated1.c4gh | cut -d' ' -f 1)
sha256sum=$(sha256sum truncated1.c4gh | cut -d' ' -f 1)

curl --cacert certs/ca.pem  -vvv -u test:test 'https://localhost:15672/api/exchanges/test/sda/publish' \
     -H 'Content-Type: application/json;charset=UTF-8' \
     --data-binary "$( echo '{
    	                       "vhost":"test",
                               "name":"sda",
    	                       "properties":{
    	                                     "delivery_mode":2,
    	                                     "correlation_id":"1",
    	                                     "content_encoding":"UTF-8",
    	                                     "content_type":"application/json"
    	                                    },
    	                       "routing_key":"files",
    	                       "payload_encoding":"string",
    	                       "payload":"{
    	                                   \"type\":\"ingest\",
    	                                   \"user\":\"test\",
    	                                   \"filepath\":\"/truncated1.c4gh\",
    	                                   \"encrypted_checksums\":[{
    	                                                             \"type\":\"sha256\",
    	                                                             \"value\":\"SHA256SUM\"},
    	                                                            {
    	                                                             \"type\":\"md5\",
    	                                                             \"value\":\"MD5SUM\"
    	                                                            }
    	                                                           ]
    	                                  }"
    	                      }' | sed -e "s/SHA256SUM/${sha256sum}/" -e "s/MD5SUM/${md5sum}/" )"

# Verify message put in error here once https://github.com/neicnordic/sda-pipeline/issues/130 is resolved.

curl --cacert certs/ca.pem  -u test:test 'https://localhost:15672/api/queues/test/error/get' \
		   -H 'Content-Type: application/json;charset=UTF-8' \
		   -d '{"count":1,"ackmode":"ack_requeue_true","encoding":"auto","truncate":50000}'


# Submit a smaller truncated file (with correct checksum)

md5sum=$(md5sum truncated2.c4gh | cut -d' ' -f 1)
sha256sum=$(sha256sum truncated2.c4gh | cut -d' ' -f 1)

curl --cacert certs/ca.pem  -vvv -u test:test 'https://localhost:15672/api/exchanges/test/sda/publish' \
     -H 'Content-Type: application/json;charset=UTF-8' \
     --data-binary "$( echo '{
    	                       "vhost":"test",
                               "name":"sda",
    	                       "properties":{
    	                                     "delivery_mode":2,
    	                                     "correlation_id":"1",
    	                                     "content_encoding":"UTF-8",
    	                                     "content_type":"application/json"
    	                                    },
    	                       "routing_key":"files",
    	                       "payload_encoding":"string",
    	                       "payload":"{
    	                                   \"type\":\"ingest\",
    	                                   \"user\":\"test\",
    	                                   \"filepath\":\"/truncated2.c4gh\",
    	                                   \"encrypted_checksums\":[{
    	                                                             \"type\":\"sha256\",
    	                                                             \"value\":\"SHA256SUM\"},
    	                                                            {
    	                                                             \"type\":\"md5\",
    	                                                             \"value\":\"MD5SUM\"
    	                                                            }
    	                                                           ]
    	                                  }"
    	                      }' | sed -e "s/SHA256SUM/${sha256sum}/" -e "s/MD5SUM/${md5sum}/" )"

# Verify message put in error here once https://github.com/neicnordic/sda-pipeline/issues/130 is resolved.

curl --cacert certs/ca.pem  -u test:test 'https://localhost:15672/api/queues/test/error/get' \
		   -H 'Content-Type: application/json;charset=UTF-8' \
		   -d '{"count":1,"ackmode":"ack_requeue_true","encoding":"auto","truncate":50000}'


# Cleanup cueues
curl --cacert certs/ca.pem  -u test:test -X DELETE 'https://localhost:15672/api/queues/test/completed/contents'
curl --cacert certs/ca.pem  -u test:test -X DELETE 'https://localhost:15672/api/queues/test/verified/contents'
