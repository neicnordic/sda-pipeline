#!/bin/bash

cd dev_utils || exit 1
exit 0
#
# Submit some messages that will trigger various failures. Do this
# before the "real" work to verify that these failures are not top of
# queue and handled again and again.
#


curl --cacert dev_utils/certs/ca.pem  -vvv -u test:test 'https://localhost:15672/api/exchanges/test/sda/publish' \
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

curl --cacert dev_utils/certs/ca.pem  -u test:test 'https://localhost:15672/api/queues/test/error/get' \
		   -H 'Content-Type: application/json;charset=UTF-8' \
		   -d '{"count":1,"ackmode":"ack_requeue_true","encoding":"auto","truncate":50000}'



# Submit an existing file but incorrect checksum

curl --cacert dev_utils/certs/ca.pem  -vvv -u test:test 'https://localhost:15672/api/exchanges/test/sda/publish' \
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

curl --cacert dev_utils/certs/ca.pem  -u test:test 'https://localhost:15672/api/queues/test/error/get' \
		   -H 'Content-Type: application/json;charset=UTF-8' \
		   -d '{"count":1,"ackmode":"ack_requeue_true","encoding":"auto","truncate":50000}'


# Submit a truncated file (with correct checksum)

md5sum=$(md5sum truncated1.c4gh | cut -d' ' -f 1)
sha256sum=$(sha256sum truncated1.c4gh | cut -d' ' -f 1)
	 
curl --cacert dev_utils/certs/ca.pem  -vvv -u test:test 'https://localhost:15672/api/exchanges/test/sda/publish' \
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

curl --cacert dev_utils/certs/ca.pem  -u test:test 'https://localhost:15672/api/queues/test/error/get' \
		   -H 'Content-Type: application/json;charset=UTF-8' \
		   -d '{"count":1,"ackmode":"ack_requeue_true","encoding":"auto","truncate":50000}'


# Submit a smaller truncated file (with correct checksum)

md5sum=$(md5sum truncated2.c4gh | cut -d' ' -f 1)
sha256sum=$(sha256sum truncated2.c4gh | cut -d' ' -f 1)
	 
curl --cacert dev_utils/certs/ca.pem  -vvv -u test:test 'https://localhost:15672/api/exchanges/test/sda/publish' \
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

curl --cacert dev_utils/certs/ca.pem  -u test:test 'https://localhost:15672/api/queues/test/error/get' \
		   -H 'Content-Type: application/json;charset=UTF-8' \
		   -d '{"count":1,"ackmode":"ack_requeue_true","encoding":"auto","truncate":50000}'


# Cleanup cueues
curl --cacert dev_utils/certs/ca.pem  -u test:test -X DELETE 'https://localhost:15672/api/queues/test/completed/contents'
curl --cacert dev_utils/certs/ca.pem  -u test:test -X DELETE 'https://localhost:15672/api/queues/test/verified/contents'
