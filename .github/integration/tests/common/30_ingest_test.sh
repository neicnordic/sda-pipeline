#!/bin/bash

now=$(date -u +%Y-%m-%dT%H:%M:%SZ)

curl -vvv -u test:test 'localhost:15672/api/exchanges/test/localega/publish' \
          -H 'Content-Type: application/json;charset=UTF-8' \
          --data-binary '{"vhost":"test",
	                  "name":"localega",
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
                                                                \"value\":\"5e9c767958cc3f6e8d16512b8b8dcab855ad1e04e05798b86f50ef600e137578\"},
                                                               {
                                                                \"type\":\"md5\",
                                                                \"value\":\"b60fa2486b121bed8d566bacec987e0d\"
                                                               }
                                                              ]
                                     }"
                         }'


RETRY_TIMES=0
until docker logs ingest --since="$now" 2>&1 | grep "Mark as archived"
do echo "waiting for ingestion to complete"
   RETRY_TIMES=$((RETRY_TIMES+1));
   if [ "$RETRY_TIMES" -eq 6 ]; then
       docker logs ingest
       exit 1
   fi
   sleep 10
done


RETRY_TIMES=0
until docker logs verify --since="$now" 2>&1 | grep "Mark completed"
do echo "waiting for verification to complete"
   RETRY_TIMES=$((RETRY_TIMES+1));
   if [ "$RETRY_TIMES" -eq 6 ]; then
       docker logs verify
       exit 1
   fi
   sleep 10
done

now=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# Publish stable id
curl -vvv -u test:test 'localhost:15672/api/exchanges/test/localega/publish' \
     -H 'Content-Type: application/json;charset=UTF-8' \
     --data-binary '{"vhost":"test",
                     "name":"localega",
                     "properties":{
                           "delivery_mode":2,
                            "correlation_id":"1",
                            "content_encoding":"UTF-8",
                            "content_type":"application/json"
                                  },
                     "routing_key":"stableIDs",
                     "payload_encoding":"string",
                     "payload":"{
                                 \"type\":\"accession\",
                                 \"user\":\"test\",
                                 \"filepath\":\"/dummy_data.c4gh\",
                                 \"accession_id\":\"EGAF00123456789\",
                                 \"decrypted_checksums\":[
                                                          {
                                                           \"type\":\"sha256\",
                                                           \"value\":\"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\"
                                                          },
                                                          {
                                                           \"type\":\"md5\",
                                                           \"value\":\"d41d8cd98f00b204e9800998ecf8427e\"
                                                          }
                                                         ]
                                }"
                    }'

RETRY_TIMES=0
until docker logs finalize --since="$now" 2>&1 | grep "Mark ready"
do echo "waiting for finalize to complete"
   RETRY_TIMES=$((RETRY_TIMES+1));
   if [ $RETRY_TIMES -eq 6 ]; then
       docker logs finalize
       exit 1
   fi
   sleep 10
done

