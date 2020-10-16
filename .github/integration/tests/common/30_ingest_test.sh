#!/bin/bash

cd dev_utils

count=1

for file in dummy_data.c4gh largefile.c4gh; do
	    
    now=$(date -u +%Y-%m-%dT%H:%M:%SZ)

    md5sum=$(md5sum "$file" | cut -d' ' -f 1)
    sha256sum=$(sha256sum "$file" | cut -d' ' -f 1)

    C4GH_PASSPHRASE=$(grep -F passphrase config.yaml | sed -e 's/.* //' -e 's/"//g')
    export C4GH_PASSPHRASE

    decsha256sum=$(crypt4gh decrypt --sk c4gh.sec.pem < "$file" | sha256sum | cut -d' ' -f 1)
    decmd5sum=$(crypt4gh decrypt --sk c4gh.sec.pem < "$file" | md5sum | cut -d' ' -f 1)
    
    curl -vvv -u test:test 'localhost:15672/api/exchanges/test/localega/publish' \
          -H 'Content-Type: application/json;charset=UTF-8' \
          --data-binary "$( echo '{
                          "vhost":"test",
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
                                      \"filepath\":\"/FILENAME\",
                                      \"encrypted_checksums\":[{
                                                                \"type\":\"sha256\",
                                                                \"value\":\"SHA256SUM\"},
                                                               {
                                                                \"type\":\"md5\",
                                                                \"value\":\"MD5SUM\"
                                                               }
                                                              ]
                                     }"
                         }' | sed -e "s/FILENAME/$file/" -e "s/MD5SUM/${md5sum}/" -e "s/SHA256SUM/${sha256sum}/" )"


    RETRY_TIMES=0
    until docker logs ingest --since="$now" 2>&1 | grep "Mark as archived"
    do echo "waiting for ingestion to complete"
       RETRY_TIMES=$((RETRY_TIMES+1));
       if [ "$RETRY_TIMES" -eq 60 ]; then
	   docker logs ingest
	   exit 1
       fi
       sleep 10
    done

    RETRY_TIMES=0
    until docker logs verify --since="$now" 2>&1 | grep "Mark completed"
    do echo "waiting for verification to complete"
       RETRY_TIMES=$((RETRY_TIMES+1));
       if [ "$RETRY_TIMES" -eq 60 ]; then
	   docker logs verify
	   exit 1
       fi
       sleep 10
    done

    now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    access=$(printf "EGAF%011d" "$count" )
    


    archivepath=$(curl -u test:test 'localhost:15672/api/queues/test/files.verified/get' \
		   -H 'Content-Type: application/json;charset=UTF-8' \
		   -d '{"count":1,"ackmode":"ack_requeue_true","encoding":"auto","truncate":50000}' | \
		  jq -r '.[0]["payload"]' |  jq -r '.["filepath"]'
   )

    
    # Publish stable id
    curl -vvv -u test:test 'localhost:15672/api/exchanges/test/localega/publish' \
	 -H 'Content-Type: application/json;charset=UTF-8' \
	 --data-binary "$( echo '{
                     "vhost":"test",
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
                                 \"filepath\":\"FILENAME\",
                                 \"accession_id\":\"ACCESSIONID\",
                                 \"decrypted_checksums\":[
                                                          {
                                                           \"type\":\"sha256\",
                                                           \"value\":\"DECSHA256SUM\"
                                                          },
                                                          {
                                                           \"type\":\"md5\",
                                                           \"value\":\"DECMD5SUM\"
                                                          }
                                                         ]
                                }"
                    }'| sed -e "s/FILENAME/$archivepath/" -e "s/DECMD5SUM/${decmd5sum}/" -e "s/DECSHA256SUM/${decsha256sum}/" -e "s/ACCESSIONID/$access/"  )"

    RETRY_TIMES=0
    until docker logs finalize --since="$now" 2>&1 | grep "Mark ready"
    do echo "waiting for finalize to complete"
       RETRY_TIMES=$((RETRY_TIMES+1));
       if [ $RETRY_TIMES -eq 60 ]; then
	   docker logs finalize
	   exit 1
       fi
       sleep 10
    done
    
    count=$((count+1))
done
