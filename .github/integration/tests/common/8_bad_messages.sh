#!/bin/bash

#
# Submit some messages that will trigger various failures. Do this
# before the "real" work to verify that these failures are not top of
# queue and handled again and again.
#

for routingkey in files archived verified completed accessionIDs; do
	curl --cacert dev_utils/certs/ca.pem -vvv -u test:test 'https://localhost:15672/api/exchanges/test/sda/publish' \
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
						"routing_key":"'"$routingkey"'",
						"payload_encoding":"string",
						"payload":"{
						I give you bad json!}"
					}'
done

for routingkey in files archived verified completed accessionIDs; do
	curl --cacert dev_utils/certs/ca.pem -vvv -u test:test 'https://localhost:15672/api/exchanges/test/sda/publish' \
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
						"routing_key":"'"$routingkey"'",
						"payload_encoding":"string",
						"payload":"{ \"json\":\"yes, but not sda\" }"
					}'
done
