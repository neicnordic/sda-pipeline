# Local testing howto

## Starting the services using docker compose

To start all the backend services using docker compose.

```command
docker-compose -f compose-backend.yml up -d
```

To start one of the sda services using docker compose.

```command
docker-compose -f compose-sda.yml up -d ingest
```

## json formatted messages

In order to start the ingestion of the dummy datafile a message needs to be publised to the `files` routing key of the `localega` exhange either via the API or the webui.

```command
curl -vvv -u test:test 'localhost:15672/api/exchanges/test/localega/publish' -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"vhost":"test","name":"localega","properties":{"delivery_mode":2,"correlation_id":"1","content_encoding":"UTF-8","content_type":"application/json"},"routing_key":"files","payload":"{\"type\": \"ingest\", \"user\": \"test\", \"filepath\": \"test/dummy_data.c4gh\", \"encrypted_checksums\":[{\"type\":\"sha256\", \"value\":\"5e9c767958cc3f6e8d16512b8b8dcab855ad1e04e05798b86f50ef600e137578\", \"type\": \"md5\", \"value\": \"b60fa2486b121bed8d566bacec987e0d\"}]}","payload_encoding":"string"}'
```

### Message to start ingestion

```json
{
    "type": "ingest",
    "user": "test",
    "filepath": "test/dummy_data.c4gh",
    "encrypted_checksums": [
        { "type": "sha256", "value": "5e9c767958cc3f6e8d16512b8b8dcab855ad1e04e05798b86f50ef600e137578" },
        { "type": "md5", "value": "b60fa2486b121bed8d566bacec987e0d" }
    ]
}
```

### Example message to start verification

```json
{
    "user": "test",
    "filepath": "test/dummy_data.c4gh",
    "file_id": "1",
    "archive_path": "123e4567-e89b-12d3-a456-426614174000",
    "encrypted_checksums": [
        { "type": "sha256", "value": "5e9c767958cc3f6e8d16512b8b8dcab855ad1e04e05798b86f50ef600e137578" },
        { "type": "md5", "value": "b60fa2486b121bed8d566bacec987e0d" }
    ]
}
```

### Example message to finalize ingestion

```json
{
    "type": "ingest",
    "user": "test",
    "filepath": "test/dummy_data.c4gh",
    "accession_id": "EGAF00123456789",
    "decrypted_checksums": [
        { "type": "sha256", "value": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" },
        { "type": "md5", "value": "d41d8cd98f00b204e9800998ecf8427e" }
    ]
}
```
