# sda-pipeline: mapper

The mapper service registers mapping of accessionIDs (stable ids for files) to datasetIDs.

## Service Description
The mapper service maps file accessionIDs to datasetIDs.

When running, mapper reads messages from the configured RabbitMQ queue (default: "mappings").
For each message, these steps are taken (if not otherwise noted, errors halt progress and the service moves on to the next message):

1.  The message is validated as valid JSON that matches the "dataset-mapping" schema (defined in sda-common).
If the message can’t be validated it is discarded with an error message in the logs.

1. AccessionIDs from the message are mapped to a datasetID (also in the message) in the database.
On failure an error message is written to the logs, but processing is not halted.

1. The RabbitMQ message is Ack'ed.
