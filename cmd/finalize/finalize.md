# sda-pipeline: finalize

Handles the so-called _Accession ID (stable ID)_ to filename mappings from Central EGA.

## Service Description
Finalize adds stable, shareable _Accession ID_'s to archive files.
When running, finalize reads messages from the configured RabbitMQ queue (default "accessionIDs").
For each message, these steps are taken (if not otherwise noted, errors halt progress and the service moves on to the next message):

1. The message is validated as valid JSON that matches the "ingestion-accession" schema (defined in sda-common).
If the message can’t be validated it is discarded with an error message in the logs.

1. if the type of the `DecryptedChecksums` field in the message is `sha256`, the value is stored.

1. A new RabbitMQ "complete" message is created and validated against the "ingestion-completion" schema.
If the validation fails, an error message is written to the logs.

1. The file accession ID in the message is marked as "ready" in the database.
On error the service sleeps for up to 5 minutes to allow for database recovery, after 5 minutes the message is Nacked, re-queued and an error message is written to the logs.

1. The complete message is sent to RabbitMQ. On error, a message is written to the logs.

1. The original RabbitMQ message is Ack'ed.
