# sda-pipeline: backup

Merges data with the encryption header and moves it to backup storage.

## Service Description
The backup service copies files from the archive storage, reattaches the encryption header, and writes the complete file to backup storage.

When running, backup reads messages from the configured RabbitMQ queue (default "backup").
For each message, these steps are taken (if not otherwise noted, errors halts progress, the message is Nack'ed, and the service moves on to the next message):

1. The message is validated as valid JSON that matches either the "ingestion-completion" or "ingestion-accession" schema (based on configuration).
If the message can’t be validated it is discarded with an error message in the logs.

1. The file path and file size is fetched from the database.

1. The file size on disk is requested from the storage system.

1. The database file size is compared against the disk file size.

1. A file reader is created for the archive storage file, and a file writer is created for the backup storage file.

1. If the service is configured to copy headers:

    1. The header is read from the database.
    On error, the error is written to the logs, but the message continues processing.

    1. The header is decrypted.
    If this causes an error, the error is written to the logs, the message is Nack'ed, but message processing continues.

    1. The header is reencrypted.
    If this causes an error, the error is written to the logs, the message is Nack'ed, but message processing continues.

    1. The header is written to the backup file writer.
    On error, the error is written to the logs, but the message continues processing.

1. The file data is copied from the archive file reader to the backup file writer.

1. A completed message is sent to RabbitMQ, if this fails a message is written to the logs, and the message is neither nack'ed nor ack'ed.

1. The message is Ack'ed.
