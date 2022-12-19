# sda-pipeline: ingest

Splits the Crypt4GH header and moves it to database. The remainder of the file
is sent to the storage backend (archive). No cryptographic tasks are done.

## Service Description

The ingest service copies files from the file inbox to the archive, and registers them in the database.

When running, ingest reads messages from the configured RabbitMQ queue (default: "ingest").
For each message, these steps are taken (if not otherwise noted, errors halt progress and the service moves on to the next message):

1. The message is validated as valid JSON that matches the "ingestion-trigger" schema (defined in sda-common).
If the message can’t be validated it is discarded with an error message in the logs.

1. A check is performed to get the status of the file that is to be ingested, if the status is `DISABLED` the work is aborted and the message will be acked.

1. A file reader is created for the filepath in the message.
If the file reader can’t be created an error is written to the logs, the message is Nacked and forwarded to the error queue.

1. The file size is read from the file reader.
On error, the error is written to the logs, the message is Nacked and forwarded to the error queue.

1. A uuid is generated, and a file writer is created in the archive using the uuid as filename.
On error the error is written to the logs and the message is Nacked and then re-queued.

1. The filename is inserted into the database along with the user id of the uploading user.
Errors are written to the error log.
Errors writing the filename to the database do not halt ingestion progress.

1. The header is read from the file, and decrypted to ensure that it’s encrypted with the correct key.
If the decryption fails, an error is written to the error log, the message is Nacked, and the message is forwarded to the error queue.

1. The header is written to the database.
Errors are written to the error log.

1. The header is stripped from the file data, and the remaining file data is written to the archive.
Errors are written to the error log.

1. The size of the archived file is read.
Errors are written to the error log.

1. A check is performed to get the status of the file, if the status is `DISABLED` the work is aborted, the file will be removed from the archive and the message will be acked.

1. The database is updated with the file size, archive path, and archive checksum, and the file is set as “archived”.
Errors are written to the error log.
This error does not halt ingestion.

1. A message is sent back to the original RabbitMQ broker containing the upload user, upload file path, database file id, archive file path and checksum of the archived file.
