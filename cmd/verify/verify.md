# sda-pipeline: verify

Uses a crypt4gh secret key, this service can decrypt the stored files and checksum them against the embedded checksum for the unencrypted file.

## Service Description

The verify service ensures that ingested files are encrypted with the correct key, and that the provided checksums match those of the ingested files.

When running, verify reads messages from the configured RabbitMQ queue (default: "archived").
For each message, these steps are taken (if not otherwise noted, errors halt progress and the service moves on to the next message.
Unless explicitly stated, error messages are *not* written to the RabbitMQ error queue, and messages are not NACK or ACKed.):

1. The message is validated as valid JSON that matches the "ingestion-verification" schema (defined in sda-common).
If the message can’t be validated it is discarded with an error message in the logs.

1. A check is performed to get the status of the file that is to be verified, if the status is `DISABLED` the work is aborted, the file will be removed from the archive and the message will be acked.

1. The service attempts to fetch the header for the file id in the message from the database.
If this fails a NACK will be sent for the RabbitMQ message, the error will be written to the logs, and sent to the RabbitMQ error queue.

1. The file size of the encrypted file is fetched from the archive storage system.
If this fails an error will be written to the logs.

1. The archive file is then opened for reading.
If this fails an error will be written to the logs and to the RabbitMQ error queue.

1. A decryptor is opened with the archive file.
If this fails an error will be written to the logs.

1. The file size, md5 and sha256 checksum will be read from the decryptor.
If this fails an error will be written to the logs.

1. If the `re_verify` boolean is not set in the RabbitMQ message, the message processing ends here, and continues with the next message.
Otherwise the processing continues with verification:

    1. A verification message is created, and validated against the "ingestion-accession-request" schema.
    If this fails an error will be written to the logs.

    1. A check is performed to get the status of the file, if the status is `DISABLED` the work is aborted, the file will be removed from the archive and the message will be acked.

    1. The file is marked as *verified* in the database (*COMPLETED* if you are using database schema <= 3).
    If this fails an error will be written to the logs.

    1. The verification message created in step 7.1 is sent to the "verified" queue.
    If this fails an error will be written to the logs.

    1. The original RabbitMQ message is ACKed.
    If this fails an error is written to the logs, but processing continues to the next step.

    1. The archive file is removed from the inbox storage.
    If this fails an error is written to the logs, and an error is written to the error queue.
