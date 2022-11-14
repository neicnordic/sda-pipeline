# sda-pipeline: intercept

The intercept service relays messages between Central EGA and Federated EGA nodes.

## Service Description

When running, intercept reads messages from the configured RabbitMQ queue (default: "files").
For each message, these steps are taken (if not otherwise noted, errors halts progress, the message is Nack'ed, the error is written to the log, and to the rabbitMQ error queue.
Then the service moves on to the next message):

1. The message type is read from the message "type" field.

1. The message schema is read from the message "msgType" field.

1. The message is validated as valid JSON following the schema read in the previous step.
If this fails an error is written to the logs, but not to the error queue and the message is not Ack'ed or Nack'ed.

1. The correct queue for the message is decided based on message type.
This is not supposed to be able to fail.

1. The message is re-sent to the correct queue.
This has no error handling as the resend-mechanism hasn't been finished.

1. The message is Ack'ed.

