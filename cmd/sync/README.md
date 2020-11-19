# Basic instructions

## Define the two s3 backends

The sync is currently using the configuration from ingest. Therefore define the two s3 backends where
`archive` is the `destination` and `inbox` is the `source`.

## Add a file in the source s3

Use `s3cmd` to upload a document to the `inbox/source` s3. Then change the name of the file in line 47 of `sync.go`.

## Run the sync

Navigate to the `sync` folder and run

```command
CONFIGFILE=../../dev_utils/config.yaml go run .
```
