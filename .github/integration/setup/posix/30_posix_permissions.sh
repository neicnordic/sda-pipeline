#!/bin/bash

docker run --rm -v dev_utils_archive:/foo alpine sh -c "chmod 777 /foo"
