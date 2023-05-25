#!/bin/bash
set -o allexport; source /app/.credentials; set +o allexport

source=/app/scripts/test.csv
target=s3://etl-on-yaml/test.csv

/usr/local/bin/aws s3 cp $source $target #--recursive
