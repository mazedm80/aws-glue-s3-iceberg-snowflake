#!/bin/bash

LOCAL_CSV_FILES=./data/
S3_TARGET_PATH=s3://airport-raw-eu-central-1-d7366c05-dev/data

echo "Performing dry run..."
aws s3 sync "$LOCAL_CSV_FILES" "$S3_TARGET_PATH" --dryrun

echo
read -p "Dry run complete. Proceed with actual upload? (Y/y to confirm): " CONFIRM

if [[ "$CONFIRM" == "Y" || "$CONFIRM" == "y" ]]; then
  echo "Uploading files..."
  aws s3 sync "$LOCAL_CSV_FILES" "$S3_TARGET_PATH"
  echo "Upload completed."
else
  echo "Upload canceled."
fi