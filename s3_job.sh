#!/bin/bash

LOCAL_CSV_FILE=./workspace/src/
S3_TARGET_PATH=s3://airport-raw-eu-central-1-d7366c05-dev/scripts

echo "Performing dry run..."
aws s3 sync "$LOCAL_CSV_FILE" "$S3_TARGET_PATH" --dryrun

echo
read -p "Dry run complete. Proceed with actual upload? (Y/y to confirm): " CONFIRM

if [[ "$CONFIRM" == "Y" || "$CONFIRM" == "y" ]]; then
  echo "Uploading file..."
  aws s3 sync "$LOCAL_CSV_FILE" "$S3_TARGET_PATH"
  echo "Upload completed."
else
  echo "Upload canceled."
fi