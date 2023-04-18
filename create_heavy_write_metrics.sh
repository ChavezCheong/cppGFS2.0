#!/bin/bash

create_successes=0
create_failures=0
write_successes=0
write_failures=0

filename="/test"
create_output=$(time -p bazel-bin/gfs_client_main --mode=create --filename="$filename" 2>&1)
if [[ "$create_output" == *"File created"* ]]; then
  echo "Create Success: $filename"
  ((create_successes++))
else
  echo "Create Failure: $filename"
  ((create_failures++))
fi
offset=0
for i in {1..500}; do
  write_output=$(time -p bazel-bin/gfs_client_main --mode=write --filename="$filename" --offset=$offset --data='Hello World!' 2>&1)
  if [[ "$write_output" == *"Data written successfully"* ]]; then
    echo "Write Success: $filename"
    ((write_successes++))
  else
    echo "Write Failure: $filename"
    ((write_failures++))
  fi
  offset=$((offset + 12))
done

echo "Create Successes: $create_successes"
echo "Create Failures: $create_failures" 
echo "Write Successes: $write_successes"
echo "Write Failures: $write_failures"
