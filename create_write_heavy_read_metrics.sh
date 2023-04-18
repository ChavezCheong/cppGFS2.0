#!/bin/bash

create_successes=0
create_failures=0
write_successes=0
write_failures=0
read_successes=0
read_failures=0

filename="/test"
create_output=$(time -p bazel-bin/gfs_client_main --mode=create --filename="$filename" 2>&1)
if [[ "$create_output" == *"File created"* ]]; then
  echo "Create Success: $filename"
  ((create_successes++))
else
  echo "Create Failure: $filename"
  ((create_failures++))
fi
write_output=$(time -p bazel-bin/gfs_client_main --mode=write --filename="$filename" --offset=0 --data='Hello World!' 2>&1)
  echo $write_output
  if [[ "$write_output" == *"Data written successfully"* ]]; then
    echo "Write Success: $filename"
    ((write_successes++))
  else
    echo "Write Failure: $filename"
    ((write_failures++))
  fi
for i in {1..500}; do
  read_output=$(time -p bazel-bin/gfs_client_main --mode=read --filename=$filename --offset=0 --nbytes=100 2>&1)
  if [[ "$read_output" == *"Data read: 'Hello World!'"* ]]; then
    echo "Read Success: $filename"
    ((read_successes++))
  else
    echo "Read Failure: $filename"
    ((read_failures++))
  fi
done

echo "Create Successes: $create_successes"
echo "Create Failures: $create_failures" 
echo "Write Successes: $write_successes"
echo "Write Failures: $write_failures"
echo "Read Successes: $read_successes"
echo "Read Failures: $read_failures"
