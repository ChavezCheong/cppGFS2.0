#!/bin/bash

create_successes=0
create_failures=0
for i in {1..300}; do
  filename="/test_$i"
  create_output=$(time -p bazel-bin/gfs_client_main --mode=create --filename="$filename" 2>&1)
  if [[ "$create_output" == *"File created"* ]]; then
    echo "Create Success: $filename"
    ((create_successes++))
  else
    echo "Create Failure: $filename"
    ((create_failures++))
  fi
done

echo "Create Successes: $create_successes"
echo "Create Failures: $create_failures"
