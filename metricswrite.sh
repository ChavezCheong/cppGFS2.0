#!/bin/bash

successes=0
failures=0

for i in {1..500}; do
  filename="/test_$i"
  output=$(time -p bazel-bin/gfs_client_main --mode=create --filename="$filename" 2>&1)
  if [[ "$output" == *"File created"* ]]; then
    echo "Success: $filename"
    ((successes++))
  else
    echo "Failure: $filename"
    ((failures++))
  fi
done

echo "Successes: $successes"
echo "Failures: $failures" 