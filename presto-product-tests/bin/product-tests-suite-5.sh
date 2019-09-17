#!/usr/bin/env bash

set -xeuo pipefail

exit_code=0

presto-product-tests/bin/run_on_docker.sh \
    singlenode-hive-impersonation \
    -g storage_formats,cli,hdfs_impersonation \
    || exit_code=1

presto-product-tests/bin/run_on_docker.sh \
    singlenode-kerberos-hive-impersonation \
    -g storage_formats,cli,hdfs_impersonation,authorization,hive_file_header \
    || exit_code=1

exit "${exit_code}"
