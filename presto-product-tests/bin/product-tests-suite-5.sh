#!/usr/bin/env bash

set -xeuo pipefail

presto-product-tests/bin/run_on_docker.sh \
    singlenode-hive-impersonation \
    -g storage_formats,cli,hdfs_impersonation

presto-product-tests/bin/run_on_docker.sh \
    singlenode-kerberos-hive-impersonation \
    -g storage_formats,cli,hdfs_impersonation,authorization,hive_file_header
