---
description: 'An extension to the s3 table function, which allows processing files
  from Amazon S3 and Google Cloud Storage in parallel with many nodes in a specified
  cluster.'
sidebar_label: 's3Cluster'
sidebar_position: 181
slug: /sql-reference/table-functions/s3Cluster
title: 's3Cluster'
---

# s3Cluster Table Function

This is an extension to the [s3](sql-reference/table-functions/s3.md) table function.

Allows processing files from [Amazon S3](https://aws.amazon.com/s3/) and Google Cloud Storage [Google Cloud Storage](https://cloud.google.com/storage/) in parallel with many nodes in a specified cluster. On initiator it creates a connection to all nodes in the cluster, discloses asterisks in S3 file path, and dispatches each file dynamically. On the worker node it asks the initiator about the next task to process and processes it. This is repeated until all tasks are finished.

## Syntax {#syntax}

```sql
s3Cluster(cluster_name, url[, NOSIGN | access_key_id, secret_access_key,[session_token]][, format][, structure][, compression_method][, headers][, extra_credentials])
s3Cluster(cluster_name, named_collection[, option=value [,..]])
```

## Arguments {#arguments}

| Argument                              | Description                                                                                                                                                                                             |
|---------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `cluster_name`                        | Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.                                                                                         |
| `url`                                 | path to a file or a bunch of files. Supports following wildcards in readonly mode: `*`, `**`, `?`, `{'abc','def'}` and `{N..M}` where `N`, `M` — numbers, `abc`, `def` — strings. For more information see [Wildcards In Path](../../engines/table-engines/integrations/s3.md#wildcards-in-path). |
| `NOSIGN`                              | If this keyword is provided in place of credentials, all the requests will not be signed.                                                                                                             |
| `access_key_id` and `secret_access_key` | Keys that specify credentials to use with given endpoint. Optional.                                                                                                                                     |
| `session_token`                       | Session token to use with the given keys. Optional when passing keys.                                                                                                                                 |
| `format`                              | The [format](/sql-reference/formats) of the file.                                                                                                                                                         |
| `structure`                           | Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                          |
| `compression_method`                  | Parameter is optional. Supported values: `none`, `gzip` or `gz`, `brotli` or `br`, `xz` or `LZMA`, `zstd` or `zst`. By default, it will autodetect compression method by file extension.                 |
| `headers`                             | Parameter is optional. Allows headers to be passed in the S3 request. Pass in the format `headers(key=value)` e.g. `headers('x-amz-request-payer' = 'requester')`. See [here](/sql-reference/table-functions/s3#accessing-requester-pays-buckets) for example of use. |
| `extra_credentials`                   | Optional. `roleARN` can be passed via this parameter. See [here](/cloud/security/secure-s3#access-your-s3-bucket-with-the-clickhouseaccess-role) for an example.                                          |

Arguments can also be passed using [named collections](operations/named-collections.md). In this case `url`, `access_key_id`, `secret_access_key`, `format`, `structure`, `compression_method` work in the same way, and some extra parameters are supported:

| Argument                       | Description                                                                                                                                                                                                                       |
|--------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `filename`                     | appended to the url if specified.                                                                                                                                                                                                 |
| `use_environment_credentials`  | enabled by default, allows passing extra parameters using environment variables `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONTAINER_CREDENTIALS_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN`, `AWS_EC2_METADATA_DISABLED`. |
| `no_sign_request`              | disabled by default.                                                                                                                                                                                                              |
| `expiration_window_seconds`    | default value is 120.                                                                                                                                                                                                             |

## Returned value {#returned_value}

A table with the specified structure for reading or writing data in the specified file.

## Examples {#examples}

Select the data from all the files in the `/root/data/clickhouse` and `/root/data/database/` folders, using all the nodes in the `cluster_simple` cluster:

```sql
SELECT * FROM s3Cluster(
    'cluster_simple',
    'http://minio1:9001/root/data/{clickhouse,database}/*',
    'minio',
    'ClickHouse_Minio_P@ssw0rd',
    'CSV',
    'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))'
) ORDER BY (name, value, polygon);
```

Count the total amount of rows in all files in the cluster `cluster_simple`:

:::tip
If your listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.
:::

For production use cases, it is recommended to use [named collections](operations/named-collections.md). Here is the example:
```sql

CREATE NAMED COLLECTION creds AS
        access_key_id = 'minio',
        secret_access_key = 'ClickHouse_Minio_P@ssw0rd';
SELECT count(*) FROM s3Cluster(
    'cluster_simple', creds, url='https://s3-object-url.csv',
    format='CSV', structure='name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))'
)
```

## Accessing private and public buckets {#accessing-private-and-public-buckets}

Users can use the same approaches as document for the s3 function [here](/sql-reference/table-functions/s3#accessing-public-buckets).

## Optimizing performance {#optimizing-performance}

For details on optimizing the performance of the s3 function see [our detailed guide](/integrations/s3/performance).

## Related {#related}

- [S3 engine](../../engines/table-engines/integrations/s3.md)
- [s3 table function](../../sql-reference/table-functions/s3.md)
