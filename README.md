[![test](https://github.com/shogo82148/s3cli-mini/workflows/test/badge.svg?branch=master)](https://github.com/shogo82148/s3cli-mini/actions)
[![Coverage Status](https://coveralls.io/repos/github/shogo82148/s3cli-mini/badge.svg?branch=master)](https://coveralls.io/github/shogo82148/s3cli-mini?branch=master)

# s3cli-mini
Golang port for AWS Command Line Interface S3 subcommand.

## Subcommands

### cp

The `cp` command copies a local file or S3 object to another location locally or in S3.

```
s3cli-mini cp <LocalPath> <S3Uri> or <S3Uri> <LocalPath> or <S3Uri> <S3Uri>
```

```bash
# download from a S3 bucket
s3cli-mini cp s3://your-bucket/foobar.zip .

# upload to a S3 bucket
s3cli-mini cp foobar.zip s3://your-bucket/

# copy the file from a S3 bucket to another S3 bucket.
s3cli-mini cp s3://your-bucket/foobar.zip s3://another-bucket/
```

### ls

The `ls` command lists S3 objects and common prefixes under a prefix or all S3 buckets.

```bash
# show bucket list
s3cli-mini ls

# list the objects in the bucket.
s3cli-mini ls s3://your-bucket/
```

### mb

The `mb` command creates an S3 bucket.

```bash
# create a bucket
s3cli-mini mb your-bucket
```

## Licence

The MIT License. See LICENSE file.
