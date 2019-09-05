# s3cli-mini
Golang port for AWS Command Line Interface S3 subcommand.

## Subcommands

### cp

`cp` command copies a local file or S3 object to another location locally or in S3.

#### Usage

```
s3cli-mini cp <LocalPath> <S3Uri> or <S3Uri> <LocalPath> or <S3Uri> <S3Uri>
```

#### Examples

```
# download from a S3 bucket
s3cli-mini cp s3://your-bucket/foobar.zip .

# upload to a S3 bucket
s3cli-mini cp foobar.zip s3://your-bucket/

# copy the file from a S3 bucket to another S3 bucket.
s3cli-mini cp s3://your-bucket/foobar.zip s3://another-bucket/
```

## Licence

The MIT License. See LICENSE file.
