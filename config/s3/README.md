# Configuration Guide

## Index

- [Configuration Guide](#configuration-guide)
  - [Index](#index)
  - [Create a storage credential for connecting to AWS S3](#create-a-storage-credential-for-connecting-to-aws-s3)
  - [S3 Bucket Configurations](#s3-bucket-configurations)
    - [General](#general)
    - [mazda-csv-data-nonprd](#mazda-csv-data-nonprd)


## Create a storage credential for connecting to AWS S3

<https://docs.databricks.com/en/connect/unity-catalog/storage-credentials.html>

## S3 Bucket Configurations

### General

| Setting | Name | Id | Link | Note |
| :-- | :-- | :-- | :-- | :-- |
| AWS IAM Role | rl-cross-account-databricks | arn:aws:iam::289450402608:role/rl-cross-account-databricks | https://us-east-1.console.aws.amazon.com/iam/home?region=us-east-1#/roles/details/rl-cross-account-databricks?section=trust_relationships | Provides cross account access to DBX Control Plane and Unity Catalog |

### mazda-csv-data-nonprd

| Setting | Name | Id | Link | Note |
| :-- | :-- | :-- | :-- | :-- |
| S3 Bucket | mazda-csv-data-nonprd | arn:aws:s3:::mazda-csv-data-nonprd | https://eu-west-1.console.aws.amazon.com/s3/buckets/mazda-csv-data-nonprd?region=eu-west-1&bucketType=general ||
| AWS Bucket Policy ||| https://eu-west-1.console.aws.amazon.com/s3/buckets/mazda-csv-data-nonprd?region=eu-west-1&bucketType=general&tab=properties |  Provides the principle with read/write/del/... rights to the bucket |

s3://mazda-csv-data-nonprd/mle-ipaas-sf-uat/
