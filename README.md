# aws-timestream-databricks-scala

Example taken from https://github.com/awslabs/amazon-timestream-tools/tree/master/sample_apps/javaV2 translated to Scala and Run using Databricks. This example creates, lists and describes Databases and Tables, and also writes records.

## Requisites
1. Install AWS JavaV2 Libraries via Maven Repositories in your Cluster:
    - software.amazon.awssdk:timestreamwrite:2.15.77
    - software.amazon.awssdk:apache-client:2.15.77
    - args4j:args4j:2.33
2. Load AWS Credentials as Environment Variables
