# Spotify End-to-End Data Engineering Project

### Introduction
In this project, I have build an ETL (Extract, Transform, Load) pipeline using the Spotify API on AWS. The pipeline will retrieve data from the Spotify API, transform it to a desired format, and load it into an AWS data store.

### Architecture
![Architecture Diagram](https://github.com/johnson9081/spotify-end-to-end-data-engineering-project/blob/main/Spotify%20Data%20Pipeline%20Architecture%20Diagram.png)

### Services Used
1. **S3 (Simple Storage Service):** Amazon S3 (Simple Storage Service) is a highly scalable object storage service that can store and retrieve any amount
of data from anywhere on the web. It is commonly used to store and distribute large media files, data backups, and static website files.
2. **AWS Lambda:** AWS Lambda is a serverless computing service that lets you run your code without managing servers. You can use Lambda to run code in
response to events like changes in S3, DynamoDB, or other ANS services.
3. **Cloud Watch:** Amazon Cloudwatch is a monitoring service for ANS resources and the applications you run on them. You can use Cloudwatch to collect and
track netrics, collect and monitor log files, and set alarms.
4. **Glue Crawler:** ANS Glue Crawler is a fully managed service that automatically crawls your data sources, identifies data formats, and infers schemas
to create an AWS Glue Data Catalog.
5. **Data Catalog:** ANS Glue Data Catalog is a fully managed metadata repository that makes it easy to discover and manage data in AWS. You can use the
Glue Data Catalog with other AWS services, such as Athena.
6. **Amazon Athena:** Anazon Athena is an interactive query service that makes it easy to analyze data in Amazon S3 using standard SOL. You can use Athen
to analyze data in your Glue Data Catalog or in other S3 buckets.


### Install Packages
```
pip install pandas
pip install numpy
pip install spotipy
```

### Project Execution Flow
Extract Data from API -> Lambda Trigger (every 1 hour) -> Run Extract Code → Store Raw Data -> Trigger Transform Function -> Transfrom Data and Load It ->
Query Using Athena
