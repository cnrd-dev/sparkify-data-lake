# Sparkify Data Lake

## Motivation

The startup, Sparkify, is a music streaming app and wants to analyse what songs their users are listening to. Their current data is available as local JSON files on AWS S3 and cannot be easily analised. The purpose of the ETL process is to extract the JSON files from S3, transform the data with Spark into fact and dimension files and write data back in parquet format into S3 for analysis.

## Data model

Data was normalized into a star schema with fact and dimention tables in order to use data for the analysis but also reuse data for future analysis.

## File descriptions

- `etl.py`: Load JSON files (song and log files) from S3, process data into the data model and load back into S3 for analytics.

## How to run the files and notebooks

Dependencies and virtual environment details are located in the `Pipfile` which can be used with `pipenv`.
Additionally, a `dl.cfg` needs to be created with the following structure:

```text
[S3]
AWS_ACCESS_KEY_ID=''
AWS_SECRET_ACCESS_KEY=''
INPUT_DATA='s3a://{bucket name}/'
OUTPUT_DATA='s3a://{bucket name}/'

[LOCAL]
INPUT_DATA='{path}/'
OUTPUT_DATA='{path}/parquet/'
```

## License

GNU GPL v3

## Author

Coenraad Pretorius
