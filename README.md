# Sparkify Data Lake

## Motivation

The startup, Sparkify, is a music streaming app and wants to analyse what songs their users are listening to. Their current data is available as local JSON files on AWS S3 and cannot be easily analised. The purpose of the ETL process is to load the JSON files from S3, process the data with Spark into fact and dimension files and write it back into S3 for analysis.

## Data model

Data was normalized into a star schema with fact and dimention tables in order to use data for the analysis but also reuse data for future analysis.

## File descriptions

- `etl.py`: Load JSON files (song and log files) from S3, process data into the data model and load back into S3 for analytics.

## How to run the files and notebooks

Dependencies and virtual environment details are located in the `Pipfile` which can be used with `pipenv`.

## License

GNU GPL v3

## Author

Coenraad Pretorius
