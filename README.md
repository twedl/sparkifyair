# Dag for Song Play Analysis

Sparkify requires an ETL pipeline to handle the activity of a large and growing userbase. This repo has python code and sql queries for a data pipeline in AWS (S3, EMR and Spark) that creates analytical database for song play analysis. Analysis could include summary statistics for song plays by user type, time, artist, and location, among many other possibilities.

## Important files

* `dags/udac_example_dag.py`: dag file
* `plugins/operators/stage_redshift.py`: StageToRedshiftOperator => for loading song and event json data to redshift staging database
* `plugins/operators/load_fact.py`: LoadFactOperator => for loading event facts from staged db to final db
* `plugins/operators/load_dimension.py`: LoadDimensionOperator => for loading song, artist, title, and time tables
* `plugins/operators/data_quality.py`: DataQualityOperator => to check if tables exist and have records

