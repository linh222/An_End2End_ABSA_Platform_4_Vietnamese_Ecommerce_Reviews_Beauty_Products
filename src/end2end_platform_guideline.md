# Guildline to build an end2end platform for Vietnamese Reviews

## Requirements
* PostgreSQL: Version 14.8
* Airflow: Version 2.2
* Tableau Desktop

## Code for Airflow
1. Set up airflow as Airflow_README.md
2. Copy the dags in end2end_platform to dags folder in Airflow
3. Copy models, codes to airflow folder
4. Start airflow

## Tableau
1. Set up the data source between PostgreSQL and Tableau.
2. Download the dashboard template at https://bit.ly/end2end_absa_vietnamese_review
3. Change the data source of dashboard to PostgreSQL
