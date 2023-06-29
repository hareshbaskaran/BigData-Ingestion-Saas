# DataGenie Hackathon - 2023 (Data Engineering)

This project involves aggregating and transforming data from a data source and loading it into a GCP SQL server. It utilizes Databricks for data processing and Airflow for pipeline automation.

## Dataset

The data source for this project can be accessed at the following link from Google BigQUery:

Dataset Link: [https://console.cloud.google.com/marketplace/product/obfuscated-ga360-data/obfuscated-ga360-data?project=lexical-script-761](https://console.cloud.google.com/marketplace/product/obfuscated-ga360-data/obfuscated-ga360-data?project=lexical-script-761)

## Databricks and GCP Cluster Details

Databricks is used for data processing, and the GCP PostgreSQL server is used for hosting the transformed data. You can log in to both platforms using the following credentials:

- Databricks:
  - Host: [https://3600576718119515.5.gcp.databricks.com/](https://3600576718119515.5.gcp.databricks.com/)
  - Workspace: "haresh"
  - Jobs: Extract, Transform, Load

- GCP POSTGRESQL Server:
  - IP Address: 34.30.53.0
  - Instance Name: "haresh"
  - Login Credentials: 
    - Email: hareshbaskaran.work@gmail.com
    - Password: Dertuport0208
  - PostgreSql JDBC Connector (Psycog2) Configuration:
    ```python
    config = {
        database="haresh",
        host="34.30.53.0",
        user="haresh",
        password="haresh",
        port='5432'
    }
    ```

## Airflow Setup

To set up the Airflow pipeline, follow these steps:

1. Clone the project repository from GitHub.
2. Navigate to the project directory.
3. Start the Docker engine by running the following command: 
 ```python
    docker-compose up -d --build
 ```
5. Ensure that the Docker daemon is running.
6. Access the Airflow UI by going to [localhost:8080](http://localhost:8080).
7. In the Airflow UI, go to Admin > Connections.
8. Locate the `databricks_default` connection and click on the Edit button.
9. Update the "Extra" field with the following JSON format:
```json
{"host":"https://3600576718119515.5.gcp.databricks.com/","token":"dapi2aac1b500daa583700ea7ac51e1d809c"}

