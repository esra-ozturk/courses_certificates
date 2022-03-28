Clone the Repository

git clone https://github.com/esra-ozturk/courses_certificates.git


Move to the /postgres directory and run :

docker-compose up -d 

Move to the /etl directory and run :

docker compose -f airflow-docker-compose.yml up airflow-init

docker compose -f airflow-docker-compose.yml up -d

Navigate to http://localhost:8080/ to connect Airflow
username = airflow
password = airflow 


With the data provided i have found  :

average amount of users time spent for each course individually

report of fastest vs. slowest users completing a course

amount of certificates per customer

Apache Airflow was used for the scheduling and orchestration of data pipeline.
