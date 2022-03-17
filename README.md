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