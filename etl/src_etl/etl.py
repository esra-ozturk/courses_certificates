import pandas as pd
import logging
import os
import psycopg2
import psycopg2.extras
from dotenv import dotenv_values


def extract(file_to_process) :
    dataframe = pd.read_json("./data/{}".format(file_to_process))
    return dataframe
    

def transform(file_name,df_extracted_data):

    def transform_users(df_extracted_data):
        df_transformed_data = df_extracted_data.copy()
        df_transformed_data.rename(columns={"id":"user_id", "email":"email","firstName":"first_name","lastName":"last_name"}, inplace=True)
        table_name = 'DWD_users'
        return table_name, df_transformed_data

    def transform_courses(df_extracted_data):
        df_transformed_data = df_extracted_data.copy()
        df_transformed_data.rename(columns={"id":"course_id", "title":"title","description":"description","publishedAt":"published_date"}, inplace=True)
        table_name = 'DWD_courses'
        return table_name,df_transformed_data

    def transform_certificates(df_extracted_data):
        df_transformed_data = df_extracted_data.copy()
        df_transformed_data.rename(columns={"course":"course_id", "user":"user_id","completedDate":"completed_date","startDate":"start_date"}, inplace=True)       
        table_name = 'DWF_certificates'
        return table_name, df_transformed_data

    def invalid_op():
        raise Exception("Unknown file")
    
    ops = {
    "users.json": transform_users,
    "courses.json": transform_courses,
    "certificates.json": transform_certificates
  }.get(file_name, invalid_op)

    return ops(df_extracted_data)

def load(table_name,df_transformed_data):

    conn = None

    try:
        # read the connection parameters & establishing the connection
        # hostname: name of the other container!
        CONFIG = dotenv_values(".env")
        if not CONFIG:
            CONFIG = os.environ
        conn = psycopg2.connect(
            database=CONFIG["POSTGRES_DB"], 
            user=CONFIG["POSTGRES_USER"], 
            password=CONFIG["POSTGRES_PASSWORD"], 
            host=CONFIG["POSTGRES_HOST"],  
            port=CONFIG["POSTGRES_PORT"]
             )

        #Creating a cursor object using the cursor() method
        cursor = conn.cursor()
        #Load transformed data into the postgres database
        df_columns = list(df_transformed_data)
        # create solumns 
        columns = ",".join(df_columns)

        # create VALUES per column
        values = "VALUES({})".format(",".join(["%s" for i in df_columns])) 

        insert_stmt = "INSERT INTO {} ({}) {}".format(table_name,columns,values)
        psycopg2.extras.execute_batch(cursor, insert_stmt, df_transformed_data.values) # it is faster
        logging.critical('--Inserting  new records to {} table into postgres--'.format(table_name))
        logging.critical(table_name)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.commit()
            conn.close()
def etl_main():
    for file_name in  os.listdir("./data/"):
        df_extracted_data = extract(file_name)
        table_name,df_transformed_data = transform(file_name,df_extracted_data)
        load(table_name,df_transformed_data)
        df_extracted_data.iloc[0:0] # clean dataframes
        df_transformed_data.iloc[0:0] # clean dataframes

if __name__ == '__main__':
    etl_main()