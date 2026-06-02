from airflow.sdk import dag, task 
from pendulum import datetime
import pandas as pd
import numpy as np
import os


@dag(
    dag_id = 'airflow_assignment'
)
def airflow_assignment():

    @task.python 
    # Extraction Of Data
    def extract_task(**kwargs):
        et = kwargs['ti']
        df = pd.read_csv('/home/nineleaps/Documents/Python Training/Datasets/netflix_titles.csv')
        records = len(df)
        print(f"Extracted {records} Records")
        extracted_data = df.to_dict()
        et.xcom_push(key = 'extracted_data', value = extracted_data)

    @task.python
    #Data Transformation
    def transform_task(**kwargs):
        tt = kwargs['ti']
        extracted_data = tt.xcom_pull(key = 'extracted_data' , task_ids = 'extract_task')

        #Conversion into dataframe
        df = pd.DataFrame(extracted_data)

        #Dropping duplicates
        df = df.drop_duplicates()

        #Handling null values
        df['director'] = df['director'].fillna('Unknown')
        df['cast'] = df['cast'].fillna('Not Available')
        df['country'] = df['country'].fillna('Unknown')

        #Connverting to suitable data types
        df['show_id'] = df['show_id'].str.upper()
        df['type'] = df['type'].str.upper()
        df['title'] = df['title'].str.title()
        df['country'] = df['country'].str.upper()
        df = df.drop(columns = ['date_added'])
        df['release_year'] = pd.to_numeric(df['release_year'],errors='coerce')
        df['rating'] = df['rating'].str.upper()
        df['description'] = df['description'].astype(str)
        df['duration'] = df['duration'].astype(str)

        #Feature Engineering
        df['duration_min'] = df['duration'].apply(lambda x: int(str(x).split()[0]) if 'min' in str(x) else np.nan)
        df['seasons'] = df['duration'].apply(lambda x: int(str(x).split()[0]) if 'Season' in str(x) else np.nan)

        transformed_data = df.to_dict()
        tt.xcom_push(key = 'transformed_data', value = transformed_data)

        print("Data Transformation Done")

    @task.python
    def load_task(**kwargs):
        #check to wether directory exists or not to save output
        output_path = '/home/nineleaps/airflow/logs/data/transformed_netflix_titles.csv'
        os.makedirs(os.path.dirname(output_path),exist_ok=True)
        lt = kwargs['ti']
        transformed_data = lt.xcom_pull(key = 'transformed_data', task_ids = 'transform_task')
        df = pd.DataFrame(transformed_data)
        df.to_csv(output_path,index=False)

        print(f"Data saved to {output_path}")
    
    @task.python
    def eda_task(**kwargs):
        et = kwargs['ti']
        transformed_data = et.xcom_pull(key = 'transformed_data' , task_ids = 'transform_task')
        df = pd.DataFrame(transformed_data)
        print("\n===== EDA RESULTS =====")

        # Top countries
        print("\nTop Countries:")
        print(df['country'].value_counts().head())

        # Content type distribution
        print("\nContent Type Distribution:")
        print(df['type'].value_counts())

        # Top genres
        print("\nTop Genres:")
        genres = df['listed_in'].str.split(', ').explode()
        print(genres.value_counts().head())

        #Year trend
        print("\nContent Added per Year:")
        print(df['release_year'].value_counts().sort_index())


    first = extract_task()
    second = transform_task()
    third = load_task()
    fourth = eda_task()

    first >> second >> [third , fourth]


airflow_assignment()