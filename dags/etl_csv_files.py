import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator


with DAG(
    'etl_csv_files',
    start_date=datetime(2024, 3, 11),
    schedule='0 8 * * *'
):
    files = {
        'customer_interactions' : 'https://drive.google.com/file/d/1WG3xelY7LHkBDygWq4qPbC0ZXk1y92hI/view?usp=drive_link',
        'product_details' : 'https://drive.google.com/file/d/1WkgRO3mlmhajPiXFOZtnOnWCXzCr_nKd/view?usp=drive_link',
        'purchase_history' : 'https://drive.google.com/file/d/1QlrrDOGYHNAu-uzDIeWijGyqdJnLFjoV/view?usp=drive_link'
    }

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    def read_from_gdrive(url):
        url = 'https://drive.google.com/uc?id=' + url.split('/')[-2]
        df = pd.read_csv(url)
        print("Sample data :")
        print(df.head())
        return df

    @task()
    def get_files_customer(**kwargs):
        url = files['customer_interactions']
        df = read_from_gdrive(url)
        return df

    @task()
    def get_files_product(**kwargs):
        url = files['product_details']
        df = read_from_gdrive(url)
        return df
    
    @task()
    def get_files_purchase(**kwargs):
        url = files['purchase_history']
        df = read_from_gdrive(url)
        return df

    @task()
    def combine_files(**kwargs):
        task_instance = kwargs['task_instance']
        df_customer = task_instance.xcom_pull('get_files_customer')
        df_product = task_instance.xcom_pull('get_files_product')
        df_purchase = task_instance.xcom_pull('get_files_purchase')

        df = df_customer.set_index('customer_id').join(df_purchase.set_index('customer_id'), how='outer').reset_index()
        df = df.set_index('product_id').join(df_product.set_index('product_id'), how='outer').reset_index()
        print(df.head())

        return df
    
    @task()
    def insert_to_db(**kwargs):
        host = 'ep-gentle-wind-a12yboxc.ap-southeast-1.aws.neon.tech'
        port = 5432
        database = 'bootcamp'
        username = 'thoni.shohibus'
        password = 'dr5isAQ1GMfP'

        conn_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'
        conn = create_engine(conn_string)
        print("Sucess Connect")

        task_instance = kwargs['task_instance']
        df = task_instance.xcom_pull('combine_files')
        df.to_sql('transaction_data_wide_table', conn, if_exists='replace')
        print("Sucess INSERT")

    start >> [get_files_customer(), get_files_product(), get_files_purchase()] >> combine_files() >> insert_to_db() >> end
