import pendulum
import pandas as pd
from pathlib import Path
import glob
from datetime import timedelta
from airflow import DAG
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


PATH_DATA = Path("/opt/airflow/data")
PATH_STAGING = "/opt/airflow/staging/"

default_args = {
    "depends_on_past" : False,
    "email": ["teste@email.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}


with DAG(
    dag_id="dag_olist_dbt",
    description="Orquestração de extração e carga para projeto olist",
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Pratica","Etl"]
) as dag:
    

    @task(task_id='extract_data')
    def task_extract(path_data:str) -> str:

        for arquivo in path_data.iterdir():
            nome_tabela = arquivo.stem
            dataframe = pd.read_csv(arquivo)
            path_parquet = PATH_STAGING+nome_tabela+'.parquet'
            dataframe.to_parquet(path_parquet,index=False)
        
        return PATH_STAGING
    
    @task(task_id='setup_database')
    def setup_database():
        hook = PostgresHook(postgres_conn_id='postgres_olist_dw')
        hook.run("CREATE SCHEMA IF NOT EXISTS bronze_olist;")


    @task(task_id='load_data')
    def task_load(path_stanging:str):
        hook = PostgresHook(postgres_conn_id='postgres_olist_dw')
        engine = hook.get_sqlalchemy_engine()

        staging = Path(path_stanging)
        lista_parquet = staging.glob("*.parquet")

        for arquivo in lista_parquet:
            nome_tabela = arquivo.stem
            df = pd.read_parquet(arquivo)
            df.to_sql(
                name=nome_tabela,
                con=engine,
                schema='bronze_olist',
                if_exists='replace',
                index=False
            )

    df = task_extract(PATH_DATA)
    
    [df , setup_database()] >> task_load(df)
    

    
