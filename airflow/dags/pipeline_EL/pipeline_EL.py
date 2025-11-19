import logging
import pandas as pd
import os
from sqlalchemy import create_engine, text
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Extract

def extract_data():
    data_dir = "E:\\main_folder\\Tech\\Projetos\\elt_olist_dbt\\airflow\\data"

    dfs_mapeados = {}
    for arquivo in os.listdir(data_dir):
        nome_tabela = arquivo.replace('.csv','')
        df_atual = pd.read_csv(os.path.join(data_dir,arquivo))
        dfs_mapeados[nome_tabela] = df_atual

    return dfs_mapeados



def load_data(dfs_mapeados:dict):
    hook = PostgresHook(postgres_conn_id='postgres_olist_dw')
    engine = hook.get_sqlalchemy_engine()
    
    for nome_tabela, df in dfs_mapeados.items():
        df.to_sql(
            name=nome_tabela,
            con=engine,
            schema='bronze_olist',
            if_exists='replace',
            index=False
        )
    logging.info("Carga de dados concluída com sucesso")
