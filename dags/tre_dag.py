from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import zipfile
import os

from datetime import datetime
import pandas as pd

### Funções para PythonOperator

# Função para descompactar o arquivo ZIP
def unzip():
    filepath_zip = '/opt/airflow/data/tre.zip'
    folder_zip = '/opt/airflow/data'
    filename_csv = 'tre.csv'

    if zipfile.is_zipfile(filepath_zip):
        with zipfile.ZipFile(filepath_zip, 'r') as zip_ref:
            zip_ref.extract(filename_csv, path=folder_zip)
        print(f"Arquivo extraído para {folder_zip}")
    else:
        print(f"{filepath_zip} não é um arquivo ZIP válido.")

# Função para ler o arquivo CSV
def extract(**kwargs):
    df = pd.read_csv('/opt/airflow/data/tre.csv', encoding='ISO-8859-1', sep=';')
    
    print(df.head())
    
    return df

# Função para reduzir dados apenas para colunas selecionadas
def transform_reduzir(**kwargs):
    ti = kwargs['ti']

    df = ti.xcom_pull(task_ids='extract_task')

    print(df.info)
    
    df_reduzido = df[['NM_MUNICIPIO', 'NR_ZONA', 'NR_SECAO', 'NR_LOCAL_VOTACAO', 'QT_APTOS', 'QT_COMPARECIMENTO', 'QT_ABSTENCOES']]
    
    print(df_reduzido.head)
    
    return df_reduzido

# Função para remover os dados duplicados
def transform_remover_duplicadas(**kwargs):
    ti = kwargs['ti']

    df_reduzido = ti.xcom_pull(task_ids='transform_reduzir_task')

    df_unico = df_reduzido.drop_duplicates()

    print(df_unico.head())
    
    return df_unico

# Função para salvar o df para arquivo
def load(**kwargs):
    ti = kwargs['ti']

    df_unico = ti.xcom_pull(task_ids='transform_remover_duplicadas_task')

    df_unico.to_csv('/opt/airflow/data/tre_qualificado.csv', index=False, mode='w', sep = ';')

# Definir DAG
dag = DAG(
    'tre_dag',
    schedule_interval=None,
    start_date=datetime(2024, 7, 3),
    catchup=False
)

# Tarefa para descompactar o arquivo CSV
unzip_task = PythonOperator(
    task_id='unzip_task',
    python_callable=unzip,
    provide_context=True,
    dag=dag
)

# Tarefa para ler o arquivo CSV
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    provide_context=True,
    dag=dag
)

# Tarefa para transformar os dados
transform_reduzir_task = PythonOperator(
    task_id='transform_reduzir_task',
    python_callable=transform_reduzir,
    provide_context=True,
    dag=dag
)

# Tarefa para transformar os dados
transform_remover_duplicadas_task = PythonOperator(
    task_id='transform_remover_duplicadas_task',
    python_callable=transform_remover_duplicadas,
    provide_context=True,
    dag=dag
)

# Tarefa para salvar os dados no arquivo
load_task = PythonOperator(
    task_id='load_task',
    python_callable=load,
    provide_context=True,
    dag=dag
)

(unzip_task >> extract_task >> transform_reduzir_task >> transform_remover_duplicadas_task >> load_task)
