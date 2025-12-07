from airflow import DAG 
from airflow.operators.python_operator import PythonOperator 
from datetime import datetime 
from functions import execute_sql_script    

OWNER = "dzheparovdaniil"

with DAG(
          dag_id=f'retention_mart_dag_{OWNER}',
          start_date = datetime(2024, 4, 21),
          schedule_interval='0 8 * * *',
          catchup=False,
          tags=[OWNER],
          default_args={
                    "owner": OWNER
          }
) as dag:

          retention_mart = PythonOperator( 
                  task_id = 'retention',
                  python_callable=execute_sql_script,
                  op_kwargs={'file_path': f'/opt/airflow/dags/{OWNER}/dags/sql/retention_mart'}
          )

retention_mart
