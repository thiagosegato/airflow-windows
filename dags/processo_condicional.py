from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow import DAG
import random

with DAG(
    dag_id='processo_condicional',
    description='Uma apresentação de uma DAG mais simples',
    start_date=days_ago(2), schedule="@once") as dag:

    @task()
    def inicio():
        print('Executando inicio')

    @task.branch()
    def condicao():
        decisao = random.randint(1, 2)
        if decisao == 1:
            return 'tarefa_A'
        else:
            return 'tarefa_B'
    
    @task()
    def tarefa_A():
        print('Executando tarefa A')

    @task()
    def tarefa_B():
        print('Executando tarefa B')

    inicio() >> condicao() >> [tarefa_A(), tarefa_B()]




    