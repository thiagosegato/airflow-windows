from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago
from airflow.decorators import task

# Acesse http://localhost:8080/dddd e crie a variável (my_password)

with DAG(dag_id='dados_sensiveis',
         start_date=days_ago(2),
         schedule=None):
    
    @task()
    def teste():
        senha = Variable.get('my_password')
        print('minha senha é:' + senha)
    
    teste()