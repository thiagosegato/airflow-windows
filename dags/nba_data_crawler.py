from operators.nba.bio import NBABioOperator
from operators.nba.stats import NBAStatsOperator
from operators.nba.salary import NBASalaryOperator
from operators.nba.merge import NBAMergeOperator
from datetime import datetime
from airflow import DAG

# Autor: @caiocolares 
# Projeto Original:
# https://github.com/caiocolares/nba-crawler-airflow

with DAG(
    dag_id='nba_data_crawler',
    description='NBA Data Crawler',
    start_date=datetime(1999,1,1), schedule="@yearly") as dag:

    estatisticas = NBAStatsOperator(task_id="estatisticas")
    salario = NBASalaryOperator(task_id="salario")
    biografia = NBABioOperator(task_id="biografia")
    unifica_arquivos = NBAMergeOperator(task_id="unifica_arquivos")

    [estatisticas, salario] >> biografia >> unifica_arquivos