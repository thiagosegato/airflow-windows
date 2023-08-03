import pandas as pd

from os.path import join
from datetime import datetime
from airflow.models import BaseOperator, TaskInstance, DAG

class NBAMergeOperator(BaseOperator):

    template_fields = ['process_date']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.process_date = "{{ ds }}"


    def execute(self, context):
        year = datetime.strptime(self.process_date, '%Y-%m-%d').year

        self.year = year
        self.base_path = "data_lake/nba/season_{start_year}_{end_year}".format(start_year=year, end_year=year+1)

        df_salary = pd.read_csv(join(self.base_path, "salary.csv"))
        df_stats = pd.read_csv(join(self.base_path, "stats.csv"))
        df_bio = pd.read_csv(join(self.base_path, "bios.csv"))

        df_stats.rename(columns={"team": "teamId", "position": "positionId"}, inplace=True)


        df_salary.drop(columns=['name','position','team'], inplace=True)
        df_salary.rename(columns={"ranking": "rankingSalary"}, inplace=True)

        merged = pd.merge(df_bio, df_stats, on=['playerId', 'playerSlug'])

        merged = pd.merge(merged, df_salary, on=['year', 'playerId', 'playerSlug'])

        merged.to_csv(join(self.base_path, "infos.csv"), index=False)


if __name__ == "__main__":
    with DAG(dag_id = "SalaryTest", start_date=datetime.now()) as dag:
        NBAMergeOperator(task_id="test_run", process_date="{{ ds }}")
    dag.test()
