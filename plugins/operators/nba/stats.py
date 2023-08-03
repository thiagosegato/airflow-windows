import requests
import csv
from pathlib import Path
from os.path import join 
from datetime import datetime

from airflow.models import BaseOperator, DAG, TaskInstance


class NBAStatsOperator(BaseOperator):
    

    template_fields = ['process_date']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = "https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba/statistics/byathlete?region=us&lang=en&contentorigin=espn&isqualified=true&page={page}&limit=50&sort=general.avgMinutes%3Adesc&season={year}&seasontype=2"
        self.process_date = "{{ ds }}"

    def request_data(self):
        print('Requesting page {page}'.format(page=self.page))
        resp = requests.get(self.url.format(year=self.year, page=self.page))
        self.page += 1
        return resp.json()

    def get_header(self, data: dict):
        header = ["year","name", "playerId", "playerSlug", "position", "team", "status"]
        header.extend(next(x['names'] for x in data['categories'] if x['name']=='general'))
        header.extend(next(x['names'] for x in data['categories'] if x['name']=='offensive'))
        header.extend(next(x['names'] for x in data['categories'] if x['name']=='defensive'))
        return header

    def get_lines(self, data: dict):
        lines = []
        for athlete in data['athletes']:
            line = [self.year,
                    athlete['athlete']['displayName'],
                    athlete['athlete']['id'],
                    athlete['athlete']['slug'],
                    athlete['athlete']['position']['abbreviation'],
                    athlete['athlete']['teamShortName'],
                    athlete['athlete']['status']['type'] ]
            #alterado para garantir ordem de dados
            #for c in a['categories']:
            #  line.extend([x for x in c['totals']])
            line.extend(next(filter(lambda x: x['name'] == 'general',   athlete['categories']))['values'])
            line.extend(next(filter(lambda x: x['name'] == 'offensive', athlete['categories']))['values'])
            line.extend(next(filter(lambda x: x['name'] == 'defensive', athlete['categories']))['values'])
            lines.append(line)
        return lines
    
    def execute(self, context):
        year = datetime.strptime(self.process_date, '%Y-%m-%d').year
        self.year = year
        self.page = 1
        self.file_name = join("data_lake/nba/season_{start_year}_{end_year}".
                                format(start_year=self.year, end_year=self.year + 1),
                              "stats.csv")
        self.create_parent_folder()
        content = []
        data = self.request_data()
        header = self.get_header(data)
        while self.page <= data['pagination']['pages']:
            lines = self.get_lines(data)
            content.extend(lines)
            data = self.request_data()
        with open(self.file_name, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(content)

    def create_parent_folder(self):
        (Path(self.file_name).parent).mkdir(parents=True, exist_ok=True)



if __name__ == "__main__":
    with DAG(dag_id = "SalaryTest", start_date=datetime.now()) as dag:
        NBAStatsOperator(task_id="test_run", process_date="{{ ds }}")
    dag.test()