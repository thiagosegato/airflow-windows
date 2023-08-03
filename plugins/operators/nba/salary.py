import requests
from bs4 import BeautifulSoup
import csv
from pathlib import Path
from os.path import join 
from datetime import datetime

from airflow.models.baseoperator import BaseOperator
from airflow.models import DAG, TaskInstance
from airflow.utils.decorators import apply_defaults

class NBASalaryOperator(BaseOperator):
    
    template_fields = ['process_date']

    @apply_defaults
    def __init__(self,  
                 *args, 
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)


        self.process_date = "{{ ds }}"
        self.url = 'https://www.espn.com/nba/salaries/_/year/{year}/page/{page}/seasontype/4'

        

    def execute(self, context):
        year = datetime.strptime(self.process_date, '%Y-%m-%d').year
        
        self.start_year = year
        self.end_year = year + 1
        self.page = 1
        self.file_name = join("data_lake/nba/season_{start_year}_{end_year}".
                                format(start_year=self.start_year, end_year=self.end_year),
                              "salary.csv")

        self.create_parent_folder()
        with open(self.file_name, 'w') as f:
            csv.delimiter = ';'
            writer = csv.writer(f)
            header = ['year','ranking','playerId', 'playerSlug','name', 'position', 'team', 'salary']
            writer.writerow(header)

            content = self.request_page()
            while content:
                writer.writerows(content)
                self.page += 1
                content = self.request_page()


    def extract_detail_from_url(self, url):
        detail = url.split('/')
        return detail[-2], detail[-1]

    def request_page(self):
        conteudo = []

        page = requests.get(self.url.format(year=self.end_year, page=self.page))
        soup = BeautifulSoup(page.text, 'lxml')
        table = soup.find('table', {"class": "tablehead"})

        for tr in table.find_all('tr'):
            if 'RK' not in tr.text:
                linha = [self.start_year]
                for i, td in enumerate(tr.find_all('td')):
                    if i == 1:
                        a = td.find('a', href=True)
                        playerId, playerSlug = self.extract_detail_from_url(a['href'])
                        linha.append(playerId)
                        linha.append(playerSlug)

                        linha.extend([ x.strip() for x in td.text.split(',')])
                    else:
                        linha.append(td.text)
                conteudo.append(linha)
        return conteudo

    def create_parent_folder(self):
        (Path(self.file_name).parent).mkdir(parents=True, exist_ok=True)



if __name__ == "__main__":
    with DAG(dag_id = "SalaryTest", start_date=datetime.now()) as dag:
        NBASalaryOperator(task_id="test_run", process_date="{{ ds }}")
    dag.test()