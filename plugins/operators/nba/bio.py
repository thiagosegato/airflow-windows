
import requests
from bs4 import BeautifulSoup
import re
import json
import pandas as pd

from os.path import join 
from datetime import datetime

from airflow.models import BaseOperator, DAG, TaskInstance

format_label = lambda x: re.sub(r'[^a-z]', '',x.strip().lower())


class NBABioOperator(BaseOperator):

    template_fields = ['process_date']
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.process_date = "{{ ds }}"

    def load_players(self):
        df = pd.read_csv(self.file_source)
        dfp = pd.DataFrame(df[['playerId', 'playerSlug']].value_counts().reset_index())
        return [self.request_bio(p['playerId'], p['playerSlug']) for _, p in dfp.iterrows()]


    # HT/WT, birthday, college, birthplace, draft info
    def request_bio(self, playerId, playerSlug):
        url = "https://www.espn.com/nba/player/bio/_/id/{id}/{slug}".format(id=playerId, slug=playerSlug)
        response = requests.get(url)
        html = response.content
        soup = BeautifulSoup(html, 'html.parser')
        biography_section = soup.find(class_='Card Bio')
        labels = ['playerId','playerSlug']
        values = [playerId, playerSlug]
        items = biography_section.find_all('div', class_='Bio__Item')
        for item in items:
            label_element = item.find('span', class_='Bio__Label')
            value_element = item.find('span', class_='flex-uniform')
            label = format_label(label_element.text)
            value = value_element.text.strip()
            labels.append(label)
            values.append(value)

        return dict(zip(labels, values))
    
    def execute(self, context):
        year = datetime.strptime(self.process_date, '%Y-%m-%d').year
        self.year = year
        self.file_source = join("data_lake/nba/season_{start_year}_{end_year}".
                                format(start_year=self.year, end_year=self.year + 1),
                              "stats.csv")
        self.file_name = join("data_lake/nba/season_{start_year}_{end_year}".
                                format(start_year=self.year, end_year=self.year + 1),
                              "bios.{extension}")
        
        players = self.load_players()
        with open(self.file_name.format(extension='json'), 'w') as f:
            json.dump(players, f)
        df = pd.read_json(self.file_name.format(extension='json'))
        df.to_csv(self.file_name.format(extension='csv'),index=False)


if __name__ == "__main__":
    with DAG(dag_id = "SalaryTest", start_date=datetime.now()) as dag:
        NBABioOperator(task_id="test_run", process_date="{{ ds }}")
    dag.test()

