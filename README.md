# Airflow Windows
Windows ou Linux!

## Pré-requisitos
1. Docker instalado [https://docs.docker.com/engine/install/](https://docs.docker.com/engine/install/);
2. Comando `docker-compose` funcional;

## Rodando o projeto

1. Clone o projeto:
```bash
  git clone https://github.com/thiagosegato/airflow-windows.git
```

2. Acesse a pasta:
```bash
  cd airflow-windows
```

2.1. (Passo Opcional) A partir daqui você poderá optar a utilizar o Vscode. Caso prefira esse passo execute o comando abaixo para abrir a IDE:
```
code .
```
* Dica: Você pode iniciar um terminal dentro do próprio Vscode para continuar a configurar o projeto!

3. Crie um ambiente virtual python:
```
docker-compose up
```

## Acessando a ferramenta
Acesse [http://localhost:8080](http://localhost:8080)<br>
Usuário: admin<br>
Senha: Copie a senha no arquivo `standalone_admin_password.txt`

## Links úteis

- [Agendamentos e rodagens das DAGs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html)
- [Crontab Guru](https://crontab.guru/)
- [NBA Data Crawler](https://github.com/caiocolares/nba-crawler-airflow)