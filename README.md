# Projeto Airflow-TRE/MT - `airflow`

Este projeto é um estudo de caso para o curso de Pós Graduação (IC-UFMT) em Gestão e Ciência de Dados da disciplina Fundamentos de Big Data.

Uma base de dados do TRE/MT sobre as eleições de 2022 será usada como fonte de dados de Big Data.

Além disso, o projeto fará uso da tecnologia Airflow para criação do fluxo ETL destes dados.

## 1. Equipe

* Lauro Cézzar;
* Marlon Tunes;
* Rodrigo Rodrigues Areco.

## 2. Base de Dados

* `tre.zip` contém a base de dados do TRE/MT sobre as eleições de 2022.

## 3. Código-fonte

* `tre_dag.py` é o arquivo principal desenvolvido em python com a biblioteca airflow para criação do fluxo ETL.

## 4. Execução

Comandos para iniciar a execução do projeto

* Inicializar

```bash
docker compose up airflow-init
```

* Subir os containeres

```bash
docker compose up
```

* Derrubar os containeres

```bash
docker compose down
```

* Limpar o ambiente dos containeres

```bash
docker compose down --volumes --remove-orphans
```

## 5. Referências

* [Airflow e Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
