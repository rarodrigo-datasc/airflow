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

### ETL

* `unzip_task`: Descompactar o arquivo `tre.zip` em `tre.csv` para tratamento dos dados;
* `extract_task`: Extrair os dados para um data frame;
* `transform_reduzir_task`: Reduzir os dados para colunas especificadas em código;
* `transform_remover_duplicadas_task`: Remover as duplicadas baseando-se no arquivo gerada pela task anterior;
* `load_task`: Carregar os dados para um arquivo como resultado da transformação.

### Comandos para iniciar a execução do projeto

* Inicializar

```bash
docker compose up airflow-init
```

* Subir os containers

```bash
docker compose up
```

* Derrubar os containers

```bash
docker compose down
```

* Limpar o ambiente dos containers

```bash
docker compose down --volumes --remove-orphans
```

## 5. Referências

* [Documentação para ambiente Airflow e Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
