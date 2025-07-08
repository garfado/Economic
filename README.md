Data Pipeline — Coleta de Indicadores Econômicos (Investing.com)

Sumário

Descrição do Projeto
Requisitos do Desafio
Arquitetura da Solução
Execução do Pipeline
Tratamento de Bloqueios e Limitações
Estrutura dos Dados
Armazenamento e Banco de Dados
Orquestração e Containerização
Possíveis Evoluções
Como Rodar Localmente
Considerações Finais

Descrição do Projeto
Este projeto implementa um pipeline automatizado para coleta e organização de três indicadores econômicos públicos do site Investing.com:

Chinese Caixin Services Index (mensal desde 2012)

Bloomberg Commodity Index (mensal desde 1991)

USD/CNY (mensal desde 1991)

O objetivo é centralizar e padronizar esses dados para apoiar análises preditivas sobre o preço do papel, conforme solicitado por um Data Scientist da equipe.

Requisitos do Desafio
Coletar dados automaticamente das três páginas indicadas.

Estruturar os dados em tabelas conforme especificação.

Armazenar em banco de dados relacional (exceto SQLite).

Demonstrar habilidades em orquestração de pipelines (Airflow), containerização (Docker) e cloud (quando possível).

Documentar o processo para reprodutibilidade.

Arquitetura da Solução
Linguagem: Python 3.x

Principais bibliotecas:

requests/cloudscraper para coleta de dados web

pandas para manipulação e limpeza dos dados

sqlalchemy/psycopg2 para integração com banco de dados

logging para monitoramento e troubleshooting

Orquestração: Apache Airflow (Docker)

Banco de Dados: PostgreSQL (Docker)

Containerização: Docker Compose

Execução do Pipeline
1. Coleta dos Dados
Requisições HTTP simulando navegador (User-Agent e headers) para tentar contornar bloqueios básicos.

Extração de tabelas HTML com pandas.read_html.

Tratamento de erros e logging detalhado:

Status HTTP e início do HTML são registrados.

Falhas como status 403 (Cloudflare) são capturadas e logadas, sem interromper o pipeline.

2. Estruturação dos Dados
Dados organizados em DataFrames com os campos:

Chinese Caixin Services Index: date, actual_state, close, forecast

Bloomberg Commodity Index: date, close, open, high, low, volume

USD/CNY: date, close, open, high, low, volume

Conversão e padronização de datas, tipos e nomes de colunas.

Filtros temporais conforme solicitado.

3. Armazenamento
Exportação para CSV (etapa intermediária para debug e validação).

Carga em banco PostgreSQL (com SQLAlchemy ou psycopg2), permitindo consultas SQL e integração com BI.

Tratamento de Bloqueios e Limitações
Investing.com utiliza Cloudflare:

Em alguns casos, o site bloqueia scraping automatizado, retornando status 403 e página de proteção.

O pipeline identifica e loga essas situações, evitando falhas não tratadas.

Evolução sugerida: uso de cloudscraper, Selenium ou Playwright para contornar bloqueios mais avançados, ou busca por APIs alternativas.

Estrutura dos Dados
Chinese Caixin Services Index
date	actual_state	close	forecast
yyyy-mm-dd	valor	valor	valor

Bloomberg Commodity Index
date	close	open	high	low	volume
yyyy-mm-dd	valor	...	...	...	...

USD/CNY
date	close	open	high	low	volume
yyyy-mm-dd	valor	...	...	...	...

Armazenamento e Banco de Dados
PostgreSQL rodando em container Docker.

Scripts SQL para criação das tabelas.

Scripts Python para carga dos dados (insert/update).

Orquestração e Containerização
Apache Airflow para agendamento e monitoramento dos jobs ETL.

Docker Compose para facilitar o setup dos serviços (Airflow, Postgres, Redis).

Logs centralizados para troubleshooting.

Possíveis Evoluções
Implementar scraping com Selenium/Playwright para superar bloqueios do Cloudflare.

Automatizar deploy em cloud (AWS ECS, GCP Cloud Composer, etc).

Expor os dados via API REST.

Monitoramento e alertas para falhas no pipeline.

Como Rodar Localmente
Clone o repositório

Configure variáveis de ambiente (usuário/senha do Postgres)

Suba os containers

text
docker-compose up -d
Acesse o Airflow em http://localhost:8080 e acione o DAG manualmente ou agende.

run_bloomberg_commodity_index_script

![image](https://github.com/user-attachments/assets/cb3692b1-a597-46b7-9d4f-75c3c2181732)

324627d888b5
*** Found local files:
***   * /opt/airflow/logs/dag_id=bloomberg_commodity_index_etl/run_id=manual__2025-07-08T14:32:58.036097+00:00/task_id=run_bloomberg_commodity_index_script/attempt=1.log
[2025-07-08, 14:33:00 UTC] {taskinstance.py:1957} INFO - Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: bloomberg_commodity_index_etl.run_bloomberg_commodity_index_script manual__2025-07-08T14:32:58.036097+00:00 [queued]>
[2025-07-08, 14:33:00 UTC] {taskinstance.py:1957} INFO - Dependencies all met for dep_context=requeueable deps ti=<TaskInstance: bloomberg_commodity_index_etl.run_bloomberg_commodity_index_script manual__2025-07-08T14:32:58.036097+00:00 [queued]>
[2025-07-08, 14:33:00 UTC] {taskinstance.py:2171} INFO - Starting attempt 1 of 2
[2025-07-08, 14:33:00 UTC] {taskinstance.py:2192} INFO - Executing <Task(PythonOperator): run_bloomberg_commodity_index_script> on 2025-07-08 14:32:58.036097+00:00
[2025-07-08, 14:33:00 UTC] {standard_task_runner.py:60} INFO - Started process 423 to run task
[2025-07-08, 14:33:00 UTC] {standard_task_runner.py:87} INFO - Running: ['***', 'tasks', 'run', 'bloomberg_commodity_index_etl', 'run_bloomberg_commodity_index_script', 'manual__2025-07-08T14:32:58.036097+00:00', '--job-id', '160', '--raw', '--subdir', 'DAGS_FOLDER/bloomberg_commodity_index_dag.py', '--cfg-path', '/tmp/tmpq99bwd81']
[2025-07-08, 14:33:00 UTC] {standard_task_runner.py:88} INFO - Job 160: Subtask run_bloomberg_commodity_index_script
[2025-07-08, 14:33:01 UTC] {task_command.py:423} INFO - Running <TaskInstance: bloomberg_commodity_index_etl.run_bloomberg_commodity_index_script manual__2025-07-08T14:32:58.036097+00:00 [running]> on host 324627d888b5
[2025-07-08, 14:33:01 UTC] {taskinstance.py:2481} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='***' AIRFLOW_CTX_DAG_ID='bloomberg_commodity_index_etl' AIRFLOW_CTX_TASK_ID='run_bloomberg_commodity_index_script' AIRFLOW_CTX_EXECUTION_DATE='2025-07-08T14:32:58.036097+00:00' AIRFLOW_CTX_TRY_NUMBER='1' AIRFLOW_CTX_DAG_RUN_ID='manual__2025-07-08T14:32:58.036097+00:00'
[2025-07-08, 14:33:30 UTC] {bloomberg_commodity_index_etl.py:40} INFO - Iniciando ETL Bloomberg Commodity Index
[2025-07-08, 14:33:37 UTC] {bloomberg_commodity_index_etl.py:77} INFO - Baixado: 01/01/1991 até 01/01/1996
[2025-07-08, 14:33:40 UTC] {bloomberg_commodity_index_etl.py:77} INFO - Baixado: 02/01/1996 até 02/01/2001
[2025-07-08, 14:33:43 UTC] {bloomberg_commodity_index_etl.py:77} INFO - Baixado: 03/01/2001 até 03/01/2006
[2025-07-08, 14:33:54 UTC] {bloomberg_commodity_index_etl.py:77} INFO - Baixado: 04/01/2006 até 04/01/2011
[2025-07-08, 14:33:57 UTC] {bloomberg_commodity_index_etl.py:77} INFO - Baixado: 05/01/2011 até 05/01/2016
[2025-07-08, 14:34:00 UTC] {bloomberg_commodity_index_etl.py:77} INFO - Baixado: 06/01/2016 até 06/01/2021
[2025-07-08, 14:34:03 UTC] {bloomberg_commodity_index_etl.py:77} INFO - Baixado: 07/01/2021 até 08/07/2025
[2025-07-08, 14:34:05 UTC] {bloomberg_commodity_index_etl.py:112} INFO - Linhas finais agregadas: 413
[2025-07-08, 14:34:06 UTC] {bloomberg_commodity_index_etl.py:117} INFO - Dados salvos na tabela 'bloomberg_commodity_index' com sucesso.
[2025-07-08, 14:34:06 UTC] {bloomberg_commodity_index_etl.py:122} INFO - ETL Bloomberg Commodity Index finalizado com sucesso.
[2025-07-08, 14:34:06 UTC] {python.py:201} INFO - Done. Returned value was: None
[2025-07-08, 14:34:06 UTC] {taskinstance.py:1138} INFO - Marking task as SUCCESS. dag_id=bloomberg_commodity_index_etl, task_id=run_bloomberg_commodity_index_script, execution_date=20250708T143258, start_date=20250708T143300, end_date=20250708T143406
[2025-07-08, 14:34:06 UTC] {local_task_job_runner.py:234} INFO - Task exited with return code 0
[2025-07-08, 14:34:06 UTC] {taskinstance.py:3281} INFO - 0 downstream tasks scheduled from follow-on schedule check




run_chinese_caixin_services_index_etl_script

![image](https://github.com/user-attachments/assets/75f64fc1-0237-4cdc-925c-977c6c99ddc3)

324627d888b5
*** Found local files:
***   * /opt/airflow/logs/dag_id=chinese_caixin_services_index_etl/run_id=manual__2025-07-08T14:33:00.400690+00:00/task_id=run_chinese_caixin_services_index_etl_script/attempt=2.log
[2025-07-08, 14:38:44 UTC] {taskinstance.py:1957} INFO - Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: chinese_caixin_services_index_etl.run_chinese_caixin_services_index_etl_script manual__2025-07-08T14:33:00.400690+00:00 [queued]>
[2025-07-08, 14:38:44 UTC] {taskinstance.py:1957} INFO - Dependencies all met for dep_context=requeueable deps ti=<TaskInstance: chinese_caixin_services_index_etl.run_chinese_caixin_services_index_etl_script manual__2025-07-08T14:33:00.400690+00:00 [queued]>
[2025-07-08, 14:38:44 UTC] {taskinstance.py:2171} INFO - Starting attempt 2 of 2
[2025-07-08, 14:38:44 UTC] {taskinstance.py:2192} INFO - Executing <Task(PythonOperator): run_chinese_caixin_services_index_etl_script> on 2025-07-08 14:33:00.400690+00:00
[2025-07-08, 14:38:44 UTC] {standard_task_runner.py:60} INFO - Started process 611 to run task
[2025-07-08, 14:38:44 UTC] {standard_task_runner.py:87} INFO - Running: ['***', 'tasks', 'run', 'chinese_caixin_services_index_etl', 'run_chinese_caixin_services_index_etl_script', 'manual__2025-07-08T14:33:00.400690+00:00', '--job-id', '163', '--raw', '--subdir', 'DAGS_FOLDER/chinese_caixin_services_index_dag.py', '--cfg-path', '/tmp/tmpibn_guox']
[2025-07-08, 14:38:44 UTC] {standard_task_runner.py:88} INFO - Job 163: Subtask run_chinese_caixin_services_index_etl_script
[2025-07-08, 14:38:44 UTC] {task_command.py:423} INFO - Running <TaskInstance: chinese_caixin_services_index_etl.run_chinese_caixin_services_index_etl_script manual__2025-07-08T14:33:00.400690+00:00 [running]> on host 324627d888b5
[2025-07-08, 14:38:46 UTC] {taskinstance.py:2481} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='***' AIRFLOW_CTX_DAG_ID='chinese_caixin_services_index_etl' AIRFLOW_CTX_TASK_ID='run_chinese_caixin_services_index_etl_script' AIRFLOW_CTX_EXECUTION_DATE='2025-07-08T14:33:00.400690+00:00' AIRFLOW_CTX_TRY_NUMBER='2' AIRFLOW_CTX_DAG_RUN_ID='manual__2025-07-08T14:33:00.400690+00:00'
[2025-07-08, 14:38:47 UTC] {chinese_caixin_services_index_etl.py:14} INFO - Status code da resposta: 403
[2025-07-08, 14:38:47 UTC] {chinese_caixin_services_index_etl.py:17} INFO - Primeiros 500 caracteres do HTML retornado:
<!DOCTYPE html>
<!--[if lt IE 7]> <html class="no-js ie6 oldie" lang="en-US"> <![endif]-->
<!--[if IE 7]>    <html class="no-js ie7 oldie" lang="en-US"> <![endif]-->
<!--[if IE 8]>    <html class="no-js ie8 oldie" lang="en-US"> <![endif]-->
<!--[if gt IE 8]><!--> <html class="no-js" lang="en-US"> <!--<![endif]-->
<head>
<title>Attention Required! | Cloudflare</title>
<meta charset="UTF-8" />
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
<meta http-equiv="X-UA-Compatible" 
[2025-07-08, 14:38:47 UTC] {chinese_caixin_services_index_etl.py:26} ERROR - Nenhuma tabela encontrada no HTML! Erro: No tables found
[2025-07-08, 14:38:47 UTC] {python.py:201} INFO - Done. Returned value was: None
[2025-07-08, 14:38:48 UTC] {taskinstance.py:1138} INFO - Marking task as SUCCESS. dag_id=chinese_caixin_services_index_etl, task_id=run_chinese_caixin_services_index_etl_script, execution_date=20250708T143300, start_date=20250708T143844, end_date=20250708T143848
[2025-07-08, 14:38:48 UTC] {local_task_job_runner.py:234} INFO - Task exited with return code 0
[2025-07-08, 14:38:48 UTC] {taskinstance.py:3281} INFO - 0 downstream tasks scheduled from follow-on schedule check






chinese_services_index_usd_cny_script

![image](https://github.com/user-attachments/assets/fa357fa2-9bea-47ab-88b4-2e4376ee071e)

Verifique os dados no banco via psql, DBeaver ou outro cliente.

SELECT * FROM log WHERE dag_id = 'bloomberg_commodity_index_etl' ORDER BY dttm DESC LIMIT 5;       

SELECT * FROM log WHERE dag_id = 'chinese_caixin_services_index_etl' ORDER BY dttm DESC LIMIT 5;

SELECT * FROM log WHERE dag_id = 'chinese_cervices_index_usd_cny_etl' ORDER BY dttm DESC LIMIT 5;


Considerações Finais
O pipeline foi implementado de forma robusta, com tratamento de erros e logs detalhados.

O bloqueio do site por Cloudflare foi identificado e documentado, com sugestões de evolução.

O projeto está pronto para ser expandido para ambientes produtivos e integrações mais avançadas.

Se quiser, posso gerar os arquivos de exemplo (docker-compose.yml, scripts Python, DAG do Airflow, etc.) para complementar seu repositório!

