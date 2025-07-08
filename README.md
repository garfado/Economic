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

run_chinese_caixin_services_index_etl_script

![image](https://github.com/user-attachments/assets/75f64fc1-0237-4cdc-925c-977c6c99ddc3)

chinese_services_index_usd_cny_script

![image](https://github.com/user-attachments/assets/fa357fa2-9bea-47ab-88b4-2e4376ee071e)

Verifique os dados no banco via psql, DBeaver ou outro cliente.

SELECT * FROM log WHERE dag_id = 'bloomberg_commodity_index_etl' ORDER BY dttm DESC LIMIT 5;

1181 | 2025-07-08 15:23:22.677396+00 | bloomberg_commodity_index_etl |                                      |           | grid       |     
                          | admin | Admin User         | [('dag_id', 'bloomberg_commodity_index_etl')]
1173 | 2025-07-08 15:21:14.793292+00 | bloomberg_commodity_index_etl |                                      |           | graph_data |     
                          | admin | Admin User         | [('dag_id', 'bloomberg_commodity_index_etl')]
1172 | 2025-07-08 15:21:11.358456+00 | bloomberg_commodity_index_etl | run_bloomberg_commodity_index_script |           | clear      | 2025-07-08 14:32:58.036097+00 | admin | Admin User         | [('dag_id', 'bloomberg_commodity_index_etl'), ('dag_run_id', 'manual__2025-07-08T14:32:58.036097+00:00'), ('confirmed', 'false'), ('execution_date', '2025-07-08T14:32:58.036097+00:00'), ('past', 'false'), ('future', 'false'), ('upstream', 'false'), ('downstream', 'true'), ('recursive', 'true'), ('only_failed', 'false'), ('task_id', 'run_bloomberg_commodity_index_script')]
1171 | 2025-07-08 15:21:11.34229+00  | bloomberg_commodity_index_etl | run_bloomberg_commodity_index_script |           | confirm    |     
                          | admin | Admin User         | [('dag_id', 'bloomberg_commodity_index_etl'), ('dag_run_id', 'manual__2025-07-08T14:32:58.036097+00:00'), ('past', 'false'), ('future', 'false'), ('upstream', 'false'), ('downstream', 'false'), ('state', 'success'), ('task_id', 'run_bloomberg_commodity_index_script')]
1170 | 2025-07-08 15:21:08.046297+00 | bloomberg_commodity_index_etl |                                      |           | grid       |     
                          | admin | Admin User         | [('dag_id', 'bloomberg_commodity_index_etl')]



                          

SELECT * FROM log WHERE dag_id = 'chinese_caixin_services_index_etl' ORDER BY dttm DESC LIMIT 10;

SELECT * FROM log WHERE dag_id = 'chinese_cervices_index_usd_cny_etl' ORDER BY dttm DESC LIMIT 10;






Considerações Finais
O pipeline foi implementado de forma robusta, com tratamento de erros e logs detalhados.

O bloqueio do site por Cloudflare foi identificado e documentado, com sugestões de evolução.

O projeto está pronto para ser expandido para ambientes produtivos e integrações mais avançadas.

Se quiser, posso gerar os arquivos de exemplo (docker-compose.yml, scripts Python, DAG do Airflow, etc.) para complementar seu repositório!

