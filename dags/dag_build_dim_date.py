from __future__ import annotations

import pendulum
import pandas as pd
from sqlalchemy import create_engine, text
import logging

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# --- CONFIGURAÇÕES ---
# String de conexão para o banco de dados da aplicação, usada pela função Python.
# O host 'mysql_db' é o nome do serviço no docker-compose.
DB_CONNECTION_STRING = "mysql+mysqlconnector://mysql_user:mysql_pass@mysql_db:3306/dados_api"
# Define o intervalo de datas que será gerado.
START_DATE = '2015-01-01'
END_DATE = '2025-12-31'

# --- LÓGICA PYTHON ---
def _populate_date_dimension():
    """
    Gera e insere um intervalo de datas na tabela dim_date.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(f"Gerando datas de {START_DATE} a {END_DATE}...")

    # Usa a biblioteca Pandas para criar um DataFrame com todas as datas no intervalo especificado.
    dates = pd.date_range(start=START_DATE, end=END_DATE, freq='D')
    df = pd.DataFrame(dates, columns=['full_date'])
    
    # Cria todas as colunas da dimensão (dia, mês, ano, etc.) a partir da data completa.
    df['date_id'] = df['full_date'].dt.strftime('%Y%m%d').astype(int)
    df['day_of_week'] = df['full_date'].dt.dayofweek + 1
    df['day_name'] = df['full_date'].dt.day_name()
    df['day_of_month'] = df['full_date'].dt.day
    df['day_of_year'] = df['full_date'].dt.dayofyear
    df['week_of_year'] = df['full_date'].dt.isocalendar().week.astype(int)
    df['month_of_year'] = df['full_date'].dt.month
    df['month_name'] = df['full_date'].dt.month_name()
    df['quarter_of_year'] = df['full_date'].dt.quarter
    df['year'] = df['full_date'].dt.year
    
    # Garante que as colunas estejam na ordem correta da tabela do banco de dados.
    df = df[[
        'date_id', 'full_date', 'day_of_week', 'day_name', 'day_of_month',
        'day_of_year', 'week_of_year', 'month_of_year', 'month_name',
        'quarter_of_year', 'year'
    ]]
    
    logging.info(f"{len(df)} linhas de data geradas.")

    # Cria a conexão com o banco de dados usando SQLAlchemy.
    engine = create_engine(DB_CONNECTION_STRING)
    try:
        # Abre uma conexão e inicia uma transação para garantir a integridade dos dados.
        with engine.connect() as connection:
            with connection.begin() as transaction:
                try:
                    # Limpa a tabela para garantir que não haja dados antigos.
                    logging.info("Limpando a tabela dim_date (TRUNCATE)...")
                    connection.execute(text("TRUNCATE TABLE dim_date;"))
                    
                    # Insere o DataFrame com as novas datas na tabela.
                    logging.info("Inserindo novos dados na tabela dim_date...")
                    df.to_sql('dim_date', con=connection, if_exists='append', index=False, chunksize=1000)
                    
                    # Confirma a transação, gravando os dados permanentemente.
                    transaction.commit()
                    logging.info("Transação commitada. Tabela dim_date populada com sucesso!")
                except Exception as e:
                    # Se ocorrer um erro, desfaz a transação.
                    logging.error(f"Erro durante a transação, revertendo... Erro: {e}")
                    transaction.rollback()
                    raise
    except Exception as e:
        logging.error(f"Ocorreu um erro geral ao conectar ou executar a transação: {e}")
        raise

# --- DEFINIÇÃO DA DAG ---
# Bloco que define as propriedades e a estrutura da DAG.
with DAG(
    # ID único da DAG, que aparece na interface do Airflow.
    dag_id="build_dim_date",
    # Data de início a partir da qual a DAG pode ser agendada.
    start_date=pendulum.datetime(2025, 7, 18, tz="America/Sao_Paulo"),
    # 'schedule=None' significa que esta DAG só roda quando acionada manualmente.
    schedule=None,
    # Impede a execução de agendamentos passados que foram perdidos.
    catchup=False,
    # Documentação que aparece na interface do Airflow.
    doc_md="Cria e popula a tabela de dimensão de tempo (dim_date).",
    # Etiquetas para organizar as DAGs na interface.
    tags=["dimensions", "date", "setup"],
) as dag:
    
    # Tarefa inicial que serve como um ponto de partida claro para o fluxo.
    start = EmptyOperator(task_id="start")

    # Tarefa principal que executa a função Python para popular a tabela.
    populate_table = PythonOperator(
        task_id="populate_date_dimension_table",
        python_callable=_populate_date_dimension,
    )

    # Tarefa final que serve como um ponto de conclusão claro para o fluxo.
    end = EmptyOperator(task_id="end")

    # Define a ordem de execução das tarefas: start -> populate_table -> end.
    start >> populate_table >> end
