from __future__ import annotations
import pendulum
from datetime import timedelta
import logging
import pandas as pd
from sqlalchemy import create_engine, desc
from faker import Faker
import random
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models.dagrun import DagRun
from airflow.utils.session import provide_session
from sqlalchemy.orm.session import Session

# --- FUNÇÃO AUXILIAR ---
# Esta função é usada pelos sensores para encontrar a data da última execução bem-sucedida
# de uma DAG externa, tornando a dependência entre DAGs mais flexível.
@provide_session
def get_most_recent_dag_run(logical_date, session: Session, **kwargs):
    # Pega o ID da DAG externa a partir do contexto da tarefa
    external_dag_id = kwargs["task"].external_dag_id
    # Busca no banco de dados do Airflow pela última execução com estado 'success'
    dag_run = (
        session.query(DagRun)
        .filter(DagRun.dag_id == external_dag_id, DagRun.state == "success")
        .order_by(desc(DagRun.execution_date))
        .first()
    )
    if not dag_run:
        return None
    # Retorna a data de execução encontrada, que o sensor usará para verificar o status.
    return dag_run.execution_date

# --- CONFIGURAÇÕES ---
# String de conexão para o banco de dados da aplicação, usada pela função Python.
DB_CONNECTION_STRING = "mysql+mysqlconnector://mysql_user:mysql_pass@mysql_db:3306/dados_api"

# --- LÓGICA PYTHON (COM PREÇOS POR MARCA) ---
def _generate_fact_sales():
    """
    Função principal de ETL para a tabela de fatos:
    1. Lê as tabelas de dimensão já criadas.
    2. Gera 50.000 registros de vendas sintéticos com lógica de negócio.
    3. Carrega os dados gerados na tabela 'fact_sales'.
    """
    logging.info("Iniciando geração de dados de vendas sintéticos com preços por marca...")
    engine = create_engine(DB_CONNECTION_STRING)
    
    # Lê as tabelas de dimensão para obter IDs válidos e informações para a lógica de negócio.
    df_models = pd.read_sql("SELECT model_id, make_name, segmento, motorizacao FROM dim_vehicle_models", engine)
    df_dealers = pd.read_sql("SELECT dealer_id, state FROM dim_dealers", engine)
    
    if df_models.empty or df_dealers.empty:
        raise ValueError("Uma ou mais tabelas de dimensão estão vazias.")

    # --- LÓGICA DE PESOS POR ESTADO ---
    # Simula um cenário de vendas mais realista, onde alguns estados vendem mais que outros.
    state_weights = {'SP': 40, 'RJ': 20, 'MG': 15, 'RS': 5, 'BA': 5, 'DF': 5, 'AM': 4, 'GO': 3, 'PE': 3}
    df_dealers['weight'] = df_dealers['state'].map(state_weights).fillna(1)
    dealer_ids = df_dealers['dealer_id'].tolist()
    dealer_weights = df_dealers['weight'].tolist()
    
    NUM_SALES = 50000
    
    # Sorteia os modelos para cada uma das 50.000 vendas.
    sales_df_generated = pd.DataFrame({
        'model_id': random.choices(df_models['model_id'].tolist(), k=NUM_SALES)
    })
    # Junta as informações do modelo (marca, segmento, etc.) com cada venda.
    sales_df_generated = pd.merge(sales_df_generated, df_models, on='model_id', how='left')
    
    # --- LÓGICA DE GERAÇÃO DE PREÇO POR MARCA ---
    # Define faixas de preço diferentes para cada marca para tornar os dados mais realistas.
    brand_price_ranges = {
        'FORD': (40000, 90000), 'TOYOTA': (45000, 100000), 'HONDA': (42000, 95000),
        'CHEVROLET': (38000, 88000), 'NISSAN': (41000, 92000), 'HYUNDAI': (39000, 90000), 'KIA': (39000, 91000),
        'BMW': (120000, 300000), 'MERCEDES-BENZ': (130000, 320000), 'AUDI': (125000, 280000),
        'TESLA': (150000, 350000), 'RIVIAN': (200000, 400000), 'LUCID': (220000, 450000),
        'default': (30000, 70000) # Um padrão para qualquer outra marca
    }

    def generate_price(row):
        make = row['make_name']
        price_range = brand_price_ranges.get(make, brand_price_ranges['default'])
        base_price = random.uniform(price_range[0], price_range[1])
        
        # Adiciona um bônus de preço para veículos elétricos e híbridos.
        if row['motorizacao'] == 'Elétrico':
            base_price *= 1.1
        elif row['motorizacao'] == 'Híbrido':
            base_price *= 1.05
            
        return round(base_price, 2)

    logging.info("Aplicando lógica de preços inteligentes por marca...")
    sales_df_generated['sale_price'] = sales_df_generated.apply(generate_price, axis=1)

    # Instancia o Faker para gerar datas aleatórias.
    fake = Faker()
    
    # Adiciona as colunas restantes à tabela de fatos.
    sales_df_generated['quantity_sold'] = 1
    sales_df_generated['date_id'] = [int(fake.date_between(start_date=pd.to_datetime("2015-01-01"), end_date=pd.to_datetime("2024-12-31")).strftime('%Y%m%d')) for _ in range(NUM_SALES)]
    sales_df_generated['dealer_id'] = random.choices(dealer_ids, weights=dealer_weights, k=NUM_SALES)
    
    # Seleciona e ordena as colunas para a carga no banco de dados.
    final_df = sales_df_generated[['date_id', 'model_id', 'dealer_id', 'quantity_sold', 'sale_price']]
    
    # Carrega o DataFrame final na tabela 'fact_sales', substituindo dados antigos.
    final_df.to_sql('fact_sales', engine, if_exists='replace', index=False, chunksize=1000)
    logging.info(f"{len(final_df)} registros de vendas gerados e carregados com sucesso!")

# --- DAG DE CRIAÇÃO DA FATO SALES ---
# Script SQL para criar a tabela de dimensão de tempo, caso ela não exista.
CREATE_DATE_DIM_SQL = "CREATE TABLE IF NOT EXISTS dim_date (date_id INT PRIMARY KEY, full_date DATE NOT NULL, day_of_week INT NOT NULL, day_name VARCHAR(10) NOT NULL, day_of_month INT NOT NULL, day_of_year INT NOT NULL, week_of_year INT NOT NULL, month_of_year INT NOT NULL, month_name VARCHAR(10) NOT NULL, quarter_of_year INT NOT NULL, year INT NOT NULL);"

# Bloco que define as propriedades e a estrutura da DAG.
with DAG(
    dag_id="build_fact_sales",
    start_date=pendulum.datetime(2025, 7, 18, tz="America/Sao_Paulo"),
    schedule_interval="@daily", # Define que a DAG deve rodar diariamente.
    catchup=False,
    tags=["facts", "sales"],
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    # Tarefa que garante que a estrutura da tabela de datas exista.
    create_date_dimension = MySqlOperator(
        task_id="create_date_dimension_if_not_exists", mysql_conn_id="mysql_default",
        sql=CREATE_DATE_DIM_SQL,
    )

    # Sensor que espera a DAG 'build_dim_dealers_fictitious' terminar com sucesso.
    wait_for_dealers_dim = ExternalTaskSensor(
        task_id="wait_for_dealers_dimension", external_dag_id="build_dim_dealers_fictitious",
        external_task_id="end", execution_date_fn=get_most_recent_dag_run,
        timeout=600, allowed_states=['success'], mode='reschedule'
    )
    
    # Sensor que espera a DAG 'build_dim_models' terminar com sucesso.
    wait_for_models_dim = ExternalTaskSensor(
        task_id="wait_for_models_dimension", external_dag_id="build_dim_models",
        external_task_id="end", execution_date_fn=get_most_recent_dag_run,
        timeout=1800, allowed_states=['success'], mode='reschedule'
    )
    
    # Tarefa que executa a função Python para gerar os dados de vendas.
    generate_facts = PythonOperator(
        task_id="generate_sales_facts", python_callable=_generate_fact_sales
    )
    
    end = EmptyOperator(task_id="end")

    # Define a ordem de execução: as 3 tarefas iniciais rodam em paralelo.
    # A tarefa 'generate_facts' só começa depois que as 3 terminarem com sucesso.
    start >> [create_date_dimension, wait_for_dealers_dim, wait_for_models_dim]
    [create_date_dimension, wait_for_dealers_dim, wait_for_models_dim] >> generate_facts
    generate_facts >> end
