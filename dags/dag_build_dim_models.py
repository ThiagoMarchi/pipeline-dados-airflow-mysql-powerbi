from __future__ import annotations
import pendulum
from datetime import timedelta
import logging
import time
import requests
import pandas as pd
from sqlalchemy import create_engine
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.empty import EmptyOperator

# --- CONFIGURAÇÕES GLOBAIS ---
# String de conexão para o banco de dados da aplicação
DB_CONNECTION_STRING = "mysql+mysqlconnector://mysql_user:mysql_pass@mysql_db:3306/dados_api"
# URL base da API de veículos
NHTSA_API_URL = "https://vpic.nhtsa.dot.gov/api/vehicles"

# --- LÓGICA DE EXTRAÇÃO E CARGA (ETL) ---
def _extract_load_models():
    """
    Função principal de ETL:
    1. Extrai dados de fabricantes e modelos da API da NHTSA.
    2. Normaliza e enriquece os dados.
    3. Carrega o resultado em uma tabela de staging no MySQL.
    """
    logging.info("Iniciando extração de modelos de veículos da NHTSA.")
    # Lista de palavras-chave para encontrar as marcas de interesse
    BRAND_KEYWORDS = [
        'Honda', 'Toyota', 'Ford', 'Chevrolet', 'GMC', 'BMW', 'Mercedes-Benz', 
        'Tesla', 'Volkswagen', 'Audi', 'Subaru', 'Mazda', 'Hyundai', 'Kia', 'Nissan',
        'Rivian', 'Lucid', 'Polestar', 'Fisker', 'BYD', 'GWM'
    ]
    # Intervalo de anos para a busca de modelos
    YEARS_TO_FETCH = range(2015, 2025)
    
    # Busca a lista completa de todos os fabricantes registrados para obter seus IDs
    response_makes = requests.get(f"{NHTSA_API_URL}/GetAllManufacturers?format=json")
    response_makes.raise_for_status()
    df_makes = pd.DataFrame(response_makes.json()['Results'])
    
    # Filtra a lista geral para encontrar os IDs e nomes oficiais dos fabricantes que nos interessam
    target_manufacturers = []
    for brand in BRAND_KEYWORDS:
        search_term = 'GREAT WALL' if brand == 'GWM' else brand
        found_df = df_makes[df_makes['Mfr_Name'].str.contains(search_term, case=False, na=False)]
        if not found_df.empty:
            for _, row in found_df.iterrows():
                target_manufacturers.append({'mfr_id': row['Mfr_ID'], 'brand_normalized': brand.upper()})
    df_targets = pd.DataFrame(target_manufacturers).drop_duplicates(subset=['mfr_id'])
    
    # Itera sobre cada fabricante e cada ano para buscar os modelos de veículos
    all_models_data = []
    for year in YEARS_TO_FETCH:
        for _, target in df_targets.iterrows():
            try:
                url = f"{NHTSA_API_URL}/GetModelsForMakeIdYear/makeId/{target['mfr_id']}/modelyear/{year}?format=json"
                models_data = requests.get(url).json()['Results']
                for model in models_data:
                    # Adiciona a marca normalizada (ex: 'FORD') a cada registro de modelo
                    model['Mfr_Name'] = target['brand_normalized']
                all_models_data.extend(models_data)
                time.sleep(0.1) # Pequena pausa para não sobrecarregar a API
            except Exception:
                continue
    
    # Consolida todos os dados em um único DataFrame e remove duplicatas
    df_final_models = pd.DataFrame(all_models_data).drop_duplicates()
    # Conecta ao banco de dados e carrega o DataFrame na tabela de staging, substituindo os dados antigos
    engine = create_engine(DB_CONNECTION_STRING)
    df_final_models.to_sql("stg_vehicle_models", engine, if_exists='replace', index=False, chunksize=1000)
    logging.info(f"Carga de {len(df_final_models)} modelos para staging concluída.")

# --- SCRIPT SQL DE TRANSFORMAÇÃO ---
# Este script SQL transforma os dados brutos da tabela de staging na tabela de dimensão final
TRANSFORM_MODELS_SQL = """
-- Apaga a tabela de dimensão antiga para garantir que os dados estejam sempre atualizados
DROP TABLE IF EXISTS dim_vehicle_models;
-- Cria a nova tabela de dimensão a partir da tabela de staging
CREATE TABLE dim_vehicle_models AS
SELECT DISTINCT
    CAST(Model_ID AS SIGNED) AS model_id,
    CAST(Mfr_Name AS CHAR(255)) AS make_name,
    CAST(Model_Name AS CHAR(255)) AS model_name,
    -- Enriquecimento dos dados: cria a coluna 'motorizacao' com base em palavras-chave
    CASE
        WHEN UPPER(Model_Name) LIKE '%EV%' OR UPPER(Model_Name) LIKE '%ELECTRIC%' OR UPPER(Mfr_Name) IN ('TESLA', 'RIVIAN', 'LUCID', 'POLESTAR', 'FISKER', 'BYD') THEN 'Elétrico'
        WHEN UPPER(Model_Name) LIKE '%HYBRID%' OR UPPER(Model_Name) LIKE '%PHEV%' THEN 'Híbrido'
        ELSE 'Combustão'
    END AS motorizacao,
    -- Enriquecimento dos dados: cria a coluna 'segmento' com base na marca ou nome do modelo
    CASE
        WHEN UPPER(Mfr_Name) IN ('BMW', 'MERCEDES-BENZ', 'AUDI', 'TESLA', 'POLESTAR', 'LUCID') THEN 'Luxo'
        WHEN UPPER(Model_Name) LIKE '%SPORT%' OR UPPER(Model_Name) LIKE '%GT%' THEN 'Esportivo'
        ELSE 'Geral'
    END AS segmento
FROM
    stg_vehicle_models
-- Limpeza dos dados: remove registros que claramente não são carros de passeio
WHERE 
    UPPER(Model_Name) NOT LIKE '%TRAILER%' AND UPPER(Model_Name) NOT LIKE '%CHASSIS%' AND
    UPPER(Model_Name) NOT LIKE '%MOTORHOME%' AND UPPER(Model_Name) NOT LIKE '%MOTORCYCLE%' AND
    UPPER(Model_Name) NOT LIKE '%CHOPPER%' AND UPPER(Model_Name) NOT LIKE '%BIKE%' AND
    UPPER(Model_Name) NOT LIKE '%TRIKE%' AND UPPER(Model_Name) NOT LIKE '%CUSTOMS%' AND
    UPPER(Model_Name) NOT LIKE '%WELDING%' AND UPPER(Mfr_Name) NOT LIKE '%TRAILERS%';
-- Adiciona uma chave primária à tabela de dimensão para garantir a integridade dos dados
ALTER TABLE dim_vehicle_models ADD PRIMARY KEY (model_id);
"""

# --- DEFINIÇÃO DA DAG (Orquestração) ---
with DAG(
    dag_id="build_dim_models",
    start_date=pendulum.datetime(2025, 7, 18, tz="America/Sao_Paulo"),
    schedule_interval="@monthly", # Define que a DAG deve rodar mensalmente
    catchup=False,
    tags=["dimensions", "models"],
) as dag:
    # Tarefa inicial, apenas para marcar o começo do fluxo
    start = EmptyOperator(task_id="start")
    
    # Tarefa que executa a função Python de extração e carga
    extract_load = PythonOperator(
        task_id="extract_and_load_models",
        python_callable=_extract_load_models
    )
    
    # Tarefa que executa o script SQL de transformação
    transform = MySqlOperator(
        task_id="transform_models",
        mysql_conn_id="mysql_default", # ID da conexão configurada na UI do Airflow
        sql=TRANSFORM_MODELS_SQL
    )

    # Tarefa final, apenas para marcar o fim do fluxo
    end = EmptyOperator(task_id="end")

    # Define a ordem de execução das tarefas: start -> extract_load -> transform -> end
    start >> extract_load >> transform >> end