from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.empty import EmptyOperator

# --- SCRIPT SQL ---
# Define o script SQL que será executado.
# Ele primeiro apaga a tabela 'dim_dealers' se ela existir, depois a recria
# e, por fim, insere os dados fictícios das concessionárias.
CREATE_AND_POPULATE_DEALERS_SQL = """
DROP TABLE IF EXISTS dim_dealers;
CREATE TABLE dim_dealers (
    dealer_id INT PRIMARY KEY, dealer_name VARCHAR(255),
    city VARCHAR(100), state VARCHAR(2)
);
INSERT INTO dim_dealers (dealer_id, dealer_name, city, state) VALUES
(1, 'Auto Center São Paulo', 'São Paulo', 'SP'),
(2, 'Veículos Rio', 'Rio de Janeiro', 'RJ'),
(3, 'Carros BH', 'Belo Horizonte', 'MG'),
(4, 'Sul Motores', 'Porto Alegre', 'RS'),
(5, 'Bahia Car', 'Salvador', 'BA'),
(25, 'Capital Veículos', 'Brasília', 'DF'),
(50, 'Norte Vendas', 'Manaus', 'AM'),
(75, 'Centro-Oeste Car', 'Goiânia', 'GO'),
(100, 'Litoral Carros', 'Recife', 'PE');
"""

# --- DEFINIÇÃO DA DAG ---
# Bloco que define as propriedades e a estrutura da DAG.
with DAG(
    # ID único da DAG, que aparece na interface do Airflow.
    dag_id="build_dim_dealers_fictitious",
    # Data de início a partir da qual a DAG pode ser agendada.
    start_date=pendulum.datetime(2025, 7, 18, tz="America/Sao_Paulo"),
    # 'schedule=None' significa que a DAG não roda automaticamente, apenas manualmente.
    schedule=None, catchup=False,
    # Documentação que aparece na interface do Airflow.
    doc_md="Cria e popula a tabela de dimensão de concessionárias com dados fictícios.",
    # Etiquetas para organizar as DAGs na interface.
    tags=["dimensions", "dealers", "setup"],
) as dag:
    # Tarefa inicial que serve como um ponto de partida claro para o fluxo.
    start = EmptyOperator(task_id="start")
    # Tarefa principal que executa o script SQL no MySQL.
    create_and_populate_table = MySqlOperator(
        task_id="create_and_populate_dealers_table",
        # ID da conexão com o MySQL configurada na interface do Airflow.
        mysql_conn_id="mysql_default", sql=CREATE_AND_POPULATE_DEALERS_SQL,
    )
    # Tarefa final que serve como um ponto de conclusão claro para o fluxo.
    end = EmptyOperator(task_id="end")
    # Define a ordem de execução das tarefas: start -> create_and_populate_table -> end.
    start >> create_and_populate_table >> end
