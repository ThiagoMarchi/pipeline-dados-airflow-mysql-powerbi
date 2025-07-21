import pandas as pd
from sqlalchemy import create_engine, text
import logging

# --- CONFIGURAÇÕES ---
DB_CONNECTION_STRING = "mysql+mysqlconnector://mysql_user:mysql_pass@localhost:3306/dados_api"
START_DATE = '2015-01-01'
END_DATE = '2025-12-31'

def populate_date_dimension():
    """
    [VERSÃO ROBUSTA COM COMMIT EXPLÍCITO]
    Gera e insere um intervalo de datas na tabela dim_date.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info(f"Gerando datas de {START_DATE} a {END_DATE}...")

    dates = pd.date_range(start=START_DATE, end=END_DATE, freq='D')
    df = pd.DataFrame(dates, columns=['full_date'])

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

    df = df[[
        'date_id', 'full_date', 'day_of_week', 'day_name', 'day_of_month',
        'day_of_year', 'week_of_year', 'month_of_year', 'month_name',
        'quarter_of_year', 'year'
    ]]

    logging.info(f"{len(df)} linhas de data geradas.")

    engine = create_engine(DB_CONNECTION_STRING)
    try:
        # Inicia uma conexão e uma transação explícitas
        with engine.connect() as connection:
            with connection.begin() as transaction:
                try:
                    logging.info("Limpando a tabela dim_date (TRUNCATE)...")
                    connection.execute(text("TRUNCATE TABLE dim_date;"))

                    logging.info("Inserindo novos dados na tabela dim_date...")
                    df.to_sql('dim_date', con=connection, if_exists='append', index=False, chunksize=1000)

                    # Se tudo correu bem, o 'commit' é feito aqui ao sair do 'with'
                    transaction.commit()
                    logging.info("Transação commitada. Tabela dim_date populada com sucesso!")

                except Exception as e:
                    logging.error(f"Erro durante a transação, revertendo... Erro: {e}")
                    # Se algo der errado, o 'rollback' é feito
                    transaction.rollback()
                    raise
    except Exception as e:
        logging.error(f"Ocorreu um erro geral ao conectar ou executar a transação: {e}")

if __name__ == '__main__':
    populate_date_dimension()