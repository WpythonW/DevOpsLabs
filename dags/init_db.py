from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    'init_crypto_database',
    start_date=datetime(2025, 2, 19),
    schedule_interval=None,
    catchup=False,
    tags=['init']
) as dag:

    create_coins_table = PostgresOperator(
        task_id='create_coins_table',
        postgres_conn_id='app_db',
        sql="""
            DROP TABLE IF EXISTS coins CASCADE;
            CREATE TABLE coins (
                id VARCHAR(50) PRIMARY KEY,
                symbol VARCHAR(20),
                name VARCHAR(100),
                nameid VARCHAR(100),
                rank INTEGER
            );
        """
    )

    create_metrics_table = PostgresOperator(
        task_id='create_metrics_table',
        postgres_conn_id='app_db',
        sql="""
            DROP TABLE IF EXISTS metrics;
            CREATE TABLE metrics (
                coin_id VARCHAR(50) REFERENCES coins(id),
                timestamp TIMESTAMP,
                price_usd DECIMAL(24,8),
                percent_change_24h DECIMAL(10,2),
                percent_change_1h DECIMAL(10,2),
                market_cap_usd DECIMAL(24,2),
                volume24 DECIMAL(24,2),
                PRIMARY KEY (coin_id, timestamp)
            );
        """
    )

    create_coins_table >> create_metrics_table
