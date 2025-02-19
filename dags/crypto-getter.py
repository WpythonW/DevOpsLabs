from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2025, 2, 18)
}

def fetch_crypto_data(**context):
    endpoint = "https://api.coinlore.net/api/tickers/"
    response = requests.get(endpoint).json()
    
    timestamp = datetime.fromtimestamp(int(response['info']['time']))
    
    coins_data = [
        {
            'id': str(coin['id']),
            'symbol': coin['symbol'],
            'name': coin['name'],
            'nameid': coin['nameid'],
            'rank': coin['rank']
        } 
        for coin in response['data']
    ]
    
    metrics_data = [
        {
            'coin_id': str(coin['id']),
            'timestamp': timestamp,
            'price_usd': coin['price_usd'],
            'percent_change_24h': coin['percent_change_24h'],
            'percent_change_1h': coin['percent_change_1h'],
            'market_cap_usd': coin['market_cap_usd'],
            'volume24': coin['volume24']
        } 
        for coin in response['data']
    ]
    
    context['task_instance'].xcom_push(key='coins_data', value=coins_data)
    context['task_instance'].xcom_push(key='metrics_data', value=metrics_data)

def save_to_database(**context):
    coins_data = context['task_instance'].xcom_pull(task_ids='fetch_crypto_data', key='coins_data')
    metrics_data = context['task_instance'].xcom_pull(task_ids='fetch_crypto_data', key='metrics_data')
    
    pg_hook = PostgresHook(postgres_conn_id='APP_DB')
    
    pg_hook.insert_rows(
        table='coins',
        rows=[tuple(coin[c] for c in ['id', 'symbol', 'name', 'nameid', 'rank']) for coin in coins_data],
        target_fields=['id', 'symbol', 'name', 'nameid', 'rank'],
        replace=True,
        replace_index='id'
    )
    
    pg_hook.insert_rows(
        table='metrics',
        rows=[tuple(metric[c] for c in ['coin_id', 'timestamp', 'price_usd', 'percent_change_24h', 'percent_change_1h', 'market_cap_usd', 'volume24']) for metric in metrics_data],
        target_fields=['coin_id', 'timestamp', 'price_usd', 'percent_change_24h', 'percent_change_1h', 'market_cap_usd', 'volume24'],
        replace=True,
        replace_index=['coin_id', 'timestamp']
    )

def calculate_aggregates(**context):
    pg_hook = PostgresHook(postgres_conn_id='APP_DB')
    
    # Вычисляем агрегаты и выводим их в лог
    result = pg_hook.get_records("""
        SELECT 
            MAX(timestamp) as timestamp,
            AVG(percent_change_24h) as avg_change_24h,
            SUM(market_cap_usd) as total_market_cap,
            (SUM(CASE WHEN c.symbol = 'BTC' THEN m.market_cap_usd ELSE 0 END) / SUM(m.market_cap_usd) * 100) as btc_dominance
        FROM metrics m
        JOIN coins c ON m.coin_id = c.id
        WHERE timestamp = (SELECT MAX(timestamp) FROM metrics)
        GROUP BY timestamp
    """)
    
    if result and result[0]:
        timestamp, avg_change, total_cap, btc_dom = result[0]
        print(f"""
        Timestamp: {timestamp}
        Average 24h Change: {avg_change:.2f}%
        Total Market Cap: ${total_cap:,.2f}
        BTC Dominance: {btc_dom:.2f}%
        """)

with DAG(
    'crypto_market_analytics',
    default_args=default_args,
    schedule_interval='*/15 * * * *',  # интервал 15 мин
    catchup=False
) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_crypto_data',
        python_callable=fetch_crypto_data
    )

    save_data = PythonOperator(
        task_id='save_to_database',
        python_callable=save_to_database
    )

    update_metrics = PythonOperator(
        task_id='calculate_aggregates',
        python_callable=calculate_aggregates
    )

    fetch_data >> save_data >> update_metrics