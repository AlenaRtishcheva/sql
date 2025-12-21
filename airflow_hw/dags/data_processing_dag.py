"""
Домашнее задание 6: Автоматизация расчетов с помощью Apache AirFlow
DAG для загрузки данных из CSV файлов в postgres и выполнения аналитических запросов
"""
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import logging
import os
import csv
from io import StringIO

# Конфигурационные константы
DATA_DIR = "/opt/airflow/dags"
POSTGRES_CONN_ID = "postgres_default"
OUTPUT_DIR = DATA_DIR

logger = logging.getLogger(__name__)

# SQL ЗАПРОСЫ

# Общий CTE для customer_transactions
CUSTOMER_TRANSACTIONS_CTE = """
    select 
        customer.customer_id,
        customer.first_name,
        customer.last_name,
        coalesce(sum(order_items.quantity * order_items.item_list_price_at_sale), 0) as total_amount
    from customer
    left join orders on customer.customer_id = orders.customer_id
    left join order_items on orders.order_id = order_items.order_id
    group by customer.customer_id, customer.first_name, customer.last_name
"""

# Запрос 1: Минимальные транзакции
REQUEST1_MIN = f"""
with customer_transactions as ({CUSTOMER_TRANSACTIONS_CTE})
select 
    'MIN_TRANSACTION' as transaction_type,
    first_name,
    last_name,
    total_amount
from customer_transactions
where total_amount > 0
order by total_amount asc
limit 3
"""

# Запрос 1: Максимальные транзакции  
REQUEST1_MAX = f"""
with customer_transactions as ({CUSTOMER_TRANSACTIONS_CTE})
select 
    'MAX_TRANSACTION' as transaction_type,
    first_name,
    last_name,
    total_amount
from customer_transactions
order by total_amount desc
limit 3
"""

# Запрос 2: ТОП-5 по сегментам
REQUEST2 = """
with customer_segment_revenue as (
    select 
        customer.customer_id,
        customer.first_name,
        customer.last_name,
        customer.wealth_segment,
        coalesce(sum(order_items.quantity * order_items.item_list_price_at_sale), 0) as total_revenue
    from customer
    left join orders on customer.customer_id = orders.customer_id
    left join order_items on orders.order_id = order_items.order_id
    where customer.wealth_segment is not null
    group by customer.customer_id, customer.first_name, customer.last_name, customer.wealth_segment
),
ranked_customers as (
    select 
        *,
        row_number() over (partition by wealth_segment order by total_revenue desc) as rank_position
    from customer_segment_revenue
)
select 
    first_name,
    last_name,
    wealth_segment,
    total_revenue
from ranked_customers
where rank_position <= 5
order by wealth_segment, total_revenue desc;
"""

# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ

def save_to_csv(filepath: str, headers: list, data_rows: list) -> None:
    """Сохраняет данные в CSV файл"""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w", encoding="utf-8", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            writer.writerows(data_rows)
        logger.info(f"файл сохранен: {filepath} ({len(data_rows)} строк)")
    except Exception as e:
        logger.error(f"Ошибка при сохранении файла {filepath}: {str(e)}")
        raise

def load_csv_to_postgres(file_path, table_name, delimiter=',', **context):
    """Загрузка CSV файла в PostgreSQL через Python"""
    try:
        logger.info(f"Загрузка {file_path} в таблицу {table_name}...")
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Файл не найден: {file_path}")
        
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        with open(file_path, 'r', encoding='utf-8') as f:
            if delimiter == ';':
                lines = f.readlines()
                lines = [line.replace(';', ',') for line in lines]
                csv_content = ''.join(lines)
                
                string_io = StringIO(csv_content)
                cursor.copy_expert(
                    f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','",
                    string_io
                )
            else:
                cursor.copy_expert(
                    f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER '{delimiter}'",
                    f
                )
        
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Успешно загружено {count} строк в {table_name}")
        return count
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке {file_path}: {str(e)}")
        raise

def load_customer_data(**context):
    """Загрузка customer.csv"""
    file_path = os.path.join(DATA_DIR, "customer.csv")
    return load_csv_to_postgres(file_path, "customer", delimiter=';', **context)

def load_product_data(**context):
    """Загрузка product.csv"""
    file_path = os.path.join(DATA_DIR, "product.csv")
    return load_csv_to_postgres(file_path, "product", delimiter=',', **context)

def load_orders_data(**context):
    """Загрузка orders.csv"""
    file_path = os.path.join(DATA_DIR, "orders.csv")
    return load_csv_to_postgres(file_path, "orders", delimiter=',', **context)

def load_order_items_data(**context):
    """Загрузка order_items.csv"""
    file_path = os.path.join(DATA_DIR, "order_items.csv")
    return load_csv_to_postgres(file_path, "order_items", delimiter=',', **context)

def execute_request_1_min(**context) -> int:
    """Выполнение запроса 1: ТОП-3 минимальных сумм транзакций"""
    logger.info("выполнение request 1: ТОП-3 минимальных сумм...")
    
    try:
        db_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        results = db_hook.get_records(REQUEST1_MIN)
        
        # Сохраняем минимальные транзакции
        output_path = os.path.join(OUTPUT_DIR, "top3_min_transactions.csv")
        save_to_csv(
            output_path,
            ["transaction_type", "first_name", "last_name", "total_amount"],
            results
        )
        
        row_count = len(results)
        context['ti'].xcom_push(key='request1_min_rows', value=row_count)
        
        logger.info(f"сохранено: {row_count} минимальных транзакций")
        return row_count
        
    except Exception as e:
        logger.error(f"ошибка при выполнении request 1 (min): {str(e)}")
        raise

def execute_request_1_max(**context) -> int:
    """Выполнение запроса 2: ТОП-3 максимальных сумм транзакций"""
    logger.info("выполнение request 2: ТОП-3 максимальных сумм...")
    
    try:
        db_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        results = db_hook.get_records(REQUEST1_MAX)
        
        # Сохраняем максимальные транзакции
        output_path = os.path.join(OUTPUT_DIR, "top3_max_transactions.csv")
        save_to_csv(
            output_path,
            ["transaction_type", "first_name", "last_name", "total_amount"],
            results
        )
        
        row_count = len(results)
        context['ti'].xcom_push(key='request1_max_rows', value=row_count)
        
        logger.info(f"сохранено: {row_count} максимальных транзакций")
        return row_count
        
    except Exception as e:
        logger.error(f"ошибка при выполнении request 2 (max): {str(e)}")
        raise

def execute_request_2(**context) -> int:
    """Выполнение запроса 3: ТОП-5 клиентов в каждом сегменте благосостояния"""
    logger.info("выполнение request 3: ТОП-5 по сегментам...")
    
    try:
        db_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        results = db_hook.get_records(REQUEST2)
        
        output_path = os.path.join(OUTPUT_DIR, "top5_by_wealth_segment.csv")
        save_to_csv(
            output_path,
            ["first_name", "last_name", "wealth_segment", "total_revenue"],
            results
        )
        
        row_count = len(results)
        context['ti'].xcom_push(key='request2_rows', value=row_count)
        
        logger.info(f"сохранено: {row_count} строк по сегментам")
        return row_count
        
    except Exception as e:
        logger.error(f"ошибка при выполнении request 3: {str(e)}")
        raise

def verify_results(**context) -> bool:
    """Проверка что результаты запросов не пустые"""
    logger.info("проверка результатов запросов...")
    
    try:
        ti = context['ti']
        
        request1_min_rows = ti.xcom_pull(task_ids='request1_min', key='request1_min_rows')
        request1_max_rows = ti.xcom_pull(task_ids='request1_max', key='request1_max_rows')
        request2_rows = ti.xcom_pull(task_ids='request2', key='request2_rows')
        
        logger.info(f"Результаты проверки:")
        logger.info(f"  request 1 (top3 min): {request1_min_rows} строк")
        logger.info(f"  request 2 (top3 max): {request1_max_rows} строк")
        logger.info(f"  request 3 (top5 по сегментам): {request2_rows} строк")
        
        if request1_min_rows == 0 or request1_max_rows == 0 or request2_rows == 0:
            error_message = f"Один из запросов вернул 0 строк: min={request1_min_rows}, max={request1_max_rows}, segment={request2_rows}"
            logger.error(error_message)
            raise AirflowException(error_message)
        
        logger.info("Все запросы вернули непустые результаты")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при проверке результатов: {str(e)}")
        raise

def log_final_status(**context) -> None:
    """Логирование финального статуса выполнения DAG"""
    try:
        dag_run = context['dag_run']
        
        if dag_run.state == 'success':
            logger.info("=" * 50)
            logger.info("DAG ВЫПОЛНЕН УСПЕШНО!")
            logger.info("Все этапы выполнены корректно")
            logger.info(f"Результаты сохранены в {OUTPUT_DIR}/")
            logger.info("=" * 50)
        else:
            logger.info("=" * 50)
            logger.info(f"DAG ЗАВЕРШЕН СО СТАТУСОМ: {dag_run.state}")
            logger.info("Требуется проверка логики выполнения")
            logger.info("=" * 50)
            
    except Exception as e:
        logger.error(f"Ошибка при логировании финального статуса: {str(e)}")

# КОНФИГУРАЦИЯ DAG

default_args = {
    "owner": "student",
    "retries": 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id="data_processing_dag",
    start_date=datetime(2025, 12, 21),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    description="домашнее задание 6: автоматизация расчетов с помощью apache airflow",
    tags=["homework", "postgres", "data_processing", "module6"],
) as dag:
    
    # Подготовка базы данных
    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            DROP TABLE IF EXISTS order_items CASCADE;
            DROP TABLE IF EXISTS orders CASCADE;
            DROP TABLE IF EXISTS product CASCADE;
            DROP TABLE IF EXISTS customer CASCADE;
            
            CREATE TABLE customer (
                customer_id             INTEGER,
                first_name              TEXT,
                last_name               TEXT,
                gender                  TEXT,
                DOB                     DATE,
                job_title               TEXT,
                job_industry_category   TEXT,
                wealth_segment          TEXT,
                deceased_indicator      TEXT,
                owns_car                TEXT,
                address                 TEXT,
                postcode                TEXT,
                state                   TEXT,
                country                 TEXT,
                property_valuation      INTEGER
            );

            CREATE TABLE product (
                product_id      INTEGER,
                brand           TEXT,
                product_line    TEXT,
                product_class   TEXT,
                product_size    TEXT,
                list_price      NUMERIC(12, 2),
                standard_cost   NUMERIC(12, 2)
            );

            CREATE TABLE orders (
                order_id      INTEGER,
                customer_id   INTEGER,
                order_date    DATE,
                online_order  TEXT,
                order_status  TEXT
            );

            CREATE TABLE order_items (
                order_item_id               INTEGER,
                order_id                    INTEGER,
                product_id                  INTEGER,
                quantity                    NUMERIC(12, 2),
                item_list_price_at_sale     NUMERIC(12, 2),
                item_standard_cost_at_sale  NUMERIC(12, 2)
            );
        """,
    )
    
    # Загрузка данных (параллельные задачи)
    with TaskGroup(group_id='load_csv_data', dag=dag) as load_data_group:
        
        load_customer_task = PythonOperator(
            task_id="load_customer_data",
            python_callable=load_customer_data,
            provide_context=True,
        )
        
        load_product_task = PythonOperator(
            task_id="load_product_data",
            python_callable=load_product_data,
            provide_context=True,
        )
        
        load_orders_task = PythonOperator(
            task_id="load_orders_data",
            python_callable=load_orders_data,
            provide_context=True,
        )
        
        load_order_items_task = PythonOperator(
            task_id="load_order_items_data",
            python_callable=load_order_items_data,
            provide_context=True,
        )
        
        # параллельное выполнение всех загрузок
        [load_customer_task, load_product_task, load_orders_task, load_order_items_task]
    
    # Выполнение аналитических запросов (параллельные задачи)
    with TaskGroup(group_id="execute_analytical_requests", dag=dag) as requests_group:
        
        request1_min = PythonOperator(
            task_id="request1_min",
            python_callable=execute_request_1_min,
            provide_context=True,
        )
        
        request1_max = PythonOperator(
            task_id="request1_max",
            python_callable=execute_request_1_max,
            provide_context=True,
        )
        
        request2 = PythonOperator(
            task_id="request2",
            python_callable=execute_request_2,
            provide_context=True,
        )
        
        # параллельное выполнение 3 запросов
        [request1_min, request1_max, request2]
    
    # Проверка результатов
    results_verification = PythonOperator(
        task_id="results_verification",
        python_callable=verify_results,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    
    # Финальная обработка
    final_status_log = PythonOperator(
        task_id="final_status_log",
        python_callable=log_final_status,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    
    completion_message = BashOperator(
        task_id="completion_message",
        bash_command='echo "ВЫПОЛНЕНИЕ DAG ЗАВЕРШЕНО. Статус: {{ dag_run.state }}"',
        trigger_rule=TriggerRule.ALL_DONE,
    )
    
    # ОПРЕДЕЛЕНИЕ ПОСЛЕДОВАТЕЛЬНОСТИ ВЫПОЛНЕНИЯ
    create_tables >> load_data_group >> requests_group >> results_verification
    results_verification >> [final_status_log, completion_message]