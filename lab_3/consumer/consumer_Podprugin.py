import json
import psycopg2
from psycopg2 import sql
from kafka import KafkaConsumer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = '10.23.29.182:9092'
KAFKA_TOPIC = 'etl-topic'
DB_CONFIG = {
    'host': '195.209.210.248',
    'port': 5432,
    'database': 'dj_putaria_db',
    'user': 'putaria',
    'password': 'z;lP9;4I2Tmn'
}

def create_table_if_not_exists(cursor, table_name, columns):
    """Создание таблицы если она не существует"""
    try:
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            )
        """, (table_name,))
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            columns_def = []
            for col in columns:
                columns_def.append(f'"{col}" TEXT')
            
            create_sql = f"""
            CREATE TABLE "{table_name}" (
                {', '.join(columns_def)}
            )
            """
            
            cursor.execute(create_sql)
            logger.info(f"Таблица '{table_name}' создана")
        else:
            logger.info(f"Таблица '{table_name}' уже существует")
            
        return True
    except Exception as e:
        logger.error(f"Ошибка создания таблицы '{table_name}': {e}")
        return False

def insert_data(cursor, table_name, columns, rows):
    """Вставка данных в таблицу"""
    try:
        cols = ', '.join([f'"{col}"' for col in columns])
        placeholders = ', '.join(['%s' for _ in columns])
        insert_sql = f'INSERT INTO "{table_name}" ({cols}) VALUES ({placeholders})'
        
        for row in rows:
            cursor.execute(insert_sql, row)
        
        logger.info(f"Добавлено {len(rows)} строк в таблицу '{table_name}'")
        return True
    except Exception as e:
        logger.error(f"Ошибка вставки данных в '{table_name}': {e}")
        return False

def process_message(message):
    """Обработка полученного сообщения"""
    try:
        data = json.loads(message.value.decode('utf-8'))
        
        required_fields = ['table', 'columns', 'rows']
        for field in required_fields:
            if field not in data:
                logger.error(f"Отсутствует поле '{field}' в сообщении")
                return False
        
        table_name = data['table']
        columns = data['columns']
        rows = data['rows']
        
        logger.info(f"Получены данные для таблицы '{table_name}'")
        logger.info(f"Колонки: {columns}")
        logger.info(f"Количество строк: {len(rows)}")
        
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        if create_table_if_not_exists(cursor, table_name, columns):
            if insert_data(cursor, table_name, columns, rows):
                logger.info(f"Данные успешно обработаны для таблицы '{table_name}'")
            else:
                logger.error(f"Ошибка вставки данных в '{table_name}'")
        
        cursor.close()
        conn.close()
        return True
        
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка парсинга JSON: {e}")
        return False
    except Exception as e:
        logger.error(f"Ошибка обработки сообщения: {e}")
        return False

def main():
    """Основная функция consumer"""
    logger.info("Запуск Kafka Consumer...")
    logger.info(f"Подключение к Kafka: {KAFKA_BROKER}")
    logger.info(f"Топик: {KAFKA_TOPIC}")
    
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='etl-consumer-group',
            value_deserializer=lambda x: x
        )
        
        logger.info("Consumer успешно подключен к Kafka")
        
        for message in consumer:
            logger.info(f"Получено сообщение из раздела {message.partition}, offset {message.offset}")
            process_message(message)
            
    except Exception as e:
        logger.error(f"Ошибка в работе consumer: {e}")

if __name__ == "__main__":
    main()