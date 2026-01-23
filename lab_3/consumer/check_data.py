
import psycopg2

DB_CONFIG = {
    'host': '195.209.210.248',
    'port': 5432,
    'database': 'dj_putaria_db',
    'user': 'putaria',
    'password': 'z;lP9;4I2Tmn'
}

def check_tables():
    """Проверка всех таблиц в базе"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        
        tables = cursor.fetchall()
        print(f"Найдено таблиц: {len(tables)}")
        
        for table in tables:
            table_name = table[0]
            cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            count = cursor.fetchone()[0]
            
            cursor.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """)
            columns = cursor.fetchall()
            
            print(f"\nТаблица: {table_name}")
            print(f"Количество записей: {count}")
            print("Структура:")
            for col in columns:
                print(f"  {col[0]}: {col[1]}")
            
            if count > 0:
                cursor.execute(f'SELECT * FROM "{table_name}" LIMIT 3')
                sample = cursor.fetchall()
                print("Пример данных:")
                for row in sample:
                    print(f"  {row}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    check_tables()