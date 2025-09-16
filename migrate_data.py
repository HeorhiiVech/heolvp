import sqlite3
import pandas as pd
from sqlalchemy import create_engine

# --- НАСТРОЙКА ---
# 1. Имя вашего файла SQLite. Судя по вашему файлу, оно должно быть таким.
SQLITE_DB_PATH = 'scrims_data.db'

# 2. Вставьте СЮДА вашу строку подключения к базе Neon/Postgres, которую вы скопировали ранее.
POSTGRES_URL = 'postgresql://neondb_owner:npg_CoZH7MEs8Rfm@ep-lucky-thunder-afxzcl54-pooler.c-2.us-west-2.aws.neon.tech/neondb?sslmode=require' # <--- ВАЖНО

# 3. Список всех таблиц из вашего файла database.py для переноса.
TABLES_TO_MIGRATE = [
    'scrims',
    'tournament_games',
    'soloq_games',
    'manual_drafts',
    'schedule_entries',
    'schedule_notes',
    'jungle_pathing',
    'player_positions_snapshots',
    'first_wards_data',
    #'all_wards_data',
    #'player_positions_timeline'
]
# --- КОНЕЦ НАСТРОЙКИ ---

print("Начало миграции данных...")

try:
    # Подключаемся к Postgres (Neon)
    postgres_engine = create_engine(POSTGRES_URL)
    print("✅ Успешно подключились к Postgres (Neon).")

    # Подключаемся к локальному файлу SQLite
    sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
    print(f"✅ Успешно подключились к SQLite файлу: {SQLITE_DB_PATH}.")

    # Проходим по каждой таблице и переносим данные
    for table_name in TABLES_TO_MIGRATE:
        print(f"\nПереносим таблицу '{table_name}'...")
        try:
            # Читаем всю таблицу из SQLite в DataFrame
            df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', sqlite_conn)
            print(f"  - Прочитано {len(df)} строк из SQLite.")

            # Если в таблице есть данные, записываем их в Postgres
            if not df.empty:
                # 'replace' - означает, что если таблица уже существует, она будет удалена и создана заново
                df.to_sql(table_name, postgres_engine, if_exists='replace', index=False)
                print(f"  - ✅ Таблица '{table_name}' успешно перенесена в Postgres.")
            else:
                print(f"  - ⚠️ Таблица '{table_name}' пуста, пропускаем запись.")

        except Exception as e:
            print(f"  - ❌ Ошибка при переносе таблицы '{table_name}': {e}")

    # Закрываем соединение с SQLite
    sqlite_conn.close()
    print("\n🎉 Миграция данных успешно завершена!")

except Exception as e:
    print(f"❌ Произошла критическая ошибка: {e}")