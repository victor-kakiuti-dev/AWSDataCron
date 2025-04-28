import sqlite3

DB_PATH = "/home/ubuntu/meu_etl/weather_data.db"


def create_database():
    conn = sqlite3.connect(DB_PATH)
    conn.close()

def create_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS clima (
            cidade TEXT,
            temperatura REAL,
            umidade INTEGER,
            pressao INTEGER,
            clima TEXT,
            data_coleta TEXT
        )
    """)
    conn.commit()
    conn.close()

# Comando para abrir o sqlite no cmd => sqlite3 "C:/Users/victo/weather_data.db"

