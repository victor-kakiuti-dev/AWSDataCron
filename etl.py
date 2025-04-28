import requests
import pandas as pd
import sqlite3
import time

# Caminho para database SQLite
DB_PATH = "/home/ubuntu/meu_etl/weather_data.db"



#Configuração
API_KEY = "2bb3ace41ccc1f29c06bad765176aa30"
CITY = 'São Paulo'	
URL = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={'2bb3ace41ccc1f29c06bad765176aa30'}&units=metric&lang=pt_br"


# Extração: Pega os dados da API
def extract():
    response = requests.get(URL)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro ao acessar a API: {response.status_code}")
        return None
    

# Tranformação: Filtra e estrutura os dados
def transform(data):
    if data:
        transformed_data = {
            "cidade": data['name'],
            "temperatura": data['main']['temp'],
            "umidade": data['main']['humidity'],
            "pressao": data['main']['pressure'],
            "clima": data['weather'][0]['description'],
            "data_coleta": pd.Timestamp.now()
        }
        return transformed_data
    return None


# Carga: Salva os dados no SQLite 
def load(data):
    conn = sqlite3.connect(DB_PATH)
    df = pd.DataFrame([data])
    df.to_sql("clima", conn, if_exists= "append", index=False)
    conn.close()
    print("Dados carregados com sucesso!")


# Fluxo de ETL
def etl_pipeline():
    print("🔄 Iniciando ETL...")
    raw_data = extract()
    transformed_data = transform(raw_data)
    if transformed_data:
        load(transformed_data)
        print("✅ Dados inseridos com sucesso!")
    else:
        print("❌ Falha no processo.")

if __name__ == "__main__":
    while True:
        etl_pipeline()
        time.sleep(3600)
        # Aguarda 1 hora antes de executar novamente



