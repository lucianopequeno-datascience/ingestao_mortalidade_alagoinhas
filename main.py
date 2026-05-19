import os
from pysus.online_data import SIM
import pandas as pd
from google.cloud import storage

def run_oda_sim_pipeline():
    # 1. Configurações
    BUCKET_NAME = "dados_alagoinhas_bronze" # Altere para o nome do seu bucket
    DESTINATION_FOLDER = "saude/sim"
    COD_ALAGOINHAS = "290070"
    STATE = "BA"
    
    print(f"Iniciando pipeline do SIM para Alagoinhas ({COD_ALAGOINHAS})...")
    
    # Cliente do Storage inicializado uma vez
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    # 2. Loop para buscar de 2000 até 2029
    for year in range(2000, 2030):
        print(f"Buscando dados de {year}...")
        
        try:
            # O módulo SIM do pysus usa o método download direto por estado e ano
            df = SIM.download(state=STATE, year=year)
            
            if df is None or df.empty:
                print(f"Nenhum arquivo retornado para o ano {year}.")
                continue
                
            # 3. Filtro para Alagoinhas
            # Garante que a coluna está como string para não falhar na filtragem
            if 'CODMUNRES' in df.columns:
                df['CODMUNRES'] = df['CODMUNRES'].astype(str)
                df_alagoinhas = df[df['CODMUNRES'].str.startswith(COD_ALAGOINHAS)]
            else:
                print(f"Atenção: Coluna CODMUNRES não encontrada em {year}. Pulando...")
                continue

            if df_alagoinhas.empty:
                print(f"Nenhum dado novo de Alagoinhas para {year}.")
                continue

            # 4. Preparação do arquivo para o Storage
            # Usamos o diretório /tmp/ pois o Cloud Run Job permite escrita apenas lá
            local_filename = f"/tmp/sim_alagoinhas_{year}.csv"
            gcs_filename = f"sim_alagoinhas_{year}.csv"
            
            df_alagoinhas.to_csv(local_filename, index=False, sep=';', encoding='utf-8')

            # 5. Upload para o Cloud Storage
            print(f"Subindo {gcs_filename} para o bucket {BUCKET_NAME}...")
            blob = bucket.blob(f"{DESTINATION_FOLDER}/{gcs_filename}")
            blob.upload_from_filename(local_filename)
            print(f"Upload concluído.")
            
            print(f"Sucesso! Arquivo disponível em {DESTINATION_FOLDER}/{gcs_filename}")
            
            # Limpa o arquivo temporário para não estourar a memória do container
            os.remove(local_filename)

        except Exception as e:
            print(f"Falha ao buscar/processar {year} (provavelmente o ano ainda não está no DATASUS). Erro: {e}")

    print("Processamento do SIM finalizado.")

if __name__ == "__main__":
    run_oda_sim_pipeline()