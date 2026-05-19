import os
import io
import pandas as pd
from pysus.online_data import SIM
from google.cloud import storage

def run_oda_sim_pipeline():
    # 1. Configurações
    BUCKET_NAME = "dados_alagoinhas_bronze" # Altere para o nome real do seu bucket de dev/prod
    DESTINATION_FOLDER = "saude/sim"
    COD_ALAGOINHAS = "290070"
    STATE = "BA"
    
    print(f"Iniciando pipeline do SIM para Alagoinhas ({COD_ALAGOINHAS})...")
    
    # Cliente do Storage inicializado uma vez
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    # 2. Loop para buscar de 2000 até 2029
    for year in range(2000, 2030):
        print(f"\n--- Buscando dados de {year} ---")
        
        try:
            # Baixa os arquivos brutos para o cache do contêiner (retorna os caminhos físicos)
            # Atenção aos parâmetros no plural exigidos pelas versões novas do PySUS
            arquivos = SIM.download(states=STATE, years=year)
            
            if not arquivos:
                print(f"Nenhum arquivo retornado pelo DATASUS para o ano {year}.")
                continue
                
            # Garante que seja uma lista (caso o PySUS retorne apenas uma string/caminho)
            if isinstance(arquivos, str):
                arquivos = [arquivos]
                
            # Lê os arquivos parquet salvos em cache e consolida num único DataFrame
            df = pd.concat([pd.read_parquet(f) for f in arquivos], ignore_index=True)
            
            if df.empty:
                print(f"O DataFrame carregado está vazio para o ano {year}.")
                continue
                
            # 3. Filtro para Alagoinhas
            if 'CODMUNRES' in df.columns:
                df['CODMUNRES'] = df['CODMUNRES'].astype(str)
                df_alagoinhas = df[df['CODMUNRES'].str.startswith(COD_ALAGOINHAS)]
            else:
                print(f"Atenção: Coluna CODMUNRES não encontrada em {year}. Pulando...")
                continue

            if df_alagoinhas.empty:
                print(f"Nenhum dado novo de Alagoinhas para {year}.")
                # Se não tem dados de Alagoinhas, ainda precisamos limpar o cache da Bahia
                limpar_cache(arquivos)
                continue

            # 4. Preparação em Memória (Sem usar o disco /tmp/ para economizar RAM)
            print(f"Preparando arquivo Parquet em memória...")
            parquet_buffer = io.BytesIO()
            df_alagoinhas.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0) # Retorna o ponteiro para o início do buffer

            # 5. Upload direto para o Cloud Storage
            gcs_filename = f"sim_alagoinhas_{year}.parquet"
            blob = bucket.blob(f"{DESTINATION_FOLDER}/{gcs_filename}")
            
            print(f"Subindo {gcs_filename} para o bucket {BUCKET_NAME}...")
            blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")
            print(f"Sucesso! Arquivo disponível em {DESTINATION_FOLDER}/{gcs_filename}")
            
            # 6. Faxina de Memória (Evita Out of Memory no Cloud Run)
            limpar_cache(arquivos)

        except Exception as e:
            print(f"Falha ao buscar/processar {year} (provavelmente o ano ainda não está no DATASUS). Erro: {e}")
            # Em caso de erro no meio do processo, tenta limpar o cache se ele foi criado
            if 'arquivos' in locals():
                limpar_cache(arquivos)

    print("\nProcessamento do SIM finalizado.")


def limpar_cache(lista_arquivos):
    """Função auxiliar para deletar os arquivos temporários do PySUS"""
    print("Limpando cache local do PySUS para liberar memória...")
    for f in lista_arquivos:
        try:
            if os.path.exists(f):
                os.remove(f)
                print(f" - Cache removido: {f}")
        except Exception as e:
            print(f" - Aviso: Não foi possível remover {f}. Erro: {e}")


if __name__ == "__main__":
    run_oda_sim_pipeline()