import os
import io
import shutil
from pathlib import Path
import pandas as pd
from pysus.online_data import SIM
from google.cloud import storage

def run_oda_sim_pipeline():
    # 1. Configurações
    BUCKET_NAME = "dados_alagoinhas_bronze" 
    DESTINATION_FOLDER = "saude/sim"
    COD_ALAGOINHAS = "290070"
    STATE = "BA"
    
    print(f"Iniciando pipeline do SIM para Alagoinhas ({COD_ALAGOINHAS})...")
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    # 2. Loop para buscar de 2000 até 2029
    for year in range(2000, 2030):
        print(f"\n--- Buscando dados de {year} ---")
        
        try:
            # O PySUS baixa, converte e retorna o DataFrame diretamente
            resultado_sim = SIM.download(groups=['cid10'], states=STATE, years=year)
            
            
            # 1. Padroniza tudo para lista se for uma string (caminho único)
            if isinstance(resultado_sim, str):
                resultado_sim = [resultado_sim]
                
            # 2. Transforma em DataFrame dependendo do que o PySUS devolveu
            if isinstance(resultado_sim, pd.DataFrame):
                df = resultado_sim
            elif isinstance(resultado_sim, (list, tuple)) and len(resultado_sim) > 0:
                # Se for uma lista de caminhos de arquivos (strings)
                if isinstance(resultado_sim[0], str):
                    df = pd.concat([pd.read_parquet(f) for f in resultado_sim], ignore_index=True)
                # Se for uma lista/tupla de DataFrames
                elif isinstance(resultado_sim[0], pd.DataFrame):
                    df = pd.concat(resultado_sim, ignore_index=True)
                else:
                    print(f"Tipo de dado inesperado na lista para {year}.")
                    limpar_cache_pysus()
                    continue
            else:
                print(f"Nenhum dado válido retornado para {year}.")
                limpar_cache_pysus()
                continue
                
            # 3. Filtro para Alagoinhas
            if 'CODMUNRES' in df.columns:
                df['CODMUNRES'] = df['CODMUNRES'].astype(str)
                # startswith captura tanto o código de 6 (290070) quanto o de 7 dígitos (2900702)
                df_alagoinhas = df[df['CODMUNRES'].str.startswith(COD_ALAGOINHAS)]
            else:
                print(f"Atenção: Coluna CODMUNRES não encontrada em {year}. Pulando...")
                continue

            if df_alagoinhas.empty:
                print(f"Nenhum dado novo de Alagoinhas para {year}.")
                limpar_cache_pysus()
                continue

            # 4. Preparação em Memória (I/O)
            print(f"Preparando arquivo Parquet em memória...")
            parquet_buffer = io.BytesIO()
            df_alagoinhas.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0) 

            # 5. Upload direto para o Cloud Storage
            gcs_filename = f"sim_alagoinhas_{year}.parquet"
            blob = bucket.blob(f"{DESTINATION_FOLDER}/{gcs_filename}")
            
            print(f"Subindo {gcs_filename} para o bucket {BUCKET_NAME}...")
            blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")
            print(f"Sucesso! Arquivo disponível em {DESTINATION_FOLDER}/{gcs_filename}")
            
            # 6. Faxina de Memória do container
            limpar_cache_pysus()

        except Exception as e:
            print(f"Falha ao buscar/processar {year}. Erro: {e}")
            limpar_cache_pysus()

    print("\nProcessamento do SIM finalizado.")


def limpar_cache_pysus():
    """Remove a pasta de cache padrão do PySUS para evitar Out of Memory no container."""
    try:
        cache_dir = Path.home() / "pysus"
        if cache_dir.exists():
            shutil.rmtree(cache_dir, ignore_errors=True)
            print(" - Cache do PySUS limpo com sucesso.")
    except Exception as e:
        print(f" - Aviso: Não foi possível limpar o diretório do PySUS. Erro: {e}")


if __name__ == "__main__":
    run_oda_sim_pipeline()