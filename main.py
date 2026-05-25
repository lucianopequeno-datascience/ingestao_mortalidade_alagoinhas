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
            # O PySUS descarrega e converte
            resultado_sim = SIM.download(groups=['cid10'], states=STATE, years=year)
            
            print(f"Download concluído. Tipo do objeto retornado: {type(resultado_sim)}")
            
            if resultado_sim is None:
                print(f"DATASUS não retornou ficheiros para {year}.")
                limpar_cache_pysus()
                continue
                
            # 1. Se for uma classe personalizada do PySUS (como o ParquetSet)
            if hasattr(resultado_sim, 'to_dataframe'):
                df = resultado_sim.to_dataframe()
                
            # 2. Se for objeto PyArrow (nova versão de algumas bibliotecas usa PyArrow Table)
            elif hasattr(resultado_sim, 'to_pandas'):
                df = resultado_sim.to_pandas()
                
            # 3. Se já for Pandas DataFrame nativo
            elif isinstance(resultado_sim, pd.DataFrame):
                df = resultado_sim
                
            # 4. Se for Lista ou Tupla (pode conter tabelas PyArrow, DataFrames ou caminhos)
            elif isinstance(resultado_sim, (list, tuple)) and len(resultado_sim) > 0:
                lista_dfs = []
                for item in resultado_sim:
                    if hasattr(item, 'to_dataframe'):
                        lista_dfs.append(item.to_dataframe())
                    elif hasattr(item, 'to_pandas'):
                        lista_dfs.append(item.to_pandas())
                    elif isinstance(item, pd.DataFrame):
                        lista_dfs.append(item)
                    elif isinstance(item, str):
                        lista_dfs.append(pd.read_parquet(item))
                
                if lista_dfs:
                    df = pd.concat(lista_dfs, ignore_index=True)
                else:
                    print(f"A lista retornada não continha dados convertíveis para {year}.")
                    limpar_cache_pysus()
                    continue
                    
            # 5. Se for apenas um caminho de ficheiro em formato string
            elif isinstance(resultado_sim, str):
                df = pd.read_parquet(resultado_sim)
                
            else:
                print(f"Formato não reconhecido para {year}: {type(resultado_sim)}")
                limpar_cache_pysus()
                continue
                
            # 3. Filtro para Alagoinhas
            if 'CODMUNRES' in df.columns:
                df['CODMUNRES'] = df['CODMUNRES'].astype(str)
                # startswith captura tanto o código de 6 (290070) quanto o de 7 dígitos (2900702)
                df_alagoinhas = df[df['CODMUNRES'].str.startswith(COD_ALAGOINHAS)]
            else:
                print(f"Atenção: Coluna CODMUNRES não encontrada em {year}. A saltar...")
                continue

            if df_alagoinhas.empty:
                print(f"Nenhum dado novo de Alagoinhas para {year}.")
                limpar_cache_pysus()
                continue

            # 4. Preparação em Memória (I/O)
            print(f"A preparar o ficheiro Parquet em memória...")
            parquet_buffer = io.BytesIO()
            df_alagoinhas.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0) 

            # 5. Upload direto para o Cloud Storage
            gcs_filename = f"sim_alagoinhas_{year}.parquet"
            blob = bucket.blob(f"{DESTINATION_FOLDER}/{gcs_filename}")
            
            print(f"A enviar {gcs_filename} para o bucket {BUCKET_NAME}...")
            blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")
            print(f"Sucesso! Ficheiro disponível em {DESTINATION_FOLDER}/{gcs_filename}")
            
            # 6. Limpeza de Memória do contentor
            limpar_cache_pysus()

        except Exception as e:
            print(f"Falha ao processar {year}. Erro: {e}")
            limpar_cache_pysus()

    print("\nProcessamento do SIM finalizado.")


def limpar_cache_pysus():
    """Remove a pasta de cache padrão do PySUS para evitar esgotamento de memória no contentor."""
    try:
        cache_dir = Path.home() / "pysus"
        if cache_dir.exists():
            shutil.rmtree(cache_dir, ignore_errors=True)
            print(" - Cache do PySUS limpo com sucesso.")
    except Exception as e:
        print(f" - Aviso: Não foi possível limpar o diretório do PySUS. Erro: {e}")


if __name__ == "__main__":
    run_oda_sim_pipeline()