from logger import setup_logging, get_logger
import pandas as pd
import numpy as np

setup_logging()
logger = get_logger(__name__)

def transformar_chunk(chunk):
    pd.set_option('future.no_silent_downcasting', True)
    """
    Aplica um conjunto de transformações de limpeza e padronização em um chunk de dados.
    """
    # 1. Padronização de datas (de 'AAAAMMDD' para 'YYYY-MM-DD')
    date_cols = ['data_situacao_cadastral', 'data_inicio_atividade', 'data_situacao_especial']
    for col in date_cols:
        # Primeiro, trata explicitamente valores '0' como nulos
        chunk[col] = chunk[col].replace(['0', '00000000'], np.nan).infer_objects(copy=False)
        # to_datetime é robusto e já retorna NaT (Not a Time) para erros
        chunk[col] = pd.to_datetime(chunk[col], format='%Y%m%d', errors='coerce').dt.date

    # 2. Limpeza de strings (remover espaços extras)
    str_cols = chunk.select_dtypes(include=['object']).columns
    for col in str_cols:
        chunk[col] = chunk[col].str.strip()

    # 3. Normalização de nulos (substituir '0', '' por None/NaN)
    # Em colunas de texto, strings vazias já são tratadas, mas podemos forçar para '0'
    for col in str_cols:
        chunk[col] = chunk[col].replace(['0', '00', '0000', '00000000'], np.nan).infer_objects(copy=False)

    # 4. Conversão de tipos numéricos (forçando erros para NaN, que se torna NULL)
    numeric_cols = chunk.select_dtypes(include=['number']).columns
    for col in numeric_cols:
        chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

    logger.info(f"Chunk transformado com {len(chunk)} registros.")
    return chunk
