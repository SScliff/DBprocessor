from logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

import time
from db import inserir_data
from transform import transformar_chunk

def processar_chunk(chunk, num_chunk):
    inicio = time.time()
    
    # Etapa de transformação
    chunk_transformado = transformar_chunk(chunk)
    
    # Etapa de inserção
    resultado_insercao = inserir_data(chunk_transformado, 'estabelecimentos')
    fim = time.time()
    
    return {
        'chunk_num': num_chunk,
        'registros': resultado_insercao.get('registros', 0),
        'tempo': fim - inicio,
        'sucesso': resultado_insercao['sucesso'],
        'erro': resultado_insercao.get('erro')
    }