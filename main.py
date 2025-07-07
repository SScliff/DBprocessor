from logger import setup_logging, get_logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from reader import ler_chunks
from settings import MAX_WORKERS
from worker import processar_chunk
import sys


# Inicializa logging centralizado
setup_logging(log_file='logs/app.log', log_level='INFO')
logger = get_logger(__name__)

def main():

    try:
        logger.info("Iniciando processamento dos dados...")
        chunks = ler_chunks()
        total_chunks = 0
        chunks_processados = 0
        registros_processados = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Mapeia cada futuro ao seu número de chunk para melhor logging
            future_to_chunk = {executor.submit(processar_chunk, chunk, i): i for i, chunk in enumerate(chunks)}
            total_chunks = len(future_to_chunk)
            
            logger.info(f"{total_chunks} chunks foram enviados para processamento.")

            for future in as_completed(future_to_chunk):
                chunk_num = future_to_chunk[future]
                try:
                    resultado = future.result()
                    if resultado['sucesso']:
                        chunks_processados += 1
                        registros_processados += resultado['registros']
                        logger.info(
                            f"Chunk {resultado['chunk_num']} processado com sucesso. "
                            f"{resultado['registros']} registros em {resultado['tempo']:.2f}s. "
                            f"Progresso: {chunks_processados}/{total_chunks} "
                            f"({(chunks_processados/total_chunks*100):.1f}%)"
                        )
                    else:
                        logger.error(f"Falha ao processar chunk {resultado['chunk_num']}: {resultado['erro']}")
                except Exception as e:
                    logger.error(f"Erro crítico ao processar o chunk {chunk_num}: {str(e)}")

        logger.info(
            f"Processamento concluído. "
            f"Total de registros processados: {registros_processados} "
            f"em {chunks_processados} chunks."
        )


    except KeyboardInterrupt:
        logger.info("Processo interrompido pelo usuário.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()