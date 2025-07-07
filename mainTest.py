from logger import setup_logging, get_logger
setup_logging()
logger = get_logger(__name__)

import pandas as pd
import time
from config import CSV_PATH, CHUNKSIZE, ENCODING, DELIMITER, COLUNAS
from db import inserir_data
from engine import engine
from sqlalchemy import text


def main_simples():
    print("=== PROCESSAMENTO SIMPLES (SEM THREADING) ===\n")

    # Verificar estado inicial da tabela
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM estabelecimentos"))
        count_inicial = result.fetchone()[0]
        print(f"Registros iniciais na tabela: {count_inicial}")

    try:
        print(f"Lendo arquivo: {CSV_PATH}")
        chunks = pd.read_csv(
            CSV_PATH,
            sep=DELIMITER,
            dtype=str,
            chunksize=CHUNKSIZE,
            encoding=ENCODING,
            low_memory=False,
            header=None,
            names=COLUNAS,
            on_bad_lines="skip"
        )

        chunk_count = 0
        total_registros = 0

        for i, chunk in enumerate(chunks):
            inicio = time.time()

            print(f"\n--- Processando Chunk {i} ---")
            print(f"Tamanho do chunk: {len(chunk)}")

            if len(chunk) == 0:
                print("❌ Chunk vazio, pulando...")
                continue

            # Mostrar amostra dos dados
            print("📄 Amostra dos dados:")
            print(chunk.head(2).to_string())

            # Converter para lista de dicionários
            dados = chunk.to_dict('records')
            print(f"📦 Convertido para {len(dados)} registros")

            # Inserir no banco
            try:
                print("💾 Inserindo no banco...")
                inserir_data(dados, 'estabelecimentos')

                # Verificar se foi inserido
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT COUNT(*) FROM estabelecimentos"))
                    count_atual = result.fetchone()[0]
                    novos_registros = count_atual - count_inicial - total_registros

                print(f"✅ Chunk {i} processado com sucesso!")
                print(f"📊 Novos registros inseridos: {novos_registros}")
                print(f"📊 Total na tabela: {count_atual}")

                chunk_count += 1
                total_registros += len(dados)

            except Exception as e:
                print(f"❌ Erro ao inserir chunk {i}: {e}")
                import traceback
                traceback.print_exc()
                break

            fim = time.time()
            print(f"⏱️  Tempo do chunk: {fim - inicio:.2f}s")

            # Processar apenas os primeiros 3 chunks para teste
            if i >= 2:
                print("\n🛑 Parando após 3 chunks para teste...")
                break

        print(f"\n=== RESUMO ===")
        print(f"Chunks processados: {chunk_count}")
        print(f"Total de registros processados: {total_registros}")

        # Verificar estado final
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM estabelecimentos"))
            count_final = result.fetchone()[0]
            print(f"Registros finais na tabela: {count_final}")
            print(f"Incremento: {count_final - count_inicial}")

    except Exception as e:
        print(f"❌ Erro no processamento: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main_simples()