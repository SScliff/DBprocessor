from logger import setup_logging, get_logger
setup_logging()
logger = get_logger(__name__)

import os
import pandas as pd
from config import CSV_PATH, CHUNKSIZE, ENCODING, DELIMITER, COLUNAS
from engine import engine
from sqlalchemy import text


def diagnosticar_sistema():
    print("=== DIAGNÓSTICO DO SISTEMA ===\n")

    # 1. Verificar arquivo CSV
    print("1. VERIFICANDO ARQUIVO CSV:")
    print(f"   Caminho: {CSV_PATH}")

    if not os.path.exists(CSV_PATH):
        print("   ERRO: Arquivo não encontrado!")
        return

    file_size = os.path.getsize(CSV_PATH)
    print(f"   Arquivo existe")
    print(f"   Tamanho: {file_size / (1024 * 1024):.2f} MB")

    # 2. Verificar primeiras linhas do CSV
    print("\n2. VERIFICANDO CONTEÚDO DO CSV:")
    try:
        with open(CSV_PATH, 'r', encoding=ENCODING) as f:
            primeiras_linhas = [f.readline().strip() for _ in range(5)]

        print(f"   Encoding {ENCODING} funciona")
        print(f"   Primeiras 5 linhas:")
        for i, linha in enumerate(primeiras_linhas, 1):
            print(f"      {i}: {linha[:100]}{'...' if len(linha) > 100 else ''}")

        # Verificar delimitador
        primeira_linha = primeiras_linhas[0] if primeiras_linhas else ""
        qtd_delimitadores = primeira_linha.count(DELIMITER)
        print(f"   Delimitadores '{DELIMITER}' encontrados: {qtd_delimitadores}")
        print(f"   Colunas esperadas: {len(COLUNAS)}")

    except Exception as e:
        print(f"   Erro ao ler arquivo: {e}")
        return

    # 3. Testar leitura com pandas
    print("\n3. TESTANDO LEITURA COM PANDAS:")
    try:
        # Ler apenas as primeiras linhas
        df_teste = pd.read_csv(
            CSV_PATH,
            sep=DELIMITER,
            dtype=str,
            nrows=5,
            encoding=ENCODING,
            low_memory=False,
            header=None,
            names=COLUNAS,
            on_bad_lines="skip"
        )

        print(f"   Leitura bem-sucedida")
        print(f"   Shape: {df_teste.shape}")
        print(f"   Colunas: {list(df_teste.columns)}")
        print(f"   Primeiros registros:")
        for i, row in df_teste.head(2).iterrows():
            print(f"      Linha {i}: {dict(row)}")

    except Exception as e:
        print(f"   Erro na leitura: {e}")
        return

    # 4. Testar chunks
    print("\n4. TESTANDO CHUNKS:")
    try:
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
            chunk_count += 1
            total_registros += len(chunk)
            print(f"   Chunk {i}: {len(chunk)} registros")

            # Mostrar apenas os primeiros 3 chunks
            if i >= 2:
                print(f"   ... (continuando)")
                break

        print(f"   Total de chunks processados: {chunk_count}")
        print(f"   Total de registros: {total_registros}")

    except Exception as e:
        print(f"   Erro ao processar chunks: {e}")
        return

    # 5. Testar conexão com banco
    print("\n5. TESTANDO CONEXÃO COM BANCO:")
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"   Conexão OK - PostgreSQL: {version}")

            # Verificar tabela
            result = conn.execute(text("SELECT COUNT(*) FROM estabelecimentos"))
            count = result.fetchone()[0]
            print(f"   Registros na tabela: {count}")

            # Verificar estrutura da tabela
            result = conn.execute(text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'estabelecimentos'
                ORDER BY ordinal_position
            """))
            colunas_db = result.fetchall()
            print(f"   Colunas na tabela: {len(colunas_db)}")

    except Exception as e:
        print(f"   Erro na conexão: {e}")
        return

    # 6. Teste de inserção simples
    print("\n6. TESTE DE INSERÇÃO SIMPLES:")
    try:
        # Criar um registro de teste
        dados_teste = {
            'cnpj_basico': 12345678,
            'cnpj_ordem': 1,
            'cnpj_dv': 90,
            'identificador_matriz_filial': 1,
            'nome_fantasia': 'TESTE LTDA',
            'situacao_cadastral': '02',
            'data_situacao_cadastral': '2023-01-01',
            'motivo_situacao_cadastral': 0,
            'nome_cidade_exterior': '',
            'pais': 105,
            'data_inicio_atividade': '2023-01-01',
            'cnae_fiscal': 6204000,
            'cnae_secundaria': '',
            'tipo_logradouro': 'RUA',
            'logradouro': 'TESTE',
            'numero_logradouro': '123',
            'complemento': '',
            'bairro': 'CENTRO',
            'cep': 12345678,
            'uf': 'SP',
            'municipio': 'SAO PAULO',
            'ddd_1': 11,
            'telefone_1': 999999999,
            'ddd_2': None,
            'telefone_2': None,
            'ddd_fax': None,
            'fax': None,
            'email': 'teste@teste.com',
            'situacao_especial': '',
            'data_situacao_especial': None
        }

        df_teste = pd.DataFrame([dados_teste])

        with engine.connect() as conn:
            # Remover registros de teste anteriores
            conn.execute(text("DELETE FROM estabelecimentos WHERE cnpj_basico = 12345678"))
            conn.commit()

        resultado = df_teste.to_sql(
            'estabelecimentos',
            con=engine,
            if_exists='append',
            index=False,
            method='multi'
        )

        print(f"   Inserção teste OK - Resultado: {resultado}")

        # Verificar se foi inserido
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM estabelecimentos WHERE cnpj_basico = 12345678"))
            count = result.fetchone()[0]
            print(f"   Registros de teste inseridos: {count}")

            # Limpar teste
            conn.execute(text("DELETE FROM estabelecimentos WHERE cnpj_basico = 12345678"))
            conn.commit()

    except Exception as e:
        print(f"   Erro no teste de inserção: {e}")
        import traceback
        traceback.print_exc()
        return

    print("\n=== DIAGNÓSTICO CONCLUÍDO ===")
    print("Sistema parece estar funcionando corretamente!")
    print("Se ainda não há inserção, o problema pode estar no processamento dos chunks em paralelo")


if __name__ == "__main__":
    diagnosticar_sistema()