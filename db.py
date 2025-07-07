from engine import engine
import pandas as pd
from logger import setup_logging, get_logger
import warnings
from settings import DELIMITER
from io import StringIO

setup_logging()  # Garante configuração do logger antes de obter o logger
logger = get_logger("db.insercao")

# Função callable para usar com o método 'COPY' do PostgreSQL, muito mais rápido
def inserir_com_copy(table, conn, keys, data_iter):
    """
    Executa a inserção de dados em massa usando o comando COPY do PostgreSQL.
    Este método é significativamente mais rápido que o `to_sql` padrão.
    """
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cursor:
        # Cria um buffer de string para formatar os dados como um CSV em memória
        s_buf = StringIO()
        
        # Converte o iterador de dados em um DataFrame do pandas
        df = pd.DataFrame(data_iter, columns=keys)

        # Pega as colunas da tabela do banco de dados
        table_cols = [c.name for c in table.columns]
        
        # Filtra o DataFrame para conter apenas as colunas que existem na tabela
        df_filtered = df[[col for col in df.columns if col in table_cols]]
        
        # Escreve o DataFrame filtrado no buffer
        df_filtered.to_csv(s_buf, header=False, index=False, sep=DELIMITER)
        s_buf.seek(0)

        # Constrói o comando COPY com as colunas corretas
        columns_sql = '({})'.format(', '.join(df_filtered.columns))
        sql = f"COPY {table.name} {columns_sql} FROM STDIN WITH (FORMAT CSV, DELIMITER '{DELIMITER}')"

        # Executa o comando COPY
        cursor.copy_expert(sql=sql, file=s_buf)

def inserir_data(chunk, tabela):
    """
    Insere um DataFrame em uma tabela do banco de dados.
    Args:
        chunk: DataFrame pandas
        tabela: nome da tabela destino
    Returns:
        dict: {'sucesso': bool, 'registros': int, 'erro': str ou None}
    """
    if chunk.empty:
        msg = "Nenhum dado fornecido para inserção"
        logger.warning(msg)
        return {'sucesso': False, 'registros': 0, 'erro': msg}

    registros_afetados = len(chunk)
    logger.info(f"Preparando para inserir {registros_afetados} registros na tabela '{tabela}'")
    try:
        with engine.begin() as conn:
            chunk.to_sql(
                tabela,
                con=conn,
                if_exists='append',
                index=False,
                method=inserir_com_copy
            )
        logger.info(f" {registros_afetados} registros inseridos com sucesso na tabela '{tabela}'")
        return {'sucesso': True, 'registros': registros_afetados, 'erro': None}
    except Exception as e:
        logger.error(f"Erro ao inserir dados na tabela '{tabela}': {str(e)}")
        return {'sucesso': False, 'registros': 0, 'erro': "Erro ao inserir dados. Consulte o log para detalhes."}

if __name__ == "__main__":
    print("=== TESTE DE INSERÇÃO DE DADOS ===")
    dados_teste = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    resultado = inserir_data(dados_teste, 'estabelecimentos')
    if resultado['sucesso']:
        print(f"\n Teste concluído! {resultado['registros']} registro(s) inserido(s) com sucesso!")
    else:
        print("\n Ocorreu um erro durante o teste de inserção.")
        print(resultado['erro'])
