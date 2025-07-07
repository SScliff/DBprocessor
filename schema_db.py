from sqlalchemy import Date, Text, create_engine, Table, Column, Integer, String, MetaData, text
from sqlalchemy.schema import Computed
from engine import DATABASE_URL

engine = create_engine(DATABASE_URL, echo=False)

metadata = MetaData()

tabela_estabelecimentos = Table(
    'estabelecimentos',
    metadata,
    Column('cnpj_basico', Integer, comment='Primeiros 8 dígitos do CNPJ'),
    Column('cnpj_ordem', Integer, comment='Próximos 4 dígitos do CNPJ'),
    Column('cnpj_dv', Integer, comment='Dois dígitos verificadores do CNPJ'),
    Column('identificador_matriz_filial', Integer),
    Column('nome_fantasia', String(60), index=True),
    Column('situacao_cadastral', String(2)),
    Column('data_situacao_cadastral', Date),
    Column('motivo_situacao_cadastral', Integer),
    Column('nome_cidade_exterior', String(60)),
    Column('pais', Integer),
    Column('data_inicio_atividade', Date),
    Column('cnae_fiscal', Integer),
    Column('cnae_secundaria', Text),
    Column('tipo_logradouro', String(40)),
    Column('logradouro', String(80)),
    Column('numero_logradouro', String(60)),
    Column('complemento', Text),
    Column('bairro', String(60)),
    Column('cep', Integer),
    Column('uf', String(5)),
    Column('municipio', String(30)),
    Column('ddd_1', Integer),
    Column('telefone_1', Integer),
    Column('ddd_2', Integer),
    Column('telefone_2', Integer),
    Column('ddd_fax', Integer),
    Column('fax', Integer),
    Column('email', String(255)),
    Column('situacao_especial', String(60)),
    Column('data_situacao_especial', Date, nullable=True),
)

# Cria índices para melhorar a performance das consultas
indexes = [
    'CREATE INDEX IF NOT EXISTS idx_estabelecimentos_cnpj_basico ON estabelecimentos (cnpj_basico)',
    'CREATE INDEX IF NOT EXISTS idx_estabelecimentos_cnae_fiscal ON estabelecimentos (cnae_fiscal)'
]

# A função abaixo só será executada quando o script for chamado diretamente
if __name__ == '__main__':
    print("Criando tabelas e índices no banco de dados...")
    try:
        with engine.connect() as conn:
            metadata.create_all(conn)
            for idx in indexes:
                conn.execute(text(idx))
            conn.commit()
        print("Tabelas e índices criados com sucesso.")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
