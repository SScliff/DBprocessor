import os
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
from settings import MAX_WORKERS

# Configuração para melhorar performance em lotes (especialmente útil para SQL Server, mas não atrapalha outros)
@event.listens_for(Engine, 'before_cursor_execute')
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    if executemany:
        try:
            cursor.fast_executemany = True
        except AttributeError:
            pass  # Nem todo driver suporta

# Carrega variáveis de ambiente
dotenv_path = os.getenv('DOTENV_PATH', '.env')
load_dotenv(dotenv_path)

# Configuração do banco de dados
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# Cria a engine com configurações otimizadas
engine = create_engine(
    DATABASE_URL,
    echo=False,
    echo_pool=False,
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_size=MAX_WORKERS,  # Garante uma conexão por worker
    max_overflow=2,  # Conexões extras de segurança
    logging_name=None,
    hide_parameters=True
)
