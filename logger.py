import logging
import sys
import os
from logging.handlers import RotatingFileHandler

# Filtro customizado para ignorar mensagens SQL específicas
def should_ignore_log_message(record):
    # Adicione aqui outros padrões se quiser ignorar mais mensagens
    ignore_patterns = [
        'INSERT INTO estabelecimentos',
        # Adicione outros padrões aqui se necessário
    ]
    return any(pattern in record.getMessage() for pattern in ignore_patterns)

class SQLFilter(logging.Filter):
    def filter(self, record):
        return not should_ignore_log_message(record)

def setup_logging(log_file=r'D:\DB_cnpj_v2\logs\app.log', log_level=logging.INFO, max_bytes=2*1024*1024, backup_count=5):
    """
    Configura logging centralizado para toda a aplicação.
    - Loga para console (stdout) e arquivo com rotação.
    - Suprime logs ruidosos do SQLAlchemy e outros.
    - Usa formatação padronizada.
    """
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    # Handler para console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    console_handler.addFilter(SQLFilter())  # Adiciona filtro

    # Handler para arquivo com rotação
    file_handler = RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count, encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(log_level)
    file_handler.addFilter(SQLFilter())  # Adiciona filtro

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers = []  # Remove handlers duplicados
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    root_logger.addFilter(SQLFilter())  # Adiciona filtro também no root logger

    # Suprime logs do SQLAlchemy e outros ruídos
    for noisy in ['sqlalchemy', 'sqlalchemy.engine', 'sqlalchemy.pool', 'urllib3', 'asyncio', 'chardet']:
        logging.getLogger(noisy).setLevel(logging.WARNING)
        logging.getLogger(noisy).propagate = False

def get_logger(name=None):
    """
    Retorna um logger já configurado para o módulo desejado.
    """
    return logging.getLogger(name)