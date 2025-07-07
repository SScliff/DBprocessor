from logger import setup_logging, get_logger
setup_logging()
logger = get_logger(__name__)

import pandas as pd
from settings import CSV_PATH, CHUNKSIZE, ENCODING, DELIMITER
from config import COLUNAS

def ler_chunks():
    return pd.read_csv(
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
