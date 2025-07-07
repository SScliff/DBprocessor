# DBprocessor

## Componentes principais

- **main.py**: Script principal. Gerencia o processamento dos dados em paralelo, controlando o fluxo de leitura, transformação e inserção dos chunks no banco.
- **worker.py**: Define como cada chunk é processado (transformação e inserção). Usado pelas threads do main.
- **transform.py**: Funções para limpeza e padronização dos dados antes da inserção.
- **db.py**: Funções para inserir dados no banco de dados, otimizando uso do método COPY do PostgreSQL.
- **engine.py**: Cria e configura a conexão (engine) com o banco de dados.
- **schema_db.py**: Define o esquema da tabela de estabelecimentos e cria índices no banco.
- **reader.py**: Faz a leitura dos dados do CSV em chunks.
- **config.py**: Define as colunas esperadas e importa configurações.
- **settings.py**: Constantes de configuração como caminhos, encoding, tamanho dos chunks etc.
- **logger.py**: Configuração centralizada de logging para todos os módulos.
- **mainTest.py**: Versão simplificada do processamento, útil para testes sem threading.
- **debugg.py**: Ferramentas e funções de diagnóstico e depuração do sistema.
- **utils.py**: Utilitários auxiliares.
- **datasets/**: Pasta para armazenar os arquivos CSV de entrada (ignorada pelo Git).
- **docker/**: Configurações para uso com Docker (banco, compose etc).
- **logs/**: Onde ficam os arquivos de log gerados pelo sistema.
