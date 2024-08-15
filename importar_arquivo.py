import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from urllib.parse import quote_plus

#VARIÁVEIS

arquivo = "C:\\Python\\Projetos\\Importacao\\metas_meli.csv"
separador = ';'
nome_tabela_redshift = 'meta_meli'
schema_redshift = 'quiteja_develop'
#preencher 0 para incluir campos na tabela já existente, ou 1 para truncar a tabela e incluir campos
truncate =  1

# criando ferramenta de leitura
if truncate == 0:
    if_exists = 'append'
else:
    if_exists = 'replace'


engine_read = create_engine(os.environ["REDSHIFT_READ_URI"], echo=False)
engine_write = create_engine(os.environ["REDSHIFT_WRITE_URI"], echo=False)
session_read = Session(engine_read)

print('Lendo o data Frame')
df = pd.read_csv(arquivo, sep = separador, low_memory = False, encoding='UTF-8')

print('Salvando os dados')
df.to_sql(nome_tabela_redshift, con=engine_read, schema=schema_redshift, if_exists = if_exists, method='multi', chunksize=10000, index=False)