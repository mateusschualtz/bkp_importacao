import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from urllib.parse import quote_plus

#VARIÁVEIS
usuario = 'willian.ieger'
senha = '3nDfRxwW53@Q'
arquivo = 'Base.csv'
separador = ';'
nome_tabela_redshift = 'whats_2909'
schema_redshift = 'quiteja_dados'
#preencher 0 para incluir campos na tabela já existente, ou 1 para truncar a tabela e incluir campos
truncate =  0

# criando ferramenta de leitura
if truncate == 0:
    if_exists = 'append'
else:
    if_exists = 'replace'


engine_read = create_engine(f'postgresql://{usuario}:{quote_plus(senha)}@redshift-cluster-datalake.clo3ms2lmdkv.sa-east-1.redshift.amazonaws.com:5439/datalake', echo=False)
session_read = Session(engine_read)

print('Lendo o data Frame')
df = pd.read_csv(arquivo, sep = separador, low_memory = False, encoding='latin-1')

print('Salvando os dados')
df.to_sql(nome_tabela_redshift, con=engine_read, schema=schema_redshift, if_exists = if_exists, method='multi', chunksize=10000, index=False)