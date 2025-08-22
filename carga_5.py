from sqlalchemy import create_engine, exc
import pandas as pd

user = 'root'
key = 'root'
port = 3306
host  = 'localhost'
banco_origin = 'vendas'
banco_destino = 'vendas_dimensional'

try:
    conexao1 = create_engine(f'mysql+pymysql://{user}:{key}@{host}:{port}/{banco_origin}')
    with conexao1.connect() as conn1:
        print(f'conexão ativa CON1')
except exc.SQLAlchemyError as ex:
    print(ex)
    exit()

try:
    conexao2 = create_engine(f'mysql+pymysql://{user}:{key}@{host}:{port}/{banco_destino}')
    with conexao2.connect() as conn2:
        print(f'conexão ativa CON2')
except exc.SQLAlchemyError as ex:
    print(ex)
    exit()

sql = f"""SELECT id AS id_transacional, sigla, pais FROM paises"""
paises = pd.read_sql(sql, conexao1)
# paises.info()
paises = paises.reset_index()
paises['id'] = paises.index + 1
paises['pais'] = paises['pais'][:100]

paises = paises[['id','id_transacional', 'sigla','pais' ]]
paises = paises[paises['pais'].notnull()] 
# print( paises  )
paises.to_sql('dim_pais',if_exists='append', index=False,con=conexao2)