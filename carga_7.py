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

"""id
id_transacional
produto
preco_unitario
descontinuado"""

sql = f"""SELECT id as id_transacional,
produto,
precounitario as preco_unitario,
descontinuado FROM produtos"""
produtos = pd.read_sql(sql, conexao1)
# produtos.info()
produtos = produtos.reset_index()
produtos['id'] = produtos.index + 1 

produtos = produtos[['id','id_transacional','produto','preco_unitario','descontinuado' ]] 
# print( produtos  )
produtos.to_sql('dim_produto',if_exists='append', index=False,con=conexao2)