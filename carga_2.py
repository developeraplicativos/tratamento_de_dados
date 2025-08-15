from sqlalchemy import create_engine, exc
import pandas as pd

user = 'root'
key = 'root'
port = 3306
host  = 'localhost'
banco_origin = 'vendas'
banco_destino = 'vendas_dimensional'

try:
    engineiori = create_engine(f'mysql+pymysql://{user}:{key}@{host}:{port}/{banco_origin}')
    with engineiori.connect() as coonri:
        print(f'conexão com o banco{banco_origin}')
except exc.SQLAlchemyError as ex:
    print(ex)
    exit()

try:
    engineiori2 = create_engine(f'mysql+pymysql://{user}:{key}@{host}:{port}/{banco_destino}')
    with engineiori2.connect() as coonri:
        print(f'conexão com o banco{banco_destino}')
except exc.SQLAlchemyError as ex:
    print(ex)
    exit()

sql = f"""
SELECT cat.id, cat.nomedacategoria
FROM categorias cat 
"""
categoria = pd.read_sql(sql, engineiori)

# print(clientes)
categoria = categoria.reset_index()

# somou 1 a todas as colunas
categoria['index'] = categoria.index + 1
# categoria.info()
  
 
# # Criar coluna id_transacional a partir do 'index' 
categoria['id_transacional'] = categoria['index']
 
# # Ajustar nome do provedor
categoria['categoria'] = categoria['nomedacategoria']
 

categoria = categoria[[
    'id',
    'id_transacional',
    'categoria' 
]] 
try:
    categoria.to_sql('dim_categoria', if_exists='append', index=False, con=engineiori2)
    print('Transferência concluída com sucesso!')
except exc.SQLAlchemyError as ex:
    print(ex, '<< erro') 