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


# exit()
sql = f"""
SELECT fo.id as id_transacional, fo.empresa as fornecedor, fo.telefone as contato
FROM fornecedores as fo
"""

fornecedor =  pd.read_sql(sql, engineiori)

# fornecedor.info()

fornecedor['id'] = fornecedor.index + 1
# print(fornecedor)
try:
    fornecedor.to_sql('dim_fornecedor', if_exists='append', index=False, con=engineiori2)
    print('Transferência concluída com sucesso!')
except exc.SQLAlchemyError as ex:
    print(ex, '<< erro') 
