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
SELECT cl.id, cl.nome, cl.sexo, cl.profissao, cl.nacionalidade AS cod_nacionalidade, pa2.pais AS nacionalidade,
    cl.email, cl.nascimento,
    cl.cadastro, cl.endereco, cl.bairro, cl.cidade, cl.estado, cl.cep,
    pa1.pais
FROM clientes cl
INNER JOIN paises pa1 ON cl.pais = pa1.sigla
INNER JOIN paises pa2 ON cl.nacionalidade = pa2.sigla
"""
clientes = pd.read_sql(sql, engineiori)

# print(clientes)
clientes = clientes.reset_index()

# somou 1 a todas as colunas
clientes['index'] = clientes.index + 1
# clientes.info()
 
# clientes['sexodescricao'] = clientes['sexo'].map({
#     'f':'Feminino',
#     'F':'Feminino',
#     'm':'Masculino',
#     'M':'Masculino',
#     'o':'Outros',
#     'O':'Outros'
# })

clientes['sexodescricao'] = clientes['sexo'].map({
    'f': 'Feminino',
    'F': 'Feminino',
    'm': 'Masculino',
    'M': 'Masculino',
    'o': 'Outros',
    'O': 'Outros'
}) 

clientes['provedor'] = clientes['email'].apply(lambda x: x.split('@')[1])
 
clientes['enderecocompleto'] = clientes['enderecocompleto'] = clientes.apply(
    lambda li: f"{li['endereco']}, {li['cidade']} - {li['estado']}, CEP: {li['cep']}, {li['pais']}",
    axis=1
)
 
# Criar coluna id_transacional a partir do 'index'
clientes['id_transacional'] = clientes['index']

# Ajustar nome do campo de sexo
clientes['sexo_descricao'] = clientes['sexodescricao']

# Ajustar nome do provedor
clientes['provedor_do_cliente'] = clientes['provedor']

# Ajustar nome do endereço completo
clientes['endereco_completo'] = clientes['enderecocompleto']

# print( clientes.columns == clientes.columns ) 


clientes_dim = clientes[[
    'id',
    'id_transacional',
    'nome',
    'sexo',
    'nacionalidade',
    'sexo_descricao',
    'profissao',
    'email',
    'provedor_do_cliente',
    'nascimento',
    'cadastro',
    'endereco_completo',
    'cod_nacionalidade'
]].rename(columns={
    'nome': 'nome_cliente',
    'sexo': 'sexo_codigo'
})
try:
    clientes_dim.to_sql('dim_cliente', if_exists='append', index=False, con=engineiori2)
    print('Transferência concluída com sucesso!')
except exc.SQLAlchemyError as ex:
    print(ex, '<< erro') 