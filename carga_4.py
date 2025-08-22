from sqlalchemy import create_engine, exc
import pandas as pd

user = 'root'
key = 'root'
port = 3306
host  = 'localhost'
banco_origin = 'vendas_emerson'
banco_destino = 'vendas_emerson_dimensional'

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


def semestre(mes):
    return 1 if mes <= 6 else 2

def quadrimestre(mes):
    if mes < 5:
        return 1 
    if mes < 9:
        return 2 
    else:
        return 3

def bimestre(mes):
    if mes < 3:
        return 1 
    if mes < 5:
        return 2 
    if mes < 7:
        return 3 
    if mes < 9:
        return 4 
    if mes < 11:
        return 5 
    else:
        return 6


def dia_da_semana(dia):
    if dia == 6:
        return 1
    else: 
        return dia + 2

inicio = '2000-01-01'
fim = '2050-12-31'

df_tempo = pd.date_range(start=inicio, end=fim, freq='D')

dias_pt = ('segunda-feira','terça-feira','quarta-feira','quinta-feira','sexta-feira','sábado','domingo')
mes_pt = ('janeiro','fevereiro','março','abril','maio','junho','julho','agosto','setembro','outubro','novembro','dezembro')

dim_tempo = pd.DataFrame({ 
    'data': df_tempo, 
    'dia': df_tempo.day,
    'mes': df_tempo.month,
    'ano': df_tempo.year,
    'data_juliana': df_tempo.strftime('%Y%J'),
    'semestre': df_tempo.month.map(semestre),
    'quadrimestre': df_tempo.month.map(quadrimestre),
    'trimestre': df_tempo.quarter,
    'bimestre': df_tempo.month.map(bimestre),
    'nome_mes': df_tempo.dayofweek.map(lambda x: mes_pt[x-1]), 
    'dia_da_semana': df_tempo.dayofweek.map(dia_da_semana),
    'nome_dia_da_semana': df_tempo.dayofweek.map(lambda x: dias_pt[x]),
    'semana_do_ano': df_tempo.isocalendar().week,
    'data_string': df_tempo.strftime('%d/%m/%Y'),
    'dia_no_ano': df_tempo.dayofyear,
    'ultimo_dia_mes': (df_tempo + pd.offsets.MonthEnd(0)).day,
    'fim_de_semana': df_tempo.day_of_week > 5,
})

dim_tempo.insert(0,'id', range(1, len(dim_tempo) + 1))

dim_tempo.to_sql('dim_tempo', con=engineiori2, if_exists='append', index=False)