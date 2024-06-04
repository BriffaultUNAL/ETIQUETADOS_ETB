import logging
import sys
import os
from urllib.parse import quote
import sqlalchemy as sa
from sqlalchemy import text, Engine, Connection, Table, DATE
import yaml
import pandas as pd
from pandas import DataFrame
from src.telegram_bot import enviar_mensaje
from pandas.io.sql import SQLTable
import asyncio

logging.basicConfig(
    level=logging.INFO,
    filename=(os.path.join(proyect_dir := os.path.dirname(
        os.path.abspath(__file__)), '..', 'log', 'logs_main.log')),
    format="%(asctime)s - %(levelname)s -  %(message)s",
    datefmt='%d-%b-%y %H:%M:%S'
)

with open(os.path.join(
        proyect_dir, '..', 'config', 'credentials.yml'), 'r') as f:

    try:
        config = yaml.safe_load(f)
        source1, source2, source3, source4 = config['source1'], config[
            'source2'], config['source3'], config['source4']
    except yaml.YAMLError as e:
        logging.error(str(e), exc_info=True)


class Engine_sql:

    def __init__(self, username: str, password: str, host: str, database: str, port: str = 3306) -> None:
        self.user = username
        self.passw = password
        self.host = host
        self.dat = database
        self.port = port

    def get_engine(self) -> Engine:
        return sa.create_engine(f"mysql+pymysql://{self.user}:{quote(self.passw)}@{self.host}:{self.port}/{self.dat}")

    def get_connect(self) -> Connection:
        return self.get_engine().connect()


engine1_60 = Engine_sql(**source1)
engine2_60 = Engine_sql(**source2)
engine_asterisk = Engine_sql(**source3)
engine_68 = Engine_sql(**source4)


def import_query(sql):

    with open(sql, 'r') as f_2:

        try:
            querys = f_2.read()
            query = text(querys)
            return query

        except yaml.YAMLError as e_2:
            logging.error(str(e_2), exc_info=True)


def to_sql_replace(table: SQLTable, con: Engine | Connection, keys: list[str], data_iter):

    satable: Table = table.table
    ckeys = list(map(lambda s: s.replace(' ', '_'), keys))
    data = [dict(zip(ckeys, row)) for row in data_iter]
    values = ', '.join(f':{nm}' for nm in ckeys)
    stmt = f"REPLACE INTO {satable.name} VALUES ({values})"
    con.execute(text(stmt), data)


def extract_to_sql(query, engine):
    with engine as con:
        df = pd.read_sql_query(query, con)
        return df


df_headcount = extract_to_sql(import_query(
    os.path.join(proyect_dir, '..', 'sql', 'df_headcount.sql')), engine2_60.get_connect())
df_recording_log = extract_to_sql(import_query(
    os.path.join(proyect_dir, '..', 'sql', 'df_recording_log.sql')), engine_asterisk.get_connect())


def transform(query_extends: str, df_recording: pd.DataFrame, tmo: int, gestion: str) -> DataFrame:

    df_extend = extract_to_sql(import_query(
        query_extends), engine1_60.get_connect())

    df_join_exthead = pd.DataFrame()

    df_join_exthead = pd.merge(
        df_extend, df_headcount, left_on='user', right_on='Documento', how='left')

    df_join_exthead['aliado'] = 'COS-BOGOTA'

    df_join_exthead['proceso'] = 'ETB'

    df_join_exthead['fecha'] = pd.to_datetime(df_join_exthead['call_date'])

    df_join_exthead['day'] = (df_join_exthead['fecha'].dt.day.astype(
        str)).apply(lambda x: '0'+x if 2 > len(x) else x)

    df_join_exthead['month'] = df_join_exthead['fecha'].dt.month.astype(str)

    df_join_exthead['year'] = df_join_exthead['fecha'].dt.year.astype(str)

    df_join_exthead['date'] = df_join_exthead['day']+'-' + \
        df_join_exthead['month']+'-'+df_join_exthead['year']

    df_join_exthead['hora'] = df_join_exthead['call_date'].astype(
        str).apply(lambda x: x[-8:].replace(':', '-'))

    df_join_exthead = df_join_exthead.drop(['day', 'month', 'year'], axis=1)

    df_join_exthead = df_join_exthead.rename(columns={'length_in_sec': 'TMO',
                                                      'status_name': 'gestion'})

    df_join_exthead['gestion'] = df_join_exthead['gestion'].apply(
        lambda x: str(x).replace(' ', '_'))

    df_join_exthead['fecha'] = pd.to_datetime(
        df_join_exthead['call_date']).dt.strftime('%Y-%m-%d')

    df_recording['lead_id'] = df_recording['lead_id'].astype(str)

    df_recording['fecha'] = pd.to_datetime(
        df_recording['start_time']).dt.strftime('%Y-%m-%d')

    df_join_exthead = pd.merge(
        df_join_exthead, df_recording, on=['fecha', 'lead_id'], how='left')

    df_join_exthead['TMO'] = pd.to_numeric(
        df_join_exthead['TMO'], errors='coerce')

    df_join_exthead = df_join_exthead.dropna(subset='TMO')

    df_join_exthead['TMO'] = df_join_exthead['TMO'].astype(int)

    df_join_exthead = df_join_exthead.query(f'TMO >= {tmo}')

    df_join_exthead['llave'] = df_join_exthead['call_date'].astype(
        str)+'-'+df_join_exthead['phone_number_dialed']

    df_join_exthead = df_join_exthead.drop_duplicates(
        subset='llave').drop(['llave', 'fecha'], axis=1)

    df_join_exthead = df_join_exthead.drop_duplicates(subset='location')

    df_join_exthead['tipo_gestion'] = gestion
    
    df_join_exthead = df_join_exthead[['call_date','phone_number_dialed', 'campaign_id', 'status', 'gestion','user','list_id',
                                       'TMO','lead_id','uniqueid','caller_code','IP_DESCARGA','Documento','Nombres_Apellidos',
                                       'Usuairo_RR', 'aliado', 'proceso','date', 'hora', 'start_time', 'filename','location',
                                       'tipo_gestion','RF']]    

    return df_join_exthead


def load(df: pd.DataFrame):

    with engine_68.get_connect() as con:

        try:

            df.to_sql('tb_etiquetados_etb', con=con, if_exists='append',
                      index=False, dtype={'DATE': DATE}, method=to_sql_replace)

            asyncio.run(enviar_mensaje(f'cargados {len(df)} datos'))
            print(f'cargados {len(df)} datos')
            logging.info(f'Se cargan {len(df)} datos')

        except Exception as e:

            logging.error(str(e), exc_info=True)
            asyncio.run(enviar_mensaje(
                f'error al cargar la data a la tabla, {e}'))
