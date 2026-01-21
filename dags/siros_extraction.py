from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import io
import zipfile
import requests
import pandas as pd
from psycopg2.extras import execute_values
from psycopg2 import sql

@dag(
    dag_id="siros_extraction",
    schedule="0 * * * *",  # Uma vez por hora
    max_active_runs=1,
    catchup=False
)
def siros_extraction():
    """DAG para extração dos dados do SIROS (ANAC) via arquivo ZIP"""

    POSTGRES_CONN_ID = "postgres"
    URL_ZIP = "https://siros.anac.gov.br/SIROS/registros/voos/voos.zip"

    # 1. Apaga a tabela caso exista e cria uma nova
    # Como você quer "apagar e subir os dados", o DROP garante que a tabela esteja limpa
    recreate_table = SQLExecuteQueryOperator(
        task_id="recreate_table",
        conn_id=POSTGRES_CONN_ID,
        sql="""
        DROP TABLE IF EXISTS airdata.siros;
        CREATE TABLE airdata.siros (
            data_referencia_utc timestamp,
            sigla_icao_empresa_aerea varchar(10),
            nome_empresa_aerea varchar(255),
            numero_etapa int,
            numero_voo varchar(20),
            sigla_icao_modelo_aeronave varchar(10),
            quantidade_assentos_previstos int,
            sigla_icao_aeroporto_origem varchar(10),
            data_partida_prevista_utc timestamp,
            sigla_icao_aeroporto_destino varchar(10),
            data_chegada_prevista_utc timestamp,
            tipo_de_voo varchar(255),
            dt_insercao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    @task
    def process_and_upload_siros():
        """Baixa o ZIP, extrai em memória e sobe para o Postgres."""
        
        print(f"[SIROS] Baixando arquivo de {URL_ZIP}")
        response = requests.get(URL_ZIP, timeout=300)
        if response.status_code != 200:
            raise Exception(f"Erro ao baixar arquivo: {response.status_code}")

        # Lendo o ZIP em memória para não deixar rastro em disco
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            # Pega o primeiro arquivo dentro do ZIP
            filename = z.namelist()[0]
            with z.open(filename) as f:
                # Lendo CSV com separador ; e encoding latin1 (comum na ANAC)
                df = pd.read_csv(f, sep=';', encoding='latin1')

        # Ajuste de nomes de colunas para o banco (remover espaços e acentos se necessário)
        # Mapeando conforme a ordem do seu exemplo
        df.columns = [
            'data_referencia_utc', 'sigla_icao_empresa_aerea', 'nome_empresa_aerea',
            'numero_etapa', 'numero_voo', 'sigla_icao_modelo_aeronave',
            'quantidade_assentos_previstos', 'sigla_icao_aeroporto_origem',
            'data_partida_prevista_utc', 'sigla_icao_aeroporto_destino',
            'data_chegada_prevista_utc', 'tipo_de_voo'
        ]

        # Converter colunas de data (ajuste o formato se necessário)
        date_cols = [
            'data_referencia_utc', 
            'data_partida_prevista_utc', 
            'data_chegada_prevista_utc'
        ]
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')

        # Tratar nulos para o Postgres
        df = df.where(pd.notnull(df), None)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        
        try:
            with conn.cursor() as cur:
                cols = list(df.columns)
                values = [tuple(row) for row in df.to_numpy()]

                insert_stmt = sql.SQL("""
                    INSERT INTO {schema}.{table} ({fields})
                    VALUES %s
                """).format(
                    schema=sql.Identifier("airdata"),
                    table=sql.Identifier("siros"),
                    fields=sql.SQL(", ").join(sql.Identifier(c) for c in cols),
                )

                print(f"[SIROS] Inserindo {len(df)} registros...")
                execute_values(cur, insert_stmt.as_string(conn), values, page_size=2000)
                conn.commit()
                print("[SIROS] Upload concluído com sucesso.")
        finally:
            conn.close()

    # Fluxo: Primeiro recria a tabela, depois processa os dados
    recreate_table >> process_and_upload_siros()

siros_extraction()