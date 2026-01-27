from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import zipfile
from datetime import datetime


@dag(dag_id="siros_extraction", schedule="0 * * * *", max_active_runs=1)
def siros_extraction():
    """DAG para extração e atualização dos dados do SIROS (Sistema de Informações de Registros de Operações de Serviço)"""

    POSTGRES_CONN_ID = "postgres"  # mesma Connection usada no Airflow

    # Task para dropar e criar a tabela novamente
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=POSTGRES_CONN_ID,
        sql="""
        DROP TABLE IF EXISTS airdata.siros;
        CREATE TABLE airdata.siros (
            data_referencia_utc TIMESTAMP,
            sigla_icao_empresa_aerea VARCHAR(10),
            nome_empresa_aerea VARCHAR(100),
            numero_etapa INTEGER,
            numero_voo VARCHAR(10),
            sigla_icao_modelo_aeronave VARCHAR(10),
            quantidade_assentos_previstos INTEGER,
            sigla_icao_aeroporto_origem VARCHAR(10),
            data_partida_prevista_utc TIMESTAMP,
            sigla_icao_aeroporto_destino VARCHAR(10),
            data_chegada_prevista_utc TIMESTAMP,
            tipo_de_voo VARCHAR(50),
            dt_insercao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    @task
    def download_siros_zip():
        """Faz o download do arquivo ZIP do SIROS"""
        # Import inside the task to handle dependencies in Airflow environment
        import requests

        URL_ZIP = "https://siros.anac.gov.br/SIROS/registros/voos/voos.zip"

        # Define o diretório temporário para armazenamento
        temp_dir = "/tmp/siros_data"
        os.makedirs(temp_dir, exist_ok=True)

        # Caminho completo para o arquivo ZIP
        zip_path = os.path.join(temp_dir, "voos.zip")

        print(f"[SIROS] Baixando arquivo ZIP de {URL_ZIP}")
        response = requests.get(URL_ZIP, timeout=300)

        if response.status_code == 200:
            with open(zip_path, 'wb') as f:
                f.write(response.content)
            print(f"[SIROS] Arquivo ZIP baixado com sucesso: {zip_path}")
            return zip_path
        else:
            raise Exception(f"[SIROS] Falha ao baixar o arquivo ZIP. Status code: {response.status_code}")


    @task
    def extract_csv_from_zip(zip_path: str):
        """Extrai o arquivo CSV do ZIP e retorna o caminho do CSV"""
        temp_dir = "/tmp/siros_data"
        csv_filename = "voos.csv"
        csv_path = os.path.join(temp_dir, csv_filename)

        print(f"[SIROS] Extraindo CSV de {zip_path}")

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Extrai o arquivo CSV
            zip_ref.extract(csv_filename, temp_dir)

        print(f"[SIROS] Arquivo CSV extraído com sucesso: {csv_path}")
        return csv_path


    @task
    def load_csv_to_postgres(csv_path: str):
        """Carrega o arquivo CSV para o PostgreSQL"""
        # Import inside the task to handle dependencies in Airflow environment
        import pandas as pd

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        print(f"[SIROS] Lendo arquivo CSV: {csv_path}")

        # Lê o CSV, pulando a primeira linha (header)
        df = pd.read_csv(csv_path, sep=';', skiprows=1, encoding='utf-8')

        # Converte colunas de data para datetime
        date_columns = [
            'Data Referência UTC',
            'Data Partida Prevista UTC',
            'Data Chegada Prevista UTC'
        ]

        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format='%d/%m/%Y %H:%M:%S', errors='coerce')

        # Renomeia as colunas para seguir convenções do banco de dados
        column_mapping = {
            'Data Referência UTC': 'data_referencia_utc',
            'Sigla ICAO Empresa Aérea': 'sigla_icao_empresa_aerea',
            'Nome da Empresa Aérea': 'nome_empresa_aerea',
            'Número Etapa': 'numero_etapa',
            'Número Voo': 'numero_voo',
            'Sigla ICAO Modelo Aeronave': 'sigla_icao_modelo_aeronave',
            'Quantidade de Assentos Previstos': 'quantidade_assentos_previstos',
            'Sigla ICAO Aeroporto Origem': 'sigla_icao_aeroporto_origem',
            'Data Partida Prevista UTC': 'data_partida_prevista_utc',
            'Sigla ICAO Aeroporto Destino': 'sigla_icao_aeroporto_destino',
            'Data Chegada Prevista UTC': 'data_chegada_prevista_utc',
            'Tipo de Voo': 'tipo_de_voo'
        }

        df = df.rename(columns=column_mapping)

        # Conecta ao banco e insere os dados
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                # Prepara os dados para inserção
                for index, row in df.iterrows():
                    # Converte valores None para NULL no SQL
                    values = []
                    for val in row:
                        if pd.isna(val):
                            values.append(None)
                        elif isinstance(val, pd.Timestamp):
                            values.append(val.strftime('%Y-%m-%d %H:%M:%S'))
                        else:
                            values.append(val)

                    # Monta a query de inserção
                    insert_query = """
                        INSERT INTO airdata.siros (
                            data_referencia_utc,
                            sigla_icao_empresa_aerea,
                            nome_empresa_aerea,
                            numero_etapa,
                            numero_voo,
                            sigla_icao_modelo_aeronave,
                            quantidade_assentos_previstos,
                            sigla_icao_aeroporto_origem,
                            data_partida_prevista_utc,
                            sigla_icao_aeroporto_destino,
                            data_chegada_prevista_utc,
                            tipo_de_voo
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cur.execute(insert_query, tuple(values))

                conn.commit()
                print(f"[SIROS] Dados carregados com sucesso. Total de registros: {len(df)}")
        finally:
            conn.close()


    # Orquestração das tasks
    downloaded_zip = download_siros_zip()
    extracted_csv = extract_csv_from_zip(downloaded_zip)
    load_task = load_csv_to_postgres(extracted_csv)

    create_table >> downloaded_zip >> extracted_csv >> load_task


siros_extraction()