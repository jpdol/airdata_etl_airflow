from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import pendulum

@dag(
    dag_id="bimtra_extraction",
    schedule="0 * * * *",  # Executa de hora em hora
    max_active_runs=1
)
def bimtra_extraction():
    POSTGRES_CONN_ID = "postgres"

    # 1. Criação da tabela baseada exatamente no JSON que você enviou
    create_table = SQLExecuteQueryOperator(
        task_id="create_table_bimtra",
        conn_id=POSTGRES_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS airdata.bimtra (
            codmovimentovalidado BIGINT PRIMARY KEY,
            deparr VARCHAR(2),
            numvoo VARCHAR(20),
            matricula VARCHAR(20),
            adpartida VARCHAR(4),
            addestino VARCHAR(4),
            transponder VARCHAR(20),
            nivelvoo VARCHAR(20),
            tipoaeronave VARCHAR(20),
            esteiraturb VARCHAR(10),
            tipovoo VARCHAR(10),
            rota TEXT,
            saida VARCHAR(100),
            proceddescida VARCHAR(100),
            box VARCHAR(50),
            fixoentrada VARCHAR(100),
            dhfixoentrada TIMESTAMP,
            fixosaida VARCHAR(100),
            dhfixosaida TIMESTAMP,
            regravoo VARCHAR(10),
            dhinseridobd TIMESTAMP,
            orgaoats VARCHAR(50),
            regraad VARCHAR(10),
            editado VARCHAR(10),
            codremessa VARCHAR(50),    -- Mudado para VARCHAR por segurança
            dhvalidado TIMESTAMP,
            operacao VARCHAR(10),
            dhmovreal TIMESTAMP,
            dhmovprev TIMESTAMP,
            dhmovestm TIMESTAMP,
            tipocruzamento VARCHAR(100),
            pob VARCHAR(20),           -- Mudado para VARCHAR
            altn VARCHAR(20),
            altn2 VARCHAR(20),
            autonomia VARCHAR(50),
            rmk TEXT,
            operador VARCHAR(50),
            rmkapp TEXT,
            equipamento1 VARCHAR(200),
            equipamento2 VARCHAR(200),
            eet VARCHAR(50),
            outrosdados TEXT,
            numaeronaves VARCHAR(20),  -- Mudado para VARCHAR
            dac VARCHAR(100),
            tqaligado VARCHAR(20),     -- Mudado para VARCHAR
            taxiway VARCHAR(100),
            numremessa VARCHAR(50),    -- Mudado para VARCHAR
            velocidade VARCHAR(50),
            pista VARCHAR(20),
            tgl VARCHAR(20),           -- Mudado para VARCHAR
            codlote VARCHAR(50),       -- Mudado para VARCHAR
            dt_insercao_airflow TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    @task
    def get_last_cod_movimento() -> int:
        """Busca o último ID no banco ou o valor inicial da Variável do Airflow."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        row = hook.get_first("SELECT MAX(codmovimentovalidado) FROM airdata.bimtra;")
        last_id = row[0] if row and row[0] is not None else None

        if last_id is None:
            # Pega o valor da variável de ambiente que você mencionou
            initial_code = Variable.get('initial_code_bimtra', default_var=0)
            print(f"Iniciando carga do código base: {initial_code}")
            return int(initial_code)

        return int(last_id)

    @task
    def update_bimtra_data(last_id: int):
        import requests
        import pandas as pd
        from psycopg2.extras import execute_values
        from psycopg2 import sql

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Sua URL conforme especificado
        base_url = (
            "https://odin-ms.icea.decea.mil.br/api/bimtra"
            "?codmovimentovalidado=gt.{last_id}&order=codmovimentovalidado.asc&limit=1000&offset={offset}"
        )

        offset = 0
        total_geral = 0
        conn = hook.get_conn()

        try:
            with conn.cursor() as cur:
                while True:
                    url = base_url.format(last_id=last_id, offset=offset)
                    print(f"[BIMTRA] Buscando: {url}")
                    
                    r = requests.get(url, timeout=60)
                    r.raise_for_status()
                    
                    data = r.json()
                    if not data:
                        print("[BIMTRA] Fim dos dados.")
                        break

                    df = pd.DataFrame(data)
                    # Converte NaNs do Pandas para None do Python (vira NULL no Postgres)
                    df = df.where(pd.notnull(df), None)

                    cols = list(df.columns)
                    values = [tuple(row) for row in df.to_numpy()]

                    # Inserção robusta com tratamento de conflito
                    insert_stmt = sql.SQL("""
                        INSERT INTO {schema}.{table} ({fields})
                        VALUES %s
                        ON CONFLICT (codmovimentovalidado) DO NOTHING
                    """).format(
                        schema=sql.Identifier("airdata"),
                        table=sql.Identifier("bimtra"),
                        fields=sql.SQL(", ").join(sql.Identifier(c) for c in cols),
                    )

                    execute_values(cur, insert_stmt.as_string(conn), values, page_size=1000)
                    conn.commit()

                    total_geral += len(df)
                    print(f"[BIMTRA] Inseridos {len(df)} registros. Total: {total_geral}")

                    if len(data) < 1000:
                        break
                    
                    offset += 1000
        finally:
            conn.close()

    # Fluxo da DAG
    last_id_ref = get_last_cod_movimento()
    create_table >> last_id_ref >> update_bimtra_data(last_id_ref)

bimtra_extraction()