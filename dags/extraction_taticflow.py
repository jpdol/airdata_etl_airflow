from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


@dag(dag_id="taticflow_extraction", schedule="0 * * * *", max_active_runs=1)
def taticflow_extraction():
    """DAG para extração e atualização dos dados da Tatic Flow"""

    POSTGRES_CONN_ID = "postgres"  # mesma Connection usada no Airflow

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=POSTGRES_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS airdata.taticflow (
            id varchar(36) NOT NULL,
            flowid varchar(36) NOT NULL,
            createdat timestamp(3) NOT NULL,
            locality varchar(4) NOT NULL,
            callsign varchar(7) NOT NULL,
            adep varchar(4) NULL,
            ades varchar(4) NULL,
            rv varchar(2) NULL,
            flighttype varchar(2) NULL,
            runway varchar(15) NULL,
            equipment varchar(70) NULL,
            acfttype varchar(4) NULL,
            eventtype varchar(3) NOT NULL,
            eobt timestamp(3) NULL,
            wpush timestamp(3) NULL,
            cpush timestamp(3) NULL,
            wtaxi timestamp(3) NULL,
            taxi timestamp(3) NULL,
            "hold" timestamp(3) NULL,
            crwy timestamp(3) NULL,
            cdep timestamp(3) NULL,
            dep timestamp(3) NULL,
            eta timestamp(3) NULL,
            arr timestamp(3) NULL,
            cpos timestamp(3) NULL,
            transponder varchar(4) NULL,
            CONSTRAINT flowmovements_flowid_key UNIQUE (flowid),
            CONSTRAINT flowmovements_pkey PRIMARY KEY (id),
            dt_insercao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    @task
    def get_last_update() -> str:
        """Obtém a última data registrada na tabela airdata.taticflow usando a conexão do Airflow."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        row = hook.get_first("SELECT MAX(createdat) FROM airdata.taticflow;")
        last_date = row[0] if row else None

        if last_date is None:
            print("Nenhum dado encontrado — iniciando de 2025-07-31")
            initial_date_str = Variable.get('initial_date')
			return initial_date_str

        return last_date.isoformat(timespec="milliseconds")

    @task
    def update_taticflow_data(last_date: str):
        """Atualiza dados do Tatic Flow a partir da última data registrada, sem SQLAlchemy (robusto)."""
        import requests
        import pandas as pd
        from psycopg2.extras import execute_values
        from psycopg2 import sql

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        base_url = (
            "https://odin-ms.icea.decea.mil.br/api/tatic_flow"
            "?createdat=gt.{date}&order=createdat.asc&limit=1000&offset={offset}"
        )

        offset = 0
        total = 0

        conn = hook.get_conn()  # conexão psycopg2 do Airflow
        try:
            with conn.cursor() as cur:
                while True:
                    url = base_url.format(date=last_date, offset=offset)
                    print(f"[TATIC_FLOW] Requisitando {url}")
                    r = requests.get(url, timeout=60)

                    if r.status_code != 200:
                        print(f"[TATIC_FLOW] Erro {r.status_code} na requisição. Encerrando.")
                        break

                    data = r.json()
                    if not data:
                        print("[TATIC_FLOW] Nenhum dado retornado. Fim da atualização.")
                        break

                    df = pd.DataFrame(data)
                    if df.empty:
                        break

                    # NaN -> None (para virar NULL no Postgres)
                    df = df.where(pd.notnull(df), None)

                    cols = list(df.columns)
                    values = [tuple(row) for row in df.to_numpy()]

                    insert_stmt = sql.SQL("""
                        INSERT INTO {schema}.{table} ({fields})
                        VALUES %s
                        ON CONFLICT (flowid) DO NOTHING
                    """).format(
                        schema=sql.Identifier("airdata"),
                        table=sql.Identifier("taticflow"),
                        fields=sql.SQL(", ").join(sql.Identifier(c) for c in cols),
                    )

                    # Bulk insert rápido
                    execute_values(cur, insert_stmt.as_string(conn), values, page_size=1000)
                    conn.commit()

                    print(f"[TATIC_FLOW] Inseridos {len(df)} registros (offset={offset}).")

                    total += len(df)
                    offset += 1000

                    if len(data) < 1000:
                        print("[TATIC_FLOW] Última página alcançada.")
                        break
        finally:
            conn.close()

        print(f"[TATIC_FLOW] Atualização concluída. Total inserido: {total}")

    last_update = get_last_update()
    update_task = update_taticflow_data(last_update)

    create_table >> last_update >> update_task


taticflow_extraction()