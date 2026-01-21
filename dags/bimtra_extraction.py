from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


@dag(dag_id="bimtra_extraction", schedule="10 * * * *", max_active_runs=1)
def bimtra_extraction():
    """DAG para extração e atualização dos dados da BIMTRA"""

    POSTGRES_CONN_ID = "postgres"  # mesma Connection usada no Airflow

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=POSTGRES_CONN_ID,
        sql="""
        CREATE TABLE airdata.bimtra (
        deparr varchar(4) NULL,
        numvoo varchar(7) NULL,
        matricula varchar(10) NULL,
        adpartida varchar(4) NULL,
        addestino varchar(4) NULL,
        transponder varchar(4) NULL,
        nivelvoo varchar(4) NULL,
        tipoaeronave varchar(7) NULL,
        esteiraturb varchar(1) NULL,
        tipovoo varchar(1) NULL,
        rota varchar(1024) NULL,
        saida varchar(30) NULL,
        proceddescida varchar(4) NULL,
        box varchar(50) NULL,
        fixoentrada varchar(10) NULL,
        dhfixoentrada timestamp(3) NULL,
        fixosaida varchar(10) NULL,
        dhfixosaida timestamp(3) NULL,
        regravoo varchar(2) NULL,
        dhinseridobd timestamp(3) NULL,
        orgaoats varchar(4) NULL,
        regraad varchar(1) NULL,
        editado varchar(1) DEFAULT '0' NULL,
        codmovimentovalidado int4 NOT NULL,
        codremessa int4 NULL,
        dhvalidado timestamp(3) NULL,
        operacao varchar(1) NULL,
        dhmovreal timestamp(3) NULL,
        dhmovprev timestamp(3) NULL,
        dhmovestm timestamp(3) NULL,
        tipocruzamento varchar(3) NULL,
        pob int4 NULL,
        altn varchar(4) NULL,
        altn2 varchar(4) NULL,
        autonomia varchar(4) NULL,
        rmk varchar(35) NULL,
        operador varchar(4) NULL,
        rmkapp varchar(180) NULL,
        equipamento1 varchar(25) NULL,
        equipamento2 varchar(1) NULL,
        eet varchar(4) NULL,
        outrosdados varchar(2048) NULL,
        numaeronaves int4 NULL,
        dac varchar(6) NULL,
        tqaligado int4 NULL,
        taxiway varchar(50) NULL,
        numremessa int4 NULL,
        velocidade varchar(5) NULL,
        pista varchar(5) NULL,
        tgl int4 NULL,
        codlote varchar(9) NULL
        dt_insercao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    @task
    def get_last_update() -> str:
        """Obtém a última data registrada na tabela airdata.bimtra usando a conexão do Airflow."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        row = hook.get_first('SELECT MAX("codmovimentovalidado") from airdata.bimtra')
        last_date = row[0] if row else None

        if last_date is None:
            print("Nenhum dado encontrado — iniciando de 2025")
            initial_date = Variable.get('initial_code_bimtra')
            return initial_date

        return last_date

    @task
    def update_bimtra_data(last_date: str):
        """Atualiza dados do Bimtra a partir da última data registrada, sem SQLAlchemy (robusto)."""
        import requests
        import pandas as pd
        from psycopg2.extras import execute_values
        from psycopg2 import sql

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        base_url = (
            "https://odin-ms.icea.decea.mil.br/api/bimtra"
            "?codmovimentovalidado=gt.{code}&order=codmovimentovalidado.asc&limit=5000&offset={offset}"
        )

        offset = 0
        total = 0

        conn = hook.get_conn()  # conexão psycopg2 do Airflow
        try:
            with conn.cursor() as cur:
                while True:
                    url = base_url.format(code=last_date, offset=offset)
                    print(f"[BIMTRA] Requisitando {url}")
                    r = requests.get(url, timeout=60)

                    if r.status_code != 200:
                        print(f"[BIMTRA] Erro {r.status_code} na requisição. Encerrando.")
                        break

                    data = r.json()
                    if not data:
                        print("[BIMTRA] Nenhum dado retornado. Fim da atualização.")
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
                        table=sql.Identifier("bimtra"),
                        fields=sql.SQL(", ").join(sql.Identifier(c) for c in cols),
                    )

                    # Bulk insert rápido
                    execute_values(cur, insert_stmt.as_string(conn), values, page_size=1000)
                    conn.commit()

                    print(f"[BIMTRA] Inseridos {len(df)} registros (offset={offset}).")

                    total += len(df)
                    offset += 1000

                    if len(data) < 1000:
                        print("[BIMTRA] Última página alcançada.")
                        break
        finally:
            conn.close()

        print(f"[BIMTRA ] Atualização concluída. Total inserido: {total}")

    last_update = get_last_update()
    update_task = update_bimtra_data(last_update)

    create_table >> last_update >> update_task


bimtra_extraction_extraction()