from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


def _ts_to_py_or_none(v):
    """Converte Timestamp/NaT/strings para python datetime ou None."""
    import pandas as pd

    if v is None:
        return None
    # pega NaT, nan, etc.
    if pd.isna(v):
        return None
    # pandas Timestamp -> python datetime
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v  # já é datetime python ou string/obj (deixa passar)


def _date_to_py_or_none(v):
    """Converte Timestamp/NaT/strings para python date ou None."""
    import pandas as pd

    if v is None:
        return None
    if pd.isna(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.date()
    # se já vier como date python, ok
    return v


@dag(dag_id="vra_extraction", schedule="0 6 * * *", max_active_runs=1)
def vra_extraction():
    POSTGRES_CONN_ID = "postgres"

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=POSTGRES_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS airdata.vra (
            id SERIAL PRIMARY KEY,
            sg_empresa_icao VARCHAR(10),
            nm_empresa TEXT,
            nr_voo VARCHAR(10),
            cd_di VARCHAR(5),
            cd_tipo_linha VARCHAR(5),
            sg_equipamento_icao VARCHAR(10),
            nr_assentos_ofertados INTEGER,
            sg_icao_origem VARCHAR(10),
            nm_aerodromo_origem TEXT,
            dt_partida_prevista TIMESTAMP,
            dt_partida_real TIMESTAMP,
            sg_icao_destino VARCHAR(10),
            nm_aerodromo_destino TEXT,
            dt_chegada_prevista TIMESTAMP,
            dt_chegada_real TIMESTAMP,
            ds_situacao_voo TEXT,
            ds_justificativa TEXT,
            dt_referencia DATE,
            ds_situacao_partida TEXT,
            ds_situacao_chegada TEXT,
            dt_insercao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    @task
    def get_last_update() -> str:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        row = hook.get_first("SELECT MAX(dt_referencia) FROM airdata.vra;")
        last_date = row[0] if row else None

        if last_date is None:
            print("Nenhum dado encontrado — iniciando de 2025-07-31")
            return initial_date_str = Variable.get('initial_date')


        date_str = last_date.strftime("%Y-%m-%d")
        print(f"Última atualização: {date_str}")
        return date_str

    @task
    def update_vra_data(last_date: str):
        import requests
        import json
        import pandas as pd
        from datetime import datetime, timedelta, date
        from psycopg2.extras import execute_values
        from psycopg2 import sql as psql

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        API_URL = "https://sas.anac.gov.br/sas/vra_api/vra/data?dt_voo={date}"

        start_date = datetime.strptime(last_date, "%Y-%m-%d").date() + timedelta(days=1)
        end_date = date.today()

        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                for single_date in pd.date_range(start=start_date, end=end_date):
                    single_date_str = single_date.strftime("%d%m%Y")

                    print(f"[VRA] Buscando dados para {single_date_str}...")
                    url = API_URL.format(date=single_date_str)
                    print("[VRA] URL: " + url)

                    response = requests.get(url, timeout=120)

                    if response.status_code != 200:
                        print(f"[VRA] Falha ao buscar dados para {single_date_str}: {response.status_code}")
                        continue

                    if response.text == '"Nenhum dado foi encontrado."':
                        print(f"[VRA] Nenhum dado disponível para {single_date_str}.")
                        continue

                    data = json.loads(response.json())
                    if not data:
                        print(f"[VRA] Nenhum dado disponível para {single_date_str}.")
                        continue

                    df = pd.DataFrame(data)
                    if df.empty:
                        print(f"[VRA] DataFrame vazio para {single_date_str}.")
                        continue

                    # ---- PARSING ROBUSTO: evita QUALQUER NaT escapar pro psycopg2 ----
                    # As datas/hora do VRA normalmente vêm como "dd/mm/YYYY HH:MM"
                    datetime_cols = [
                        "dt_partida_prevista",
                        "dt_partida_real",
                        "dt_chegada_prevista",
                        "dt_chegada_real",
                    ]
                    for col in datetime_cols:
                        if col in df.columns:
                            # converte para datetime; inválidos viram NaT
                            df[col] = pd.to_datetime(df[col], format="%d/%m/%Y %H:%M", errors="coerce")
                            # agora converte cada célula para python datetime ou None
                            df[col] = df[col].apply(_ts_to_py_or_none)

                    if "dt_referencia" in df.columns:
                        df["dt_referencia"] = pd.to_datetime(df["dt_referencia"], format="%d/%m/%Y", errors="coerce")
                        df["dt_referencia"] = df["dt_referencia"].apply(_date_to_py_or_none)

                    # garante que o dataframe não volte a "datetime64" e não carregue NaT escondido
                    df = df.astype(object)
                    df = df.replace({pd.NaT: None})

                    # também normaliza NaN -> None
                    df = df.where(pd.notnull(df), None)
                    # ---------------------------------------------------------------

                    cols = list(df.columns)
                    values = [tuple(row) for row in df.to_numpy(dtype=object)]

                    insert_stmt = psql.SQL("""
                        INSERT INTO {schema}.{table} ({fields})
                        VALUES %s
                    """).format(
                        schema=psql.Identifier("airdata"),
                        table=psql.Identifier("vra"),
                        fields=psql.SQL(", ").join(psql.Identifier(c) for c in cols),
                    )

                    execute_values(cur, insert_stmt.as_string(conn), values, page_size=5000)
                    conn.commit()

                    print(f"[VRA] Inseridos {len(df)} registros para {single_date_str}.")

        finally:
            conn.close()

    last_update = get_last_update()
    update_task = update_vra_data(last_update)

    create_table >> last_update >> update_task


vra_extraction()
