from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import date, datetime, timedelta


def make_request(
        stations: list = None,
        start_date: date = date(day=1, month=1, year=1990),
        end_date: date = date.today(),
):
    import requests
    from collections import defaultdict
    from pandas import DataFrame

    if not stations:
        stations = get_all_stations()
    stations_request_str = "".join([f"&station={station}" for station in stations])

    start_day = start_date.day
    start_month = start_date.month
    start_year = start_date.year

    end_day = end_date.day
    end_month = end_date.month
    end_year = end_date.year

    print(stations_request_str)
    url = fr'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?network=BR__ASOS{stations_request_str}&data=all&' \
          f'year1={start_year}&month1={start_month}&day1={start_day}&year2={end_year}&month2={end_month}&day2={end_day}' \
          f'&tz=Etc%2FUTC&format=onlycomma&latlon=no&elev=no&missing=null&trace=T&direct=no&report_type=3&report_type=4'

    print(f'URL utilizado: {url}')
    print('Fazendo a requisição...')
    response = requests.get(url)

    print(f"Código de resposta: {response.status_code}")

    if response.status_code == 200:
        conteudo = response.content.decode('utf-8')
        # print(conteudo)
        linhas = conteudo.split('\n')
        cabecalho = linhas[0].split(',')
        # print(f'Quantidade de campos no cabeçalho: {len(cabecalho)}')
        dados = [linha.split(',') for linha in linhas[1:] if linha.strip()]
        # print(f'Quantidade de dados em uma linha: {sum([len(linha) for linha in dados])/len(dados)}')

        df_scheme = defaultdict(list)
        for i, titulo in enumerate(cabecalho):
            # print(f'Analisando o titulo: {titulo}, com i={i}')
            for linha in dados:
                # print(cabecalho)
                if len(linha) != 1:
                    if len(linha) > i:  # Verifica se o índice existe
                        if type(linha[i]) == str:
                            if linha[i] in ['null', '"null"', "'null'", '']:
                                linha[i] = None
                        # print(linha[i])
                        df_scheme[titulo].append(linha[i])
                else:
                    df_scheme[titulo].append(None)
            # print('Titulo analisado!')
        dataframe = DataFrame(df_scheme)
        return dataframe
    else:
        return None
    # TODO decidir se tem a necessidade de criar algum filtro por colunas que são julgadas não necessárias pra esse bd
    # TODO Fazer o link com o Banco de dados
    # TODO Fazer o link com a dag
    # cols_to_eliminate = ['p01m', 'p01i', 'ice_accretion_1hr', 'ice_accretion_3hr', 'ice_accretion_6hr', 'snowdepth', ,]

    # dataframe.to_csv('teste.csv', header=True, index=False, encoding='utf-8', sep=';')  # Salva o df pra testar


def get_all_stations() -> list:
    import json
    raw_json = json.loads(get_html_from_url(
        url='https://mesonet.agron.iastate.edu/geojson/network/BR__ASOS.geojson?only_online=0'
    ))
    # print(raw_json)
    icao_codes = []
    for feature in raw_json['features']:
        # print(f"Feature: {feature['id']}")
        icao_codes.append(feature['id'])
    return icao_codes


def get_html_from_url(url: str, verbose: bool = True) -> str:
    import requests
    response = requests.get(url)
    if verbose:
        print(f"URL utilizado: {url}")
        print(f"Status da requisição pra url: {response.status_code}")
    html_content = response.text
    return html_content


@dag(dag_id='metar_extraction', schedule='0 6 * * *', max_active_runs=1)
def metar_extraction():
    """DAG para extração e atualização dos dados METAR"""
    
    POSTGRES_CONN_ID = "postgres"  # mesma Connection usada no Airflow

    # Task para criar a tabela METAR se não existir
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id=POSTGRES_CONN_ID,
        sql="""
          CREATE TABLE IF NOT EXISTS airdata.metar (
            station TEXT,                        -- código ICAO do aeródromo
            valid TIMESTAMP,                     -- datetime em questão
            tmpf REAL,                           -- temperatura em fahrenheit do ar
            tmpc REAL,                           -- temperatura em celsius do ar
            dwpf REAL,                           -- dew point em fahrenheit
            dwpc REAL,                           -- dew point em celsius
            relh REAL,                           -- umidade relativa (porcentagem)
            feel REAL,                           -- heat index/wind chill em fahrenheit
            drct REAL,                           -- direção do vento em ângulo
            sknt REAL,                           -- velocidade do vento em nós
            sped REAL,                           -- velocidade do vento em milhas por hora
            alti REAL,                           -- altímetro em polegadas
            mslp REAL,                           -- pressão ao nível do mar em mb
            p01m REAL,                           -- precipitações em uma hora (mm)
            p01i REAL,                           -- precipitações em uma hora (polegadas)
            vsby REAL,                           -- visibilidade em milhas
            gust REAL,                           -- rajada de vento em nós
            gustMph REAL,                        -- rajada de vento em milhas por hora
            skyc1 TEXT,                          -- cobertura de nuvem no nível 1
            skyc2 TEXT,                          -- cobertura de nuvem no nível 2
            skyc3 TEXT,                          -- cobertura de nuvem no nível 3
            skyc4 TEXT,                          -- cobertura de nuvem no nível 4
            skyl1 REAL,                          -- altura das nuvens no nível 1 (pés)
            skyl2 REAL,                          -- altura das nuvens no nível 2 (pés)
            skyl3 REAL,                          -- altura das nuvens no nível 3 (pés)
            skyl4 REAL,                          -- altura das nuvens no nível 4 (pés)
            wxCodes TEXT,                        -- código de clima presente
            ice_accretion_1hr TEXT,              -- gelo acumulado em uma hora (float | string)
            ice_accretion_3hr TEXT,              -- gelo acumulado em três horas (float | string)
            ice_accretion_6hr TEXT,              -- gelo acumulado em seis horas (float | string)
            peak_wind_gust REAL,                 -- pico de rajada de vento em nós
            peak_wind_drct REAL,                 -- ângulo da rajada de vento de pico em graus
            peak_wind_time TIMESTAMP,            -- horário do pico de rajada de vento
            snowdepth REAL,                      -- profundidade da neve (não especificado)
            metar TEXT,                          -- mensagem METAR crua
            dt_insercao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    	"""
    )

    @task
    def get_last_update() -> str:
        """Obtém a última data registrada na tabela airdata.metar usando a conexão do Airflow."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        row = hook.get_first("SELECT MAX(valid) FROM airdata.metar;")
        last_date = row[0] if row else None

        if last_date is None:
            print("Nenhum dado encontrado — iniciando da data inicial definida no Airflow")
            initial_date_str = Variable.get('initial_date')
            return initial_date_str

        return last_date.isoformat().split('T')[0]  # Retorna apenas a parte da data

    @task
    def update_metar_data(last_date_str: str):
        """
        Atualiza dados do METAR a partir da última data registrada, sem SQLAlchemy (robusto).
        """
        import pandas as pd
        from psycopg2.extras import execute_values
        from psycopg2 import sql
        from datetime import datetime

        # Converter a string de volta para objeto date
        try:
            # Tenta converter do formato YYYY-MM-DD
            last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
        except ValueError:
            # Se falhar, usa a data inicial
            initial_date_str = Variable.get('initial_date')
            last_date = datetime.strptime(initial_date_str, '%Y-%m-%d').date()

        # Ajustar para começar do dia seguinte
        start_date = last_date + timedelta(days=1)
        end_date = date.today()

        print(f'Data de início: {start_date.strftime("%d/%m/%Y")}')
        print(f'Data de fim: {end_date.strftime("%d/%m/%Y")}')

        # Verificar se há dados para buscar
        if start_date > end_date:
            print("Não há novos dados para buscar - a data inicial é posterior à data final")
            return

        print('Obtendo estações meteorológicas')
        stations = get_all_stations()

        print('Realizando a requisição')
        data = make_request(
            stations=stations,
            start_date=start_date,
            end_date=end_date
        )
        print('Requisição encerrada')

        if data is None:
            print('Dados não obtidos, algum erro na requisição do site.')
            return

        if data.empty:
            print('Nenhum dado novo recebido.')
            return

        # NaN -> None (para virar NULL no Postgres)
        data = data.where(pd.notnull(data), None)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()  # conexão psycopg2 do Airflow
        try:
            with conn.cursor() as cur:
                cols = list(data.columns)
                values = [tuple(row) for row in data.to_numpy()]

                insert_stmt = sql.SQL("""
                    INSERT INTO {schema}.{table} ({fields})
                    VALUES %s
                """).format(
                    schema=sql.Identifier("airdata"),
                    table=sql.Identifier("metar"),
                    fields=sql.SQL(", ").join(sql.Identifier(c) for c in cols),
                )

                # Bulk insert rápido
                execute_values(cur, insert_stmt.as_string(conn), values, page_size=1000)
                conn.commit()

                print(f"[METAR] Inseridos {len(data)} registros.")

        finally:
            conn.close()

        print(f'[METAR] Atualização concluída. Total de registros: {len(data)}')

    last_update = get_last_update()
    update_task = update_metar_data(last_update)

    create_table >> last_update >> update_task


metar_extraction()