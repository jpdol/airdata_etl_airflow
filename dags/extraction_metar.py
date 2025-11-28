from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task
from datetime import date


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
        dados = [linha.split(',') for linha in linhas[1:]]
        # print(f'Quantidade de dados em uma linha: {sum([len(linha) for linha in dados])/len(dados)}')

        df_scheme = defaultdict(list)
        for i, titulo in enumerate(cabecalho):
            # print(f'Analisando o titulo: {titulo}, com i={i}')
            for linha in dados:
                # print(cabecalho)
                if len(linha) != 1:
                    if type(linha[i]) == str:
                        if linha[i] in ['null', '"null"', "'null'"]:
                            linha[i] = None
                    # print(linha[i])
                    df_scheme[titulo].append(linha[i])
            # print('Titulo analisado!')
        dataframe = DataFrame(df_scheme)
        return dataframe
    else:
        return None
    # TODO decidir se tem a necessidade de criar algum filtro por colunas que são julgadas não necessárias pra esse bd
    # TODO Fazer o link com o Banco de dados
    # TODO Fazer o link com a dag
    # cols_to_eliminate = ['p01m', 'p01i', 'ice_accretion_1hr', 'ice_accretion_3hr', 'ice_accretion_6hr', 'snowdepth', ]

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
    # Task para criar a tabela VRA se não existir
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres',
        sql="""
          CREATE TABLE IF NOT EXISTS airdata.metar (
            station TEXT,                        -- código ICAO do aeródromo
            valid TIMESTAMP,                      -- datetime em questão
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
            ice_accretion_1hr TEXT,                -- gelo acumulado em uma hora (float | string)
            ice_accretion_3hr TEXT,                -- gelo acumulado em três horas (float | string)
            ice_accretion_6hr TEXT,                -- gelo acumulado em seis horas (float | string)
            peak_wind_gust REAL,                   -- pico de rajada de vento em nós
            peak_wind_drct REAL,                   -- ângulo da rajada de vento de pico em graus
            peak_wind_time TIMESTAMP,              -- horário do pico de rajada de vento
            snowdepth REAL,                      -- profundidade da neve (não especificado)
            metar TEXT                           -- mensagem METAR crua
        );
    	"""
    )

    @task
    def insert_metar_data(start_date: date, end_date: date = date.today(), stations: list[str] | None = None):
        """
        Insere os dados do METAR a partir de uma data inicial, final e as estações.
        Se o campo 'stations' estiver vazio, serão obtidas de todas as estações
        """
        from sqlalchemy import create_engine
        import configparser

        print(f'Data de inicio: {start_date.strftime("%d/%m/%Y")}')
        print(f'Data de fim: {end_date.strftime("%d/%m/%Y")}')

        # Leitura das configurações do banco de dados
        config = configparser.ConfigParser()
        config.read("db.cfg")
        DB_CONFIG = {
            "host": config["database"]["host"],
            "port": config["database"]["port"],
            "dbname": config["database"]["dbname"],
            "user": config["database"]["user"],
            "password": config["database"]["password"],
        }

        # Criação da engine de conexão com o banco de dados
        engine = create_engine(
            f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        )

        if not stations:
            print('Estações não providenciadas. Obtendo todas as estações')
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

        print('Inserindo os dados na tabela airdata.metar')
        data.to_sql(
            "metar",
            con=engine,
            schema="airdata",
            if_exists="append",
            index=False,
            chunksize=5000,
            method="multi"
        )
        print('Inserção de dados finalizada')

    insert_data = insert_metar_data(
        start_date=date(day=1, month=1, year=2025),
        end_date=date(day=31, month=1, year=2025),
    )
    create_table >> insert_data


metar_extraction()
# if __name__ == '__main__':
#     from datetime import date
#
#     start_date = date(day=1, month=1, year=2025)
#     end_date = date(day=31, month=1, year=2025)
#     make_request(
#         stations=['SBGR'],
#         start_date=start_date,
#         end_date=end_date
#     )

'''
station: código ICAO (str)
valid: datetime em questão (datetime)
tmpf: temperatura em fahrenheight do ar (float)
tmpc: temperatura em celcius do ar (float)
dwpf: dew point em farenhieht (float)
dwpc: dew point em celcius (float)
relh: Humidade relativa (porcentagem)
feel: Heat Index/Wind chill em fahrenheit (float)
drct: Direção do vento em angulo(float)
sknt: velocidade do vento em nós (float)
sped: velocidade do vento em milhas por hora (float)
alti: altimetro em polegadas (float)
mslp: pressão em nivel do mar em mb (float)
USA -> p01m: precipitacoes em uma hora, em mm (float)
USA -> p01i: precipitações em uma hora, em polegadas (float)
vsby: Visibilidade em milhas (float)
gust: rajada de vento em nós (float)
gust_mph: rajada de vento em milhas por hora (float)
skyc1: Cobertura de nuvem no nivel 1 (string)
skyc2: Cobertura de nuvem no nivel 2 (string)
skyc3: Cobertura de nuvem no nivel 3 (string)
skyl1: Altura das nuvens no nivel 1 em pés (float)
skyl2: Altura das nuvens no nivel 2 em pés (float)
skyl3: Altura das nuvens no nivel 3 em pés (float)
wxcodes: Código de Clima presente (string)
ice_accretion_1hr: Gelo acumulado em uma hora (float | string)
ice_accretion_3hr: Gelo acumulado em tres horas (float | string)
ice_accretion_6hr: Gelo acumulado em seis horas (float | string)
peak_wind_gust: Pico de rajada de vento em nós (float)
peak_wind_gust_mph: Pico de rajada de vento em milhas por hora (float)
peak_wind_drct: Angulo da rajada de vento de pico em graus (float)
peak_wind_time: Horario de pico de rajada de vento (datetime)
snowdepth: Não sei
metar: mensagem metar cru
'''
