from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task
from datetime import date, timedelta
from airflow.models import Variable

def make_request(reference_date: date = date.today()):
    """
    Faz requisição para a API de microdados combinados da ANAC

    Args:
        reference_date: Data de referência para a consulta

    Returns:
        DataFrame com os dados ou None em caso de erro
    """
    import requests
    import json
    from pandas import DataFrame

    # Formata a data no padrão ddmmyyyy
    date_str = reference_date.strftime('%d%m%Y')

    url = f'https://sas.anac.gov.br/datavoo_api/combinada?dt_referencia={date_str}'

    print(f'URL utilizado: {url}')
    print(f'Data de referência: {reference_date.strftime("%d/%m/%Y")}')
    print('Fazendo a requisição...')

    response = requests.get(url)

    print(f"Código de resposta: {response.status_code}")

    if response.status_code == 200:
        try:
            # Parse do JSON
            text = response.content.decode('utf-8').strip('\ufeff').strip()
            data = json.loads(text)
            data = json.loads(data)

            if not data or data == 'Nenhum dado foi encontrado.':
                print('Nenhum dado retornado pela API')
                return None

            print(f'{type(data)=}')
            print(f'{data=}'[:100])

            # Converte para DataFrame
            dataframe = DataFrame(data)

            print(f'Total de registros obtidos: {len(dataframe)}')
            return dataframe

        except json.JSONDecodeError as e:
            print(f'Erro ao decodificar JSON: {e}')
            return None
    else:
        print(f'Erro na requisição: {response.status_code}')
        return None


@dag(dag_id='microdados_combinados_extraction', schedule='0 8 * * *', max_active_runs=1)
def microdados_combinados_extraction():
    """
    DAG para extração dos microdados combinados da ANAC
    Executa diariamente às 8h UTC
    """

    # Task para criar a tabela se não existir
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres',
        sql="""
        CREATE TABLE IF NOT EXISTS airdata.microdados_combinados (
            sg_empresa_icao TEXT,              -- código ICAO da empresa
            nm_empresa TEXT,                   -- nome da empresa
            nm_pais_sede TEXT,                 -- país sede da empresa
            nr_voo TEXT,                       -- número do voo
            nr_singular TEXT,                  -- número singular
            cd_di TEXT,                        -- código DI
            ds_grupo_di TEXT,                  -- descrição do grupo DI
            ds_natureza_etapa TEXT,            -- natureza da etapa (nacional/internacional)
            dt_referencia DATE,                -- data de referência
            cd_tipo_linha TEXT,                -- código do tipo de linha
            hr_partida_real TIME,              -- hora de partida real
            dt_partida_real DATE,              -- data de partida real
            sg_icao_origem TEXT,               -- código ICAO do aeroporto de origem
            nm_municipio_origem TEXT,          -- município de origem
            sg_uf_origem TEXT,                 -- UF de origem
            nm_pais_origem TEXT,               -- país de origem
            nr_etapa INTEGER,                  -- número da etapa
            hr_chegada_real TIME,              -- hora de chegada real
            dt_chegada_real DATE,              -- data de chegada real
            sg_icao_destino TEXT,              -- código ICAO do aeroporto de destino
            nm_municipio_destino TEXT,         -- município de destino
            sg_uf_destino TEXT,                -- UF de destino
            nm_pais_destino TEXT,              -- país de destino
            nr_escala_destino INTEGER,         -- número de escalas no destino
            cd_cotran TEXT,                    -- código COTRAN
            nr_passag_pagos INTEGER,           -- número de passageiros pagantes
            nr_passag_gratis INTEGER,          -- número de passageiros gratuitos
            kg_bagagem_livre REAL,             -- bagagem livre em kg
            kg_bagagem_excesso REAL,           -- bagagem em excesso em kg
            kg_carga_paga REAL,                -- carga paga em kg
            kg_carga_gratis REAL,              -- carga gratuita em kg
            kg_correio REAL                    -- correio em kg
        );

        -- Criar índices para melhorar performance de queries
        CREATE INDEX IF NOT EXISTS idx_combinados_dt_referencia 
            ON airdata.microdados_combinados(dt_referencia);
        CREATE INDEX IF NOT EXISTS idx_combinados_empresa 
            ON airdata.microdados_combinados(sg_empresa_icao);
        CREATE INDEX IF NOT EXISTS idx_combinados_origem 
            ON airdata.microdados_combinados(sg_icao_origem);
        CREATE INDEX IF NOT EXISTS idx_combinados_destino 
            ON airdata.microdados_combinados(sg_icao_destino);
        CREATE INDEX IF NOT EXISTS idx_combinados_voo 
            ON airdata.microdados_combinados(sg_empresa_icao, nr_voo, dt_partida_real);
        """
    )

    @task
    def insert_microdados_data(start_date: date = None, end_date: date = None):
        """
        Insere os dados dos microdados combinados em um intervalo de datas.

        Args:
            start_date: Data inicial do intervalo. Se None, usa o dia anterior (D-1)
            end_date: Data final do intervalo. Se None, usa a mesma data que start_date
        """
        from sqlalchemy import create_engine
        import configparser
        from pandas import concat

        # Se não informada, usa o dia anterior
        if start_date is None:
            start_date = date.today() - timedelta(days=1)

        # Se end_date não informada, usa a mesma que start_date
        if end_date is None:
            end_date = start_date

        # Valida que end_date >= start_date
        if end_date < start_date:
            print(f'Erro: Data final ({end_date}) é anterior à data inicial ({start_date})')
            return

        print(f'Período: {start_date.strftime("%d/%m/%Y")} até {end_date.strftime("%d/%m/%Y")}')

        # Calcula número de dias no intervalo
        num_days = (end_date - start_date).days + 1
        print(f'Total de dias a processar: {num_days}')

        # Leitura das configurações do banco de dados
        config = configparser.ConfigParser()
        config.read("/opt/airflow/config/db.cfg")
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

        # Itera sobre cada dia no intervalo
        current_date = start_date
        total_records = 0
        success_count = 0
        failed_dates = []

        while current_date <= end_date:
            print(f'\n{"=" * 60}')
            print(f'Processando data: {current_date.strftime("%d/%m/%Y")} ({success_count + 1}/{num_days})')
            print(f'{"=" * 60}')

            try:
                print('Realizando a requisição')
                data = make_request(reference_date=current_date)

                if data is None or len(data) == 0:
                    print(f'Nenhum dado obtido para {current_date.strftime("%d/%m/%Y")}')
                    failed_dates.append(current_date.strftime("%d/%m/%Y"))
                    current_date += timedelta(days=1)
                    continue

                print(f'Requisição encerrada - {len(data)} registros obtidos')

                # Tratamento de campos de data e hora
                # Converte strings de data no formato DD/MM/YYYY para datetime
                if 'dt_referencia' in data.columns:
                    data['dt_referencia'] = data['dt_referencia'].apply(
                        lambda x: date.fromisoformat(
                            x.split('/')[::-1].__str__().replace("['", "").replace("', '", "-").replace("']",
                                                                                                        "")) if x else None
                    )

                if 'dt_partida_real' in data.columns:
                    data['dt_partida_real'] = data['dt_partida_real'].apply(
                        lambda x: date.fromisoformat(
                            x.split('/')[::-1].__str__().replace("['", "").replace("', '", "-").replace("']",
                                                                                                        "")) if x else None
                    )

                if 'dt_chegada_real' in data.columns:
                    data['dt_chegada_real'] = data['dt_chegada_real'].apply(
                        lambda x: date.fromisoformat(
                            x.split('/')[::-1].__str__().replace("['", "").replace("', '", "-").replace("']",
                                                                                                        "")) if x else None
                    )

                # Converte campos numéricos
                numeric_columns = [
                    'nr_escala_destino', 'nr_passag_pagos', 'nr_passag_gratis',
                    'kg_bagagem_livre', 'kg_bagagem_excesso', 'kg_carga_paga',
                    'kg_carga_gratis', 'kg_correio', 'nr_etapa'
                ]

                for col in numeric_columns:
                    if col in data.columns:
                        data[col] = data[col].replace('', None)
                        data[col] = data[col].apply(lambda x: float(x) if x and str(x) != '0' else None)

                print(f'Inserindo {len(data)} registros na tabela airdata.microdados_combinados')
                data.to_sql(
                    "microdados_combinados",
                    con=engine,
                    schema="airdata",
                    if_exists="append",
                    index=False,
                    chunksize=1000,
                    method="multi"
                )
                print(f'✓ Inserção concluída para {current_date.strftime("%d/%m/%Y")}')

                total_records += len(data)
                success_count += 1

            except Exception as e:
                print(f'✗ Erro ao processar {current_date.strftime("%d/%m/%Y")}: {str(e)}')
                failed_dates.append(current_date.strftime("%d/%m/%Y"))

            # Avança para o próximo dia
            current_date += timedelta(days=1)

        # Resumo final
        print(f'\n{"=" * 60}')
        print('RESUMO DA EXECUÇÃO')
        print(f'{"=" * 60}')
        print(f'Total de dias processados: {success_count}/{num_days}')
        print(f'Total de registros inseridos: {total_records}')

        if failed_dates:
            print(f'\n⚠ Datas com falha ({len(failed_dates)}):')
            for failed_date in failed_dates:
                print(f'  - {failed_date}')
        else:
            print('\n✓ Todas as datas foram processadas com sucesso!')

        print(f'{"=" * 60}\n')

    # Pega a variável global
    initial_date_str = Variable.get('initial-date')
    ano, mes, dia = initial_date_str.split('-')
    start_date = date(day=dia, month=mes, year=ano)
    end_date = date.today()

    # Define a sequência de execução
    # Por padrão, busca dados do dia anterior (D-1)
    # Para carregar um intervalo de datas, especifique start_date e end_date
    insert_data = insert_microdados_data(
        start_date=start_date,
        end_date=end_date
    )

    create_table >> insert_data


# Instancia a DAG
microdados_combinados_extraction()


