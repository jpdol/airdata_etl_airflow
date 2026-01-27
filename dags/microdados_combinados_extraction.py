from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import date, timedelta


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
            # Parse do JSON - REMOVIDO O JSON.DUMPS DUPLICADO
            text = response.content.decode('utf-8').strip('\ufeff').strip()

            # Verifica se a resposta é uma string que precisa ser parseada novamente
            if text.startswith('"') and text.endswith('"'):
                # Pode ser um JSON escapado como string
                text = json.loads(text)

            data = json.loads(text)

            if not data or data == 'Nenhum dado foi encontrado.':
                print('Nenhum dado retornado pela API')
                return None

            print(f'{type(data)=}')
            print(f'Total de registros obtidos: {len(data)}')

            # Converte para DataFrame
            dataframe = DataFrame(data)

            print(f'Total de registros obtidos: {len(dataframe)}')
            return dataframe

        except json.JSONDecodeError as e:
            print(f'Erro ao decodificar JSON: {e}')
            print(f'Texto recebido: {text[:500]}...')  # Mostra parte do texto para debug
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
    
    POSTGRES_CONN_ID = "postgres"  # mesma Connection usada no Airflow

    # Task para criar a tabela se não existir
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id=POSTGRES_CONN_ID,
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
            kg_correio REAL,                   -- correio em kg
            dt_insercao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    def get_last_update() -> str:
        """Obtém a última data registrada na tabela airdata.microdados_combinados usando a conexão do Airflow."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        row = hook.get_first("SELECT MAX(dt_referencia) FROM airdata.microdados_combinados;")
        last_date = row[0] if row else None

        if last_date is None:
            print("Nenhum dado encontrado — iniciando da data inicial definida no Airflow")
            initial_date_str = Variable.get('initial_date')
            return initial_date_str

        return last_date.isoformat()  # Retorna no formato YYYY-MM-DD

    @task
    def update_microdados_data(last_date_str: str):
        """
        Atualiza dados dos microdados combinados a partir da última data registrada, sem SQLAlchemy (robusto).
        """
        import pandas as pd
        from psycopg2.extras import execute_values
        from psycopg2 import sql
        from datetime import datetime, date

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

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Itera sobre cada dia no intervalo
        current_date = start_date
        total_records = 0
        success_count = 0
        failed_dates = []

        while current_date <= end_date:
            print(f'\n{"=" * 60}')
            print(f'Processando data: {current_date.strftime("%d/%m/%Y")} ({success_count + 1}/{(end_date - start_date).days + 1})')
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
                def convert_date(date_str):
                    if pd.isna(date_str) or date_str is None:
                        return None
                    try:
                        # Tenta converter do formato DD/MM/YYYY
                        day, month, year = date_str.split('/')
                        return date(int(year), int(month), int(day))
                    except (ValueError, AttributeError):
                        return None

                if 'dt_referencia' in data.columns:
                    data['dt_referencia'] = data['dt_referencia'].apply(convert_date)

                if 'dt_partida_real' in data.columns:
                    data['dt_partida_real'] = data['dt_partida_real'].apply(convert_date)

                if 'dt_chegada_real' in data.columns:
                    data['dt_chegada_real'] = data['dt_chegada_real'].apply(convert_date)

                # Converte campos numéricos
                numeric_columns = [
                    'nr_escala_destino', 'nr_passag_pagos', 'nr_passag_gratis',
                    'kg_bagagem_livre', 'kg_bagagem_excesso', 'kg_carga_paga',
                    'kg_carga_gratis', 'kg_correio', 'nr_etapa'
                ]

                for col in numeric_columns:
                    if col in data.columns:
                        data[col] = data[col].replace('', None)
                        # Correção: mantém os valores zero e converte apenas strings válidas para números
                        def convert_numeric(value):
                            if pd.isna(value) or value is None:
                                return None
                            try:
                                val_str = str(value).strip()
                                if val_str == '' or val_str.lower() == 'null':
                                    return None
                                return float(val_str)
                            except (ValueError, TypeError):
                                return None

                        data[col] = data[col].apply(convert_numeric)

                # NaN -> None (para virar NULL no Postgres)
                data = data.where(pd.notnull(data), None)

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
                            table=sql.Identifier("microdados_combinados"),
                            fields=sql.SQL(", ").join(sql.Identifier(c) for c in cols),
                        )

                        # Bulk insert rápido
                        execute_values(cur, insert_stmt.as_string(conn), values, page_size=1000)
                        conn.commit()

                        print(f"[MICRODADOS_COMBINADOS] Inseridos {len(data)} registros para {current_date.strftime('%d/%m/%Y')}.")

                finally:
                    conn.close()

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
        print(f'Total de dias processados: {success_count}/{(end_date - start_date).days + 1}')
        print(f'Total de registros inseridos: {total_records}')

        if failed_dates:
            print(f'\n⚠ Datas com falha ({len(failed_dates)}):')
            for failed_date in failed_dates:
                print(f'  - {failed_date}')
        else:
            print('\n✓ Todas as datas foram processadas com sucesso!')

        print(f'{"=" * 60}\n')

    last_update = get_last_update()
    update_task = update_microdados_data(last_update)

    create_table >> last_update >> update_task


# Instancia a DAG
microdados_combinados_extraction()