from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable

def parse_datetime(x):
	"""Realiza o parsing de uma string para datetime no formato dd/mm/YYYY HH:MM"""
	import pandas as pd

	return pd.to_datetime(x, format="%d/%m/%Y %H:%M", errors="coerce")

def parse_date(x):
	"""Realiza o parsing de uma string para date no formato dd/mm/YYYY"""
	import pandas as pd

	return pd.to_datetime(x, format="%d/%m/%Y", errors="coerce")
	
@dag(dag_id='vra_extraction', schedule='0 6 * * *', max_active_runs=1)
def vra_extraction():
	"""DAG para extração e atualização dos dados da VRA (Voo Regular Ativo - ANAC)"""
	
	# Task para criar a tabela VRA se não existir
	create_table = SQLExecuteQueryOperator(
	task_id='create_table',
	conn_id='postgres',
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
	"""
)
	
	@task
	def get_last_update() -> str:
		"""Task para obter a última data registrada na tabela VRA."""
		import psycopg2
		import configparser

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
		# Conexão ao banco de dados
		conn = psycopg2.connect(**DB_CONFIG)
		cur = conn.cursor()
		# Consulta para obter a última data de referência
		cur.execute("SELECT MAX(dt_referencia) FROM airdata.vra;")
		last_date = cur.fetchone()[0]
		cur.close()
		conn.close()

		# Verifica se há dados na tabela
		if last_date is None:
			# Pega a variável global
			initial_date_str = Variable.get('initial-date')
			print(f"Nenhum dado encontrado — iniciando na data da variavel global ({initial_date_str})")
			return initial_date_str
		date_str = last_date.strftime("%Y-%m-%d")
		print(f"Última atualização: {date_str}")
		return date_str

	@task
	def update_vra_data(last_date: str):
		"""Task para atualizar os dados da VRA a partir da data seguinte à última data registrada"""
		from sqlalchemy import create_engine
		import requests
		import json
		from datetime import datetime, timedelta, date
		import pandas as pd		
		import configparser

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

		# URL base da API do VRA
		API_URL = "https://sas.anac.gov.br/sas/vra_api/vra/data?dt_voo={date}"

		# Criação da engine de conexão com o banco de dados
		engine = create_engine(
					f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
					f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
				)

		# Define os limites de data para extração
		start_date = datetime.strptime(last_date, "%Y-%m-%d").date() + timedelta(days=1) # Data seguinte à última data registrada
		end_date = date.today() # Data atual

		# Loop para buscar e inserir dados para cada data no intervalo
		for single_date in pd.date_range(start=start_date, end=end_date):
			single_date_str = single_date.strftime("%d%m%Y")

			print(f"Buscando dados para {single_date_str}...")
			print("URL: " + API_URL.format(date=single_date_str))
			response = requests.get(API_URL.format(date=single_date_str)) # Requisição à API
					
			if response.status_code == 200: # Verifica se a requisição foi bem-sucedida
				if response.text == '"Nenhum dado foi encontrado."': # Verifica se não há dados para a data
					print(f"Nenhum dado disponível para {single_date_str}.")
				else: # Processa os dados retornados
					data = json.loads(response.json()) # Parsing do JSON retornado
					if data:
						df = pd.DataFrame(data) # Conversão dos dados para DataFrame
						# Parsing das colunas de data e hora
						for col in ["dt_partida_prevista", "dt_partida_real", "dt_chegada_prevista", "dt_chegada_real"]:
							if col in df.columns:
								df[col] = df[col].apply(parse_datetime)
						# Parsing da coluna de data
						df["dt_referencia"] = df["dt_referencia"].apply(parse_date)
						
						# Inserção dos dados no banco de dados
						df.to_sql(
							"vra",
							con=engine,
							schema="airdata",
							if_exists="append",
							index=False,
							chunksize=5000,
							method="multi"
						)
					else:
						print(f"Nenhum dado disponível para {single_date_str}.")
			else:
				print(f"Falha ao buscar dados para {single_date_str}: {response.status_code}")

	# Definição da ordem das tasks
	last_update = get_last_update()
	update_task = update_vra_data(last_update)
	
	create_table >> last_update >> update_task

vra_extraction()