from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
	
@dag(dag_id='taticflow_extraction', schedule='0 * * * *', max_active_runs=1)
def taticflow_extraction():
	"""DAG para extração e atualização dos dados da Tatic Flow"""
	
	# Task para criar a tabela VRA se não existir
	create_table = SQLExecuteQueryOperator(
	task_id='create_table',
	conn_id='postgres',
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
		cur.execute("SELECT MAX(createdat) FROM airdata.taticflow;")
		last_date = cur.fetchone()[0]
		cur.close()
		conn.close()

		# Verifica se há dados na tabela
		if last_date is None:
			# Pega a variável global
			initial_date_str = Variable.get('initial_date')

			print(f"Nenhum dado encontrado — iniciando no dia da variavel global ({initial_date_str})")
			return initial_date_str
		# date_str = last_date.strftime("%Y-%m-%d")
		# print(f"Última atualização: {date_str}")
		# return date_str
		# em get_last_update()
		return last_date.isoformat(timespec="milliseconds")



	@task
	def update_taticflow_data(last_date: str):
		"""Atualiza dados do Tatic Flow a partir da última data registrada"""
		from sqlalchemy import create_engine
		import requests
		import pandas as pd
		from datetime import datetime, timedelta
		import configparser

		# Leitura da configuração
		config = configparser.ConfigParser()
		config.read("db.cfg")
		DB_CONFIG = {
			"host": config["database"]["host"],
			"port": config["database"]["port"],
			"dbname": config["database"]["dbname"],
			"user": config["database"]["user"],
			"password": config["database"]["password"],
		}

		# Conexão com o banco
		engine = create_engine(
			f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
			f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
		)

		base_url = "https://odin-ms.icea.decea.mil.br/api/tatic_flow?createdat=gt.{date}&order=createdat.asc&limit=1000&offset={offset}"
		offset = 0
		total = 0

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

			# Converte campos de data/hora
			# for col in df.columns:
			# 	if "at" in col or col in ["eobt", "dep", "arr", "eta"]:
			# 		df[col] = pd.to_datetime(df[col], errors="coerce")

			# Insere no banco
			df.to_sql("taticflow", con=engine, schema="airdata", if_exists="append", index=False)
			print(f"[TATIC_FLOW] Inseridos {len(df)} registros (offset={offset}).")

			total += len(df)
			offset += 1000

			# Para se retornou menos de 1000 registros
			if len(data) < 1000:
				print("[TATIC_FLOW] Última página alcançada.")
				break

		print(f"[TATIC_FLOW] Atualização concluída. Total inserido: {total}")


	# Definição da ordem das tasks
	last_update = get_last_update()
	update_task = update_taticflow_data(last_update)
	
	create_table >> last_update >> update_task

taticflow_extraction()