"""
Ultra-fast SQL Server → Snowflake pipeline
- Extraction par chunks
- Directement en Parquet (PyArrow)
- Upload vers Snowflake stage
"""
import os
import dlt
from pathlib import Path
from datetime import datetime
from typing import List
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyodbc
import pyarrow as pa
import pyarrow.parquet as pq
import snowflake.connector
from tqdm import tqdm
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
load_dotenv()


class SQLServerToSnowflakeParquet:
    def __init__(self, max_workers: int = 4):
        # SQL Server
         # Configuration SQL Server
        driver            = dlt.secrets["sources.sql_database.credentials.driver"]
        database          = dlt.secrets["sources.sql_database.credentials.database"]
        password          = dlt.secrets["sources.sql_database.credentials.pwd"]
        username          = dlt.secrets["sources.sql_database.credentials.uid"]
        server            = dlt.secrets["sources.sql_database.credentials.server"]
        port              = dlt.secrets["sources.sql_database.credentials.port"]
        self.sql_driver   = driver #os.getenv("SQL_DRIVER", "{ODBC Driver 18 for SQL Server}")
        self.sql_server   = server #os.getenv("SQL_SERVER")
        self.sql_database = database #os.getenv("SQL_DATABASE")
        self.sql_username = username #os.getenv("SQL_USERNAME")
        self.sql_password = password #os.getenv("SQL_PASSWORD")
        self.sql_port     = port #os.getenv("SQL_PASSWORD")  
        
        
        self.source_table = os.getenv("SOURCE_TABLE", "VLinkLocalisation")
        self.key_column = os.getenv("KEY_COLUMN", "IDVLinkLocalisation")
        self.chunk_size = int(os.getenv("CHUNK_SIZE", 5_000_000))

        # Snowflake
        snowflake_account   = dlt.secrets["destination.snowflake.credentials.account"]
        snowflake_database  = dlt.secrets["destination.snowflake.credentials.database"]
        snowflake_user      = dlt.secrets["destination.snowflake.credentials.username"]
        snowflake_password  = dlt.secrets["destination.snowflake.credentials.password"]
        snowflake_warehouse = dlt.secrets["destination.snowflake.credentials.warehouse"]
        snowflake_role      = dlt.secrets["destination.snowflake.credentials.role"]
        self.sf_account     = snowflake_account #os.getenv("SF_ACCOUNT")
        self.sf_user        = snowflake_user #os.getenv("SF_USER")
        self.sf_password    = snowflake_password #os.getenv("SF_PASSWORD")
        self.sf_warehouse   = snowflake_warehouse #os.getenv("SF_WAREHOUSE", "COMPUTE_WH")
        self.sf_database    = snowflake_database #os.getenv("SF_DATABASE", "equipement")
        self.sf_schema      = "equipement" #os.getenv("SF_SCHEMA", "public")
        self.sf_role        = snowflake_role #os.getenv("SF_ROLE", "ACCOUNTADMIN")        
        self.target_table = os.getenv("TARGET_TABLE", "a_bronze_vlinklocalisation_")
        self.stage_name = os.getenv("STAGE_NAME", "vlinklocalisation_stage")

        # Temp Parquet dir
        self.temp_dir = Path(os.getenv("TEMP_DIR", "./temp_stage"))
        self.temp_dir.mkdir(exist_ok=True, parents=True)

        # Threads
        self.max_workers = max_workers

        # Connexions
        self.sql_conn = None
        self.sf_conn = None

    def connect_sql_server(self):
        self.sql_conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self.sql_driver};DATABASE={self.sql_database};"
            f"UID={self.sql_username};PWD={self.sql_password};Encrypt=yes;TrustServerCertificate=yes"
        )
        logger.info(f"✅ Connecté à SQL Server")

    def connect_snowflake(self):
        self.sf_conn = snowflake.connector.connect(
            account=self.sf_account,
            user=self.sf_user,
            password=self.sf_password,
            warehouse=self.sf_warehouse,
            database=self.sf_database,
            schema=self.sf_schema,
            role=self.sf_role
        )
        logger.info("✅ Connecté à Snowflake")

    def get_id_ranges(self) -> List[tuple]:
        """Déterminer les min/max id pour chunks"""
        cursor = self.sql_conn.cursor()
        cursor.execute(f"SELECT MIN({self.key_column}), MAX({self.key_column}) FROM {self.source_table} (NOLOCK)")
        min_id, max_id = cursor.fetchone()
        ranges = []
        start = min_id
        while start <= max_id:
            end = start + self.chunk_size - 1
            ranges.append((start, min(end, max_id)))
            start = end + 1
        logger.info(f"✅ {len(ranges)} chunks calculés")
        return ranges

    def extract_chunk_to_parquet(self, id_range, idx) -> Path:
        """Extraire un chunk et l’écrire directement en Parquet"""
        start_id, end_id = id_range
        file_path = self.temp_dir / f"chunk_{idx:04d}.parquet"

        query = f"""
            SELECT * FROM {self.source_table} (NOLOCK)
            WHERE {self.key_column} BETWEEN {start_id} AND {end_id}
            ORDER BY {self.key_column}
        """
        cursor = self.sql_conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        if rows:
            table = pa.Table.from_pylist([dict(zip(columns, row)) for row in rows])
            pq.write_table(table, file_path, compression='snappy')

        return file_path

    def extract_to_parquet_parallel(self) -> List[Path]:
        """Extraction multi-threadée SQL Server → Parquet"""
        id_ranges = self.get_id_ranges()
        parquet_files = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.extract_chunk_to_parquet, r, i+1): i for i, r in enumerate(id_ranges)}
            for future in tqdm(as_completed(futures), total=len(futures), desc="Extraction"):
                parquet_files.append(future.result())

        logger.info(f"✅ Extraction terminée: {len(parquet_files)} fichiers Parquet")
        return parquet_files

    def create_stage(self):
        cursor = self.sf_conn.cursor()
        cursor.execute("""
            CREATE OR REPLACE FILE FORMAT parquet_format
            TYPE = PARQUET
            COMPRESSION = SNAPPY
        """)
        cursor.execute(f"CREATE OR REPLACE STAGE {self.stage_name} FILE_FORMAT = parquet_format")
        logger.info(f"✅ Stage {self.stage_name} créé")

    def upload_to_stage(self, files: List[Path]):
        cursor = self.sf_conn.cursor()
        for f in tqdm(files, desc="Upload Parquet"):
            cursor.execute(f"PUT file://{f.absolute()} @{self.stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
        logger.info("✅ Upload terminé")

    def copy_into_snowflake(self):
        cursor = self.sf_conn.cursor()
        cursor.execute(f"""
            COPY INTO {self.target_table}
            FROM @{self.stage_name}
            FILE_FORMAT = parquet_format
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = CONTINUE
            PURGE = TRUE
        """)
        logger.info("✅ COPY INTO terminé")

    def cleanup_temp(self, files: List[Path]):
        for f in files:
            f.unlink()
        logger.info("✅ Fichiers temporaires supprimés")

    def run(self):
        start_time = datetime.now()
        self.connect_sql_server()
        self.connect_snowflake()

        parquet_files = self.extract_to_parquet_parallel()
        self.create_stage()
        self.upload_to_stage(parquet_files)
        self.copy_into_snowflake()
        self.cleanup_temp(parquet_files)

        duration = datetime.now() - start_time
        logger.info(f"✅ Pipeline terminé en {duration}")


if __name__ == "__main__":
    pipeline = SQLServerToSnowflakeParquet(max_workers=8)  # Ajuster selon CPU
    pipeline.run()
