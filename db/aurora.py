import pandas as pd
import psycopg2
import io

class AuroraExecutor:
    def execute(self,sql):
        with psycopg2.connect(
        dbname='matchlibrary',
        user='postgres',
        password='Bungee_2020!',
        host='match-library.cluster-cf6pgrepnkpy.us-east-1.rds.amazonaws.com',
        port=5432
    ) as conn:
            data = io.StringIO()
            with conn.cursor() as cursor:
                cursor.copy_expert("COPY ({}) TO STDOUT WITH CSV HEADER".format(sql), data)
        data.seek(0)
        df = pd.read_csv(data)
        return df