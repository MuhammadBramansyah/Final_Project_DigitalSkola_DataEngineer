import encodings
import pandas as pd
import os
import connection
import model
import json

from sqlalchemy import create_engine
from hdfs import InsecureClient
from datetime import datetime

if __name__ == "__main__":
    engine = create_engine("postgresql://postgres:postgres:221299@localhost:5432/finalProjectDB")

    conf_postgresql = connection.param_config("postgresql")
    conf_hadoop = connection.param_config("hadoop")["ip"]

    conn = connection.postgres_conn(conf_postgresql)
    cur = conn.cursor()

    client = InsecureClient(conf_hadoop)

    list_tables = model.list_tables()
    for table in list_tables:
        sql = table[1]
        cur.execute(sql)
        data = cur.fetchall()
        
        time = datetime.now().strftime("%Y%m%d")
        df = pd.DataFrame(data, columns=[col[0] for col in cur.description])
        
        with client.write(f"/FinalProject/{time}/{table[0]}_{time}.csv", encoding="utf-8") as writer:
            df.to_csv(writer,index=False)
        
        print(f"done ingestion data {table}")

    