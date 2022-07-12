import os 
import json 

import pandas as pd 
from sqlalchemy import create_engine

import model
import connection

if __name__ =="__main__":
    engine = create_engine("postgresql://postgres:221299@localhost:5432/finalProjectDataMart")

    conf_postgresql = connection.param_config("datawarehouse")
    conn = connection.postgres_conn(conf_postgresql)
    curr = conn.cursor()

    sql = model.datamart_table()
    
    curr.execute(sql)
    data = curr.fetchall()
    df = pd.DataFrame(data,columns=[col[0] for col in curr.description])
    
    df.to_csv("report_test.csv")



