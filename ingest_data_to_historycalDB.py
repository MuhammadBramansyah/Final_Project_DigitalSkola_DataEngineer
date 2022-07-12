import os
import json

import pandas as pd
from sqlalchemy import create_engine

if __name__ == "__main__":
    path = os.getcwd() + "//" + "dataraw" + "//"

    for dic in [("distribution_centers.csv","dim_distribution_centers"),
                ("employees.csv","fact_employees"),
                ("events.csv","fact_events"),
                ("inventory_items.csv","fact_inventory_items"),
                ("order_items.csv","fact_order_items"),
                ("orders.csv","dim_orders"),
                ("products.csv","dim_products"),
                ("users.csv","fact_users")]:

        df = pd.read_csv(path + dic[0])
        engine = create_engine("postgresql://postgres:221299@localhost:5432/HistorycalDB")
        df.to_sql(dic[1], engine, if_exists= "replace", index = False)
