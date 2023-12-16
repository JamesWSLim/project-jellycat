import psycopg2
import pandas as pd
from sqlalchemy import create_engine

df = pd.read_csv("./data/jellycat.csv", index_col=False)
df["date_created"] = pd.to_datetime(df["date_created"])
engine = create_engine('postgresql://root:root@localhost:5432/jellycat_db')
df.to_sql('jellycat', engine, if_exists='append', index=False)