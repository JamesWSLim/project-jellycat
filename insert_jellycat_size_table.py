import psycopg2
import pandas as pd
from sqlalchemy import create_engine

df = pd.read_csv("./data/jellycat_sizes_with_primary.csv", index_col=False)
df = df[["jellycat_id", "size", "price", "stock"]]
df["price"] = df["price"].str.split(' ').str[0].str[1:].astype(float)
engine = create_engine('postgresql://root:root@localhost:5432/jellycat_db')
df.to_sql('size', engine, if_exists='append', index=False)