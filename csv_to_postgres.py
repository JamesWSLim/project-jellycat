from sqlalchemy import create_engine
import psycopg2
import pandas as pd

df = pd.read_csv("data/jellycat_with_primary_2024-01-05.csv")
df = df[["jellycatid", "jellycatname", "category", "link", "imagelink", "datecreated"]]

### Connect to your postgres DB
conn = psycopg2.connect(
    host="localhost",
    database="jellycat_db",
    user="root",
    password="root")

### drop jellycat table if exist with cascade
sql = """DROP TABLE IF EXISTS jellycat CASCADE"""
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()

### create jellycat table
sql = """CREATE TABLE jellycat (
    jellycatid uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    jellycatname TEXT,
    category TEXT,
    link TEXT,
    imagelink TEXT,
    datecreated timestamp
);"""
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()

### insert jellycat data to postgresql
engine = create_engine('postgresql://root:root@localhost:5432/jellycat_db')
df.to_sql('jellycat', engine, if_exists='append', index=False)

### read csv
df_sizes = pd.read_csv("data/jellycat_sizes_with_primary_2024-01-05.csv", index_col=False)
df_sizes = df_sizes[["jellycatid", "size", "price", "stock", "datecreated"]]

### drop size table if exist with cascade (dropping all the foreign tables)
sql = """DROP TABLE IF EXISTS size CASCADE"""
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()

### create size table
sql = """CREATE TABLE size (
    jellycatsizeid uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    jellycatid uuid,
    size TEXT,
    price DECIMAL,
    stock TEXT,
    datecreated timestamp,
    CONSTRAINT jellycatsizeidfk FOREIGN KEY(jellycatid) REFERENCES jellycat(jellycatid)
);"""
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()

### insert sizes data into postgresql
df_sizes.to_sql('size', engine, if_exists='append', index=False)