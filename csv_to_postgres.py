from sqlalchemy import create_engine
import pandas as pd
import psycopg2

engine = create_engine('postgresql://root:root@localhost:5432/jellycat_db')

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

df_jellycats = pd.read_csv("./data/jellycat_2024-01-22.csv")
df_jellycats = df_jellycats[['jellycatname','category','link','imagelink','datecreated']]
df_jellycats.to_sql('jellycat', engine, if_exists='append', index=False)

### drop size table if exist with cascade (dropping all the foreign tables)
sql = """DROP TABLE IF EXISTS size CASCADE"""
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()

### create size table
sql = """CREATE TABLE size (
    jellycatsizeid uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    jellycatid uuid,
    jellycatname TEXT,
    size TEXT,
    height DECIMAL,
    width DECIMAL,
    price DECIMAL,
    stock TEXT,
    datecreated timestamp,
    CONSTRAINT jellycatsizeidfk FOREIGN KEY(jellycatid) REFERENCES jellycat(jellycatid)
);"""
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()

df_sizes = pd.read_csv("./data/jellycat_sizes_2024-01-22.csv")
df_sizes = df_sizes[['jellycatid','jellycatname','size','height','width','price','stock','datecreated']]
df_sizes.to_sql('size', engine, if_exists='append', index=False)

### create stock table
sql = """DROP TABLE IF EXISTS stock CASCADE"""
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()

sql = """CREATE TABLE stock (
        jellycatstockid uuid DEFAULT gen_random_uuid() PRIMARY KEY,
        jellycatsizeid uuid,
        stockcount INTEGER,
        CONSTRAINT jellycatstockidfk FOREIGN KEY(jellycatsizeid) REFERENCES size(jellycatsizeid)
    );"""
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
df_stocks = pd.read_csv("./data/jellycat_stock_2024-01-22.csv")
df_stocks[['jellycatsizeid','stockcount']]
df_stocks.to_sql('stock', engine, if_exists='append', index=False)