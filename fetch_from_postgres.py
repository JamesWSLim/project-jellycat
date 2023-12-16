import psycopg2
import pandas as pd

# Connect to your postgres DB
conn = psycopg2.connect(
    host="localhost",
    database="jellycat_db",
    user="root",
    password="root")

# Retrieve query results
sql = 'select jellycat_id, jellycat_name, information, link, max(date_created) as "most_recent_date" from jellycat group by jellycat_id, jellycat_name'
df = pd.read_sql_query(sql, conn)
print(df)
df.to_csv("jellycat_with_primary.csv")