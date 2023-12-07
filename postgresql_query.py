import psycopg2

conn_string = 'postgresql://root:root@localhost:5432/jellycat_db'
conn = psycopg2.connect(conn_string)
conn.autocommit = True
cursor = conn.cursor
sql1 = '''select * from jellycat_data;'''
cursor.execute(sql1)
for i in cursor.fetchall():
    print(i)

conn.close()