import psycopg2

# Establishing the connection
conn = psycopg2.connect(
    database="postgres", user='postgres', password='admin', host='127.0.0.1', port= '5432'
)
conn.autocommit = True

# Creating a cursor object using the cursor() method
cursor = conn.cursor()

cursor.execute(open("naukri.sql", "r").read())

# Closing the connection
conn.close()