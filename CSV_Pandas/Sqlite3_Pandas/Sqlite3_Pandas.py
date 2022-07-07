import sqlite3
import pandas as pd

conn = sqlite3.connect('app.db')
cur = conn.cursor()

#SQLite database called 'app.db' was prepared
#Query that will retrieve the first ten records from the "app_user" table.
#Present the result of this query as a DataFrame from the pandas package and assign to the df variable
#Set the id column as the index of the DataFrame
with open('create_database.sql', 'r', encoding='utf-8') as file:
    sql = file.read()

cur.executescript(sql)

df = pd.read_sql(
    '''SELECT * FROM "app_user" LIMIT 10''', conn, index_col='id'
)

#A query was created that extracts the firsts ten records of the "app_user" table and transforms into a DataFrame (df variable)
#Export this DataFrame to a CSV file named 'app_user.csv'
df.to_csv('app_user.csv')

#A query was created that extracts the first ten records of the "app_user" table and transforms into a DataFrame (df variable)
#Export this DataFrame to an HTML file named 'app_user.html'
df.to_html('app_user.html')

#A query was created that extracts the first ten records of the "app_user" table and transforms into a DataFrame (df variable)
#Export this DataFrame to a JSON file named 'app_user.json' (set indent level = 4)
df.to_json('app_user.json', indent=4)

conn.close()

#Using pandas package, load the file 'comment_archive.csv' into the DataFrame object. Then using this object, create a table named "app_comment_archive"
#in the database 'app_archive.db'
#Query and extrack all records from the "app_comment_archive" table and print to the console
conn = sqlite3.connect('app_archive.db')
cur = conn.cursor()

df = pd.read_csv('comment_archive.csv')
df.to_sql(
    'app_comment_archive',
    con=conn,
    index=False,
    if_exists='replace',
)

cur.execute('''SELECT * FROM "app_comment_archive"''')

for row in cur.fetchall():
    print(row)
print("\n")

#Using the pandas package, load this file into the DataFrame object. Then using the object create a table names "test_app_user" in the database 'app.db'
#Create a query which extracts all records from the "test_app_user" table and print to the console
df = pd.read_json('app_user.json')
df.to_sql('test_app_user', con=conn, index=False, if_exists='replace')

cur.execute('''SELECT * FROM "test_app_user"''')

for row in cur.fetchall():
    print(row)
print("\n")

conn.close()