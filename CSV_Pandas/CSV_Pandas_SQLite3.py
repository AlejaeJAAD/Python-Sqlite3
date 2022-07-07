#Create a database named 'app.db'
#Create a table "app_user" with the following columns
#id - integer
#first_name - text
#last_name - text
#age - integer
#country - text
#city - text
#email - text
#add NOT NULL constraint to each column. Also add a primary key constraint with the AUTOINCREMENT option to the id column
#the app_user.csv, insert the data from this file into the "app_user" table
import csv
import sqlite3

conn = sqlite3.connect('app.db')
cur = conn.cursor()

cur.executescript('''DROP TABLE IF EXISTS "app_user";
CREATE TABLE IF NOT EXISTS "app_user" (
  "id" integer NOT NULL,
  "first_name" text NOT NULL,
  "last_name" text NOT NULL,
  "age" integer NOT NULL,
  "country" text NOT NULL,
  "city" text NOT NULL,
  "email" text NOT NULL,
  PRIMARY KEY("id" AUTOINCREMENT)
)''')

with open('app_user.csv', 'r') as file:
    reader = csv.DictReader(file)
    records = tuple(reader)

cur.executemany('''INSERT INTO
  "app_user" (
    "id",
    "first_name",
    "last_name",
    "age",
    "country",
    "city",
    "email"
  )
VALUES
  (
    :id,
    :first_name,
    :last_name,
    :age,
    :country,
    :city,
    :email
  );''', records)

conn.commit()

#Query that groups the data by age (age column) and calculates the number of users for a given age.
#Display two columns in the output table
#age
#cnt_users - number of users for a given age
#sort the output table in descending order by cnt_users, then in descending order by age, and leave only those records where the value in the
#cnt_users column is greater than 1 (there are at least two users for a given age) Print the result in the console
cur.execute('''SELECT
  "age",
  COUNT(*) AS "cnt_users"
FROM
  "app_user"
GROUP BY
  "age"
HAVING
  "cnt_users" > 1
ORDER BY
  "cnt_users" DESC,
  "age" DESC''')

rows = cur.fetchall()
for row in rows:
    print(row)
print("\n")

#Query that extracts all records from the "app_user" table for users between 20 and 30 years old (inclusive) sorted by ascending id
#Using the csv module, save the result of this query to a file named 'users_20_to_30.csv'
cur.execute('''PRAGMA table_info("app_user")''')
headers = [item[1] for item in cur.fetchall()]

cur.execute('''SELECT
  *
FROM
  "app_user"
WHERE
  "age" BETWEEN 20 AND 30
ORDER BY
  "id"''')
rows = cur.fetchall()

with open('users_20_to_30.csv', 'w', newline='') as file:
    writer = csv.writer(file, delimiter=',')
    writer.writerow(headers)
    for row in rows:
        writer.writerow(row)

conn.close()