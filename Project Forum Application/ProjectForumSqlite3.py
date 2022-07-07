import sqlite3
import pandas as pd

conn = sqlite3.connect('app.db')
cur = conn.cursor()

#Create database names 'app.db'
#Create a table "app_user" with the following columns
#id - integer
#first_name - text
#last_name - text
#age - integer
#country - text
#city - text
#email - text
#add NOT NULL constrain to each column. Add a primary key constraint with the AUTOINCREMENT to the id column
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
);''')

conn.commit()

#Create table "app_thread" with the following columns
#id - integer
#title - text
#creator_id - integer
#add NOT NULL constraint to each column. Also add primary key constraint with the AUTOINCREMENT option to the id column
#To the creator_id column add a foreign key constraint referring to the id column of the "app_user" table with
#ON DELETE CASCADE ON UPDATE CASCADE
cur.executescript('''DROP TABLE IF EXISTS "app_thread";
CREATE TABLE IF NOT EXISTS "app_thread" (
  "id" integer NOT NULL,
  "title" text NOT NULL,
  "creator_id" integer NOT NULL,
  PRIMARY KEY("id" AUTOINCREMENT),
  FOREIGN KEY("creator_id") REFERENCES "app_user"("id")
  ON DELETE CASCADE ON UPDATE CASCADE
);''')

conn.commit()

#Create a table "app_comment" with the following columns
#id - integer
#body - text
#created - text
#thread_id - integer
#user_id - integer
##add NOT NULL constraint to each column. Also add primary key constraint with the AUTOINCREMENT to the id column
#add a foreign key constraint to the thread_id column referring to the id column of the "app_thread" table with
#ON DELETE CASCADE ON UPDATE CASCADE
#add a foreign key constraint to the user_id column relating to the id column of the "app_user" table with the
#ON DELETE CASCADE ON UPDATE CASCADE
cur.executescript('''DROP TABLE IF EXISTS "app_comment";
CREATE TABLE IF NOT EXISTS "app_comment" (
  "id" integer NOT NULL,
  "body" text NOT NULL,
  "created" text NOT NULL,
  "thread_id" integer NOT NULL,
  "user_id" integer NOT NULL,
  PRIMARY KEY("id" AUTOINCREMENT),
  FOREIGN KEY("thread_id") REFERENCES "app_thread"("id")
  ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY("user_id") REFERENCES "app_user"("id")
  ON DELETE CASCADE ON UPDATE CASCADE
);''')

conn.commit()

#Create a table "app_group" with the following columns
#id - integer
#name - text
#add NOT NULL constraint to each column. Also add a primary key constraint with the AUTOINCREMENT option to the id column
cur.executescript('''DROP TABLE IF EXISTS "app_group";
CREATE TABLE IF NOT EXISTS "app_group" (
  "id" integer NOT NULL,
  "name" text NOT NULL,
  PRIMARY KEY("id" AUTOINCREMENT)
);''')

conn.commit()

#Create a table "app_group_user" with the following columns
#id - integer
#group_id - integer
#user_id - integer
#add NOT NULL constraint to each column. Also add a primary key constraint with the AUTOINCREMENT option to the id column
#add a foreign key constraint to the column_group_id referring to the id column of the "app_group" table with
#ON DELETE CASCADE ON UPDATE CASCADE
#add a foreign key constraint to the user_id column referring to the id column of the "app_user" table with
#ON DELETE CASCADE ON UPDATE CASCADE
cur.executescript('''DROP TABLE IF EXISTS "app_group_user";
CREATE TABLE IF NOT EXISTS "app_group_user" (
  "id" integer NOT NULL,
  "group_id" integer NOT NULL,
  "user_id" integer NOT NULL,
  PRIMARY KEY("id" AUTOINCREMENT),
  FOREIGN KEY("group_id") REFERENCES "app_group"("id")
  ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY("user_id") REFERENCES "app_user"("id")
  ON DELETE CASCADE ON UPDATE CASCADE  
);''')

conn.commit()

#Insert records into the "app_user" table
cur.executescript('''INSERT INTO
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
    1,
    "John",
    "Lewis",
    61,
    "Tonga",
    "East Michael",
    "johnsonjack@esmartdata.org"
  );

INSERT INTO
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
    2,
    "Lance",
    "Boyer",
    21,
    "Seychelles",
    "Janicetown",
    "thodges@esmartdata.org"
  );

INSERT INTO
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
    3,
    "Michael",
    "Larson",
    22,
    "Czech Republic",
    "West Melissa",
    "kmalone@esmartdata.org"
  );''')

conn.commit()

cur.execute('''SELECT * FROM "app_user"''')
for row in cur:
    print(row)
print("\n")

#Use the create_scheme.sql script to create schema
with open('create_schema.sql', 'r') as file:
    sql = file.read()

cur.executescript(sql)

#Use the scripts
#create_schema.sql
#load_data.sql
#Create a database schema and load data into tables
with open('load_data.sql', 'r') as file:
    load_data_sql = file.read()

cur.executescript(load_data_sql)

conn.commit()

#Query that will display the number of records in the "app_user" table
num_rows = cur.execute(
    '''SELECT COUNT(*) FROM "app_user"'''
).fetchall()[0][0]
print(num_rows)
print("\n")

#Query that will display the number of records for all tables
user_len = cur.execute(
    '''SELECT COUNT(*) FROM "app_user"'''
).fetchall()[0][0]

thread_len = cur.execute(
    '''SELECT COUNT(*) FROM "app_thread"'''
).fetchall()[0][0]

comment_len = cur.execute(
    '''SELECT COUNT(*) FROM "app_comment"'''
).fetchall()[0][0]

group_len = cur.execute(
    '''SELECT COUNT(*) FROM "app_group"'''
).fetchall()[0][0]

group_user_len = cur.execute(
    '''SELECT COUNT(*) FROM "app_group_user"'''
).fetchall()[0][0]

print(f'user: {user_len}')
print(f'thread: {thread_len}')
print(f'comment: {comment_len}')
print(f'group: {group_len}')
print(f'group_user: {group_user_len}')
print("\n")

conn.commit()

#Query that will display the names (sorted in ascending order) of all tables in the database 'app.db' and print it to the console
cur.execute('''SELECT
  "tbl_name"
FROM
  "sqlite_master"
WHERE
  "type" = "table"
ORDER BY
  "tbl_name"''')

for table in cur.fetchall():
    print(f'table name: {table[0]}')
print("\n")

#Create the following indexes
#"app_thread_creator_id_idx" for the creator_id column of the "app_thread" table
#"app_comment_thread_id_idx" for the thread_id column of the "app_comment" table
#"app_comment_user_id_idx" for the user_id column of the "app_comment" table
#"app_group_user_group_id_idx" for the group_id column of the "app-group_user" table
#"app_group_user_user_id_idx for the user_id column of the "app_group_user" table
cur.executescript('''DROP INDEX IF EXISTS "app_thread_creator_id_idx";
CREATE INDEX "app_thread_creator_id_idx" 
ON "app_thread" ("creator_id");

DROP INDEX IF EXISTS "app_comment_thread_id_idx";
CREATE INDEX IF NOT EXISTS "app_comment_thread_id_idx" 
ON "app_comment" ("thread_id");

DROP INDEX IF EXISTS "app_comment_user_id_idx";
CREATE INDEX IF NOT EXISTS "app_comment_user_id_idx" 
ON "app_comment" ("user_id");

DROP INDEX IF EXISTS "app_group_user_group_id_idx";
CREATE INDEX IF NOT EXISTS "app_group_user_group_id_idx" 
ON "app_group_user" ("group_id");

DROP INDEX IF EXISTS "app_group_user_user_id_idx";
CREATE INDEX IF NOT EXISTS "app_group_user_user_id_idx" 
ON "app_group_user" ("user_id");''')

conn.commit()

#Create a unique index named "app_group_user_group_id_user_id_idx_uniq" consisint of two columns:
#group_id and user_id of the table "app_group_user"
cur.executescript('''DROP INDEX IF EXISTS 
"app_group_user_group_id_user_id_idx_uniq";
CREATE UNIQUE INDEX IF NOT EXISTS 
"app_group_user_group_id_user_id_idx_uniq" 
ON "app_group_user" ("group_id","user_id");''')

conn.commit()

#Query that will extract names (sorted in ascending order) of all indexes in the database and print them to the console
cur.execute('''SELECT
  "type",
  "name"
FROM
  "sqlite_master"
WHERE
  "type" = "index"
ORDER BY
  "name"''')

for index in cur.fetchall():
    print(index)
print("\n")

#Consider the following query
#SELECT * FROM "app_user" WHERE "id" = 3
#Using the appropiate SQL statement, check how the records of the "app_user" table are filtered
cur.execute(
    '''EXPLAIN QUERY PLAN SELECT * FROM "app_user" WHERE "id" = 3'''
)
result = cur.fetchall()[0][-1]
print(result)
print("\n")

#Query that will extract all records from the "app_comment" table for user_id = 3 sorted in ascending order by the created column
cur.execute('''SELECT *
FROM
  "app_comment"
WHERE
  "user_id" = 43
ORDER BY
  "created"''')

for row in cur.fetchall():
    print(row)
print("\n")

#Consider the following query
#SELECT * FROM "app_group_user" WHERE "group_id" = 2 AND "user_id" = 41
#Using the appropiate SQL statement, check how the records of the "app_group_user" table are filtered
cur.execute('''EXPLAIN QUERY PLAN 
SELECT * FROM "app_group_user" 
WHERE "group_id" = 2 AND "user_id" = 41''')

result = cur.fetchall()[0][-1]
print(result)
print("\n")

#Query that will extract all records from the table "app_group_user" for a user with user_id = 41 and group_id = 2
cur.execute('''SELECT * FROM "app_group_user" 
WHERE "user_id" = 41 AND "group_id" = 2''')

result = cur.fetchall()[0]
print(result)
print("\n")

#We want to extract the ten most commenting users from the database. To do this, create a query that will join the appropiate tables and display
#the following columns to the console:
#user_id - column from the "app_comment" table
#first_name - column from the "app_user" table
#last_name - column from the "app_user" table
#email - column from the "app_user" table
#cnt - number of comments for a given user
#sort the output table in descending order by the cnt column and print it to the console
cur.execute('''SELECT
  t1.user_id,
  t2.first_name,
  t2.last_name,
  t2.email,
  COUNT(*) AS "cnt"
FROM
  "app_comment" AS t1
  LEFT JOIN "app_user" AS t2 ON t1.user_id = t2.id
GROUP BY
  t1.user_id
ORDER BY
  "cnt" DESC,
  t2.first_name
LIMIT
  10''')

for row in cur.fetchall():
    print(row)
print("\n")

#From this query
#SELECT
#   t1.user_id,
#   t2.first_name,
#   t2.last_name,
#   t2.email,
#   COUNT(*) as "cnt"
# FROM
#   "app_comment" AS t1
#   LEFT JOIN "app_user" AS t2 ON t1.user_id = t2.id
#GROUP BY
#   "user_id"
#ORDER BY
#   "cnt" DESC,
#   t2.first_name
#LIMIT
#   10
#This query returns information about the top ten commenters on the forum. Create a view named "top_10_users_view" from this query
cur.executescript('''DROP VIEW IF EXISTS "top_10_users_view";
CREATE VIEW "top_10_users_view" AS
SELECT
  t1.user_id,
  t2.first_name,
  t2.last_name,
  t2.email,
  COUNT(*) AS "cnt"
FROM
  "app_comment" AS t1
  LEFT JOIN "app_user" AS t2 ON t1.user_id = t2.id
GROUP BY
  t1.user_id
ORDER BY
  "cnt" DESC,
  t2.first_name  
LIMIT
  10''')

conn.commit()

#Query that will ectract information about all created views in the database 'app.db' and print it to the console
cur.execute(
    'SELECT "type", "name" FROM "sqlite_master" WHERE "type" = "view"'
)
result = cur.fetchall()[0]
print(result)
print("\n")

#Create the appropiate queries that will display:
#column names from the "app_user" table
#column names in the "top_10_users_view" view
#Print the column names as lists to the console
cur.execute('''SELECT * FROM "app_user"''')
app_user_cols = [desc[0] for desc in cur.description]
print(app_user_cols)
print("\n")

cur.execute('''SELECT * FROM "top_10_users_view"''')
result = [desc[0] for desc in cur.description]
print(result)
print("\n")

#Query to the "top_10_users_view" view that will extract information about the three most active forum users
cur.execute('''SELECT
  "first_name",
  "last_name",
  "cnt"
FROM
  "top_10_users_view"
LIMIT
  3''')

for row in cur.fetchall():
    print(row)
print("\n")

#Query to the "app_comment" table that will extract information about comments added between "2021-05-23" and "2021-05-25"
#Extract the following columns from the table
#id
#body
#created
#sort the output table in descending order by the created column, limit the result to the first ten records and print to the console
cur.execute('''SELECT
  "id",
  "body",
  "created"
FROM
  "app_comment"
WHERE
  "created" BETWEEN "2021-05-23" AND "2021-05-25"
ORDER BY
  "created" DESC
LIMIT
  10''')

for row in cur.fetchall():
    print(row)
print("\n")

#The following query
#SELECT
#   "id",
#   "body",
#   "created"
#FROM
#   "app_comment"
#WHERE
#   "created" BETWEEN "2021-05-23" AND "2021-05-25"
#ORDER BY
#   "created" DESC
#LIMIT
#   10
#This query returns ten records from the table "app_comment" using the pandas package, create a DataFrame from these records and assign to the df variable
cur.execute('''SELECT
  "id",
  "body",
  "created"
FROM
  "app_comment"
WHERE
  "created" BETWEEN "2021-05-23" AND "2021-05-25"
ORDER BY
  "created" DESC
LIMIT
  10''')

columns = [desc[0] for desc in cur.description]
df = pd.DataFrame(data=cur.fetchall(), columns=columns)
df = df.set_index('id')
print(df)

#######################################################################################################################################
                                                    #DML - DATA MANIPULATION LANGUAGE
#######################################################################################################################################
#Add a column named is_banned to the "app_user" table with an integer data type and a default value of 0. The is_banned column shows wheter the user is
#banned or not. Since SQLite doesn't have a separate data type for boolean values (TRUE / FALSE) we use a integer
#(0 means false, 1 means true)
#print the first ten records of the "app_user"
cur.execute('''ALTER TABLE "app_user" 
ADD COLUMN "is_banned" INTEGER DEFAULT 0''')
conn.commit()

cur.execute('''SELECT * FROM "app_user" LIMIT 10''')

for row in cur.fetchall():
    print(row)
print("\n")

#Rename the table "app_group_user" to "app_membership"
#Print to the console a list with all table names starting with "app", sorted alphabetically
cur.execute(
    '''DROP TABLE IF EXISTS "app_membership"'''
)
cur.execute(
    '''ALTER TABLE "app_group_user" RENAME TO "app_membership"'''
)
conn.commit()

cur.execute(
    '''SELECT "name" FROM "sqlite_master" WHERE "type" = "table"'''
)
table_names = [
    item[0]
    for item in cur.fetchall()
    if item[0].startswith('app')
]

table_names.sort()
print(table_names)
print("\n")

#In the "app_user" table for the id = 10, update the email address (email column) to "terry@esmartdata.org"
#Print the first fifteen records of the "app_user" table
cur.execute(
    '''UPDATE "app_user" SET "email" = "terry@esmartdata.org" 
WHERE "id" = 10'''
)
conn.commit()

cur.execute('''SELECT * FROM "app_user" LIMIT 15''')

for row in cur.fetchall():
    print(row)
print("\n")

#In the "app_user" table for the id = 5, update the country (country column) to "France" and city (city column) to "Paris"
#Print first ten records of the "app_user"
cur.execute('''UPDATE
  "app_user"
SET
  "country" = "France",
  "city" = "Paris"
WHERE
  "id" = 5''')
conn.commit()

cur.execute('''SELECT * FROM "app_user" LIMIT 10''')

for row in cur.fetchall():
    print(row)
print("\n")

#A column named is_banned was added to the "app_user" table with a integer data type and a default value 0. The is_banned column shows wheter the user is banned or not.
#Block users with id = 8 and id = 36 (set the value in the is_banned column to 1).
#Print all the banned users from the "app_user" table
#cur.execute(
#    '''ALTER TABLE "app_user" ADD COLUMN "is_banned" INTEGER DEFAULT 0'''
#)
cur.execute(
    '''UPDATE "app_user" SET "is_banned" = 1 WHERE "id" IN (8, 36)'''
)
conn.commit()

cur.execute('''SELECT * FROM "app_user" WHERE "is_banned"''')

for row in cur.fetchall():
    print(row)
print("\n")

#A column named is_banned was added to the "app_user" table with an integer data type and a default value 0. The is_banned columns shows
#wheter the user is banned or not.
#Query that will extract all comments from the "app_comment" table for banned users. You can use subqueries.
cur.execute(
    '''SELECT
    *
    FROM
    "app_comment"
    WHERE
    "user_id" IN (
        SELECT
        "id"
        FROM
        "app_user"
        WHERE
        "is_banned"
    )
    ORDER BY
    "created"'''
)

for row in cur.fetchall():
    print(row)
print("\n")

#A column named is_banned was added to the "app_user# table with an integer data type and a default value 0. The is_banned column shows wheter the user is banned ort not.
#Query that will remove all banned users from the database.
cur.execute('''PRAGMA foreign_keys = ON''')
cur.execute('''DELETE FROM "app_user" WHERE "is_banned"''')
conn.commit()

#Query that will extract total number of threads from the table "app_thread"
cur.execute('''SELECT COUNT(*) FROM "app_thread"''')
num_threads = cur.fetchall()[0][0]
print(num_threads)
print("\n")

#Delete all thread from the "app_thread" table for the user with creator_id = 21.
#Query that extracts the number of threads remaining in the "app_thread"
cur.execute('''PRAGMA foreign_keys = ON''')
cur.execute('''DELETE FROM "app_thread" WHERE "creator_id" = 21''')
conn.commit()

cur.execute('''SELECT COUNT(*) FROM "app_thread"''')
num_threads = cur.fetchall()[0][0]
print(num_threads)
print("\n")

#Remove the user from the "app_user" table with the given email address:
#lisa46@esmartdata.org"
#Remember to enable foreign key support (PRAGMA command)
cur.execute('''PRAGMA foreign_keys = ON''')
cur.execute(
    '''DELETE FROM "app_user" WHERE "email" = "lisa46@esmartdata.org"'''
)

conn.commit()

conn.close()
