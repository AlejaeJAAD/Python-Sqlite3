#Create a SQLite database named 'company.db'

#Create a table "esmartdata_user" with the following columns
#id - integer
#first_name - text
#last_name - text
#add NOT NULL to each column, also a primary key with the AUTOINCREMENT option to the id column
import sqlite3

conn = sqlite3.connect('company.db')
cur = conn.cursor()

#######################################################################################################################################
                                                    #ONE TO ONE
#######################################################################################################################################

cur.executescript('''DROP TABLE IF EXISTS "esmartdata_user";
CREATE TABLE IF NOT EXISTS "esmartdata_user" (
  "id" integer NOT NULL,
  "first_name" text NOT NULL,
  "last_name" text NOT NULL,
  PRIMARY KEY("id" AUTOINCREMENT)
);''')

conn.commit()

#esmartdata_developer, before creating this table, you should consider the type of relationship between the tables. In this case (one user -> one developer)
#one-to-one
#user_id - intenger
#level - text
#add NOT NULL to each column, add a primary key constraint to the user_id column
#also add to the user_id column the foreign key constraint referring to the id column of the table "esmartdata_user" with the
#ON DELETE CASCADE ON UPDATE CASCADE
cur.executescript('''DROP TABLE IF EXISTS "esmartdata_developer";
CREATE TABLE IF NOT EXISTS "esmartdata_developer" (
  "user_id" integer NOT NULL,
  "level" text NOT NULL,
  PRIMARY KEY("user_id"),
  FOREIGN KEY("user_id") REFERENCES "esmartdata_user"("id")
  ON DELETE CASCADE ON UPDATE CASCADE
);''')

#######################################################################################################################################
                                                    #MANY TO MANY
#######################################################################################################################################

#Create a table "esmartdata_tech" with the following columns
#id - integer
#name - text
#add NOT NULL constraint to each column. Also add a primary key constraint with the AUTOINCREMENT option to the id column
#Create a table "esmartdata_developer_techs with the following columns
#id - integer
#developer_id - integer
#tech_id - integer
#add NOT NULL constraint to each column. Also add a primary key constraint with the AUTOINCREMENT optionj to the id column
#To the developer_id column, add a foreign key constraint referring to the id column of the "esmartdata_developer" table with
#ON DELETE CASCADE ON UPDATE CASCADE
#To the tech_id column, add a foreign key constraint referring to the id column of the "esmartdata_tech" table with
#ON DELETE CASCADE ON UPDATE CASCADE
cur.executescript('''DROP TABLE IF EXISTS "esmartdata_tech";
CREATE TABLE IF NOT EXISTS "esmartdata_tech" (
  "id" integer NOT NULL,
  "name" text NOT NULL,
  PRIMARY KEY("id" AUTOINCREMENT)
);

DROP TABLE IF EXISTS "esmartdata_developer_techs";
CREATE TABLE IF NOT EXISTS "esmartdata_developer_techs" (
  "id" integer NOT NULL,
  "developer_id" integer NOT NULL,
  "tech_id" integer NOT NULL,
  PRIMARY KEY("id" AUTOINCREMENT),
  FOREIGN KEY("developer_id") 
  REFERENCES "esmartdata_developer"("user_id")
  ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY("tech_id") 
  REFERENCES "esmartdata_tech"("id")
  ON DELETE CASCADE ON UPDATE CASCADE
);''')

#Load the data from insertsTwoFourTables
#esmartdata_user
#esmartdata_tech
#esmartdata_developer
#esmartdada_developer_techs
with open('insertsToFourTables.sql', 'r', encoding='utf-8') as file:
    sql = file.read()

cur.executescript(sql)

#Query that will join the data from the "esmartdata_user" table with the "esmartdata_developer" table LEFT JOIN
#first_name
#last_name
#level
cur.execute('''SELECT
  t1.first_name,
  t1.last_name,
  t2.level
FROM
  "esmartdata_user" AS t1
LEFT JOIN "esmartdata_developer" AS t2 ON t1.id = t2.user_id''')

for row in cur.fetchall():
    print(row)
print("\n")

#Query that will join the data from "esmartdata_user" table with the "esmartdata_developer" INNER JOIN
#first_name
#last_name
#level
cur.execute('''SELECT
  t1.first_name,
  t1.last_name,
  t2.level
FROM
  "esmartdata_user" AS t1
INNER JOIN "esmartdata_developer" AS t2 ON t1.id = t2.user_id''')

for row in cur.fetchall():
    print(row)
print("\n")

#Query that will jion the tables "esmartdata_user", "esmartdata_developer", "esmartdata_developer_techs", "esmartdata-tech" INNER JOIN
#first_name
#last_name
#level
#tech_name (name column from the table "esmartdata_tech")
cur.execute('''SELECT
  t1.first_name,
  t1.last_name,
  t2.level,
  t4.name AS "tech_name"
FROM
  "esmartdata_user" AS t1
INNER JOIN "esmartdata_developer" AS t2 ON t1.id = t2.user_id
INNER JOIN "esmartdata_developer_techs" AS t3 ON t1.id = t3.developer_id
INNER JOIN "esmartdata_tech" AS t4 ON t3.tech_id = t4.id''')

for row in cur.fetchall():
    print(row)
print("\n")

#Extract the information about the number of known technologies for each developer
#Query that will join tables "esmartdata_user", "esmartdata_developer", "esmartdata_developer_techs", "esmartdata_tech" INNER JOIN
#first_name
#last_name
#level
#num_techs - number of technologies for each developer
#sort the output by the num_techs column in descending order and by the first_name column in ascending order
cur.execute('''SELECT
  t1.first_name,
  t1.last_name,
  t2.level,
  COUNT(*) AS "num_techs"
FROM
  "esmartdata_user" AS t1
INNER JOIN "esmartdata_developer" AS t2 ON t1.id = t2.user_id
INNER JOIN "esmartdata_developer_techs" AS t3 ON t1.id = t3.developer_id
INNER JOIN "esmartdata_tech" AS t4 ON t3.tech_id = t4.id
GROUP BY
  t1.id
ORDER BY
  "num_techs" DESC,
  t1.first_name''')

for row in cur.fetchall():
    print(row)
print("\n")

#Extract information from the database about the number of developers broken down by experience
#Query that will join the tables "esmartdata_user" and "esmartdata_developer" INNER JOIN
#level
#num_developer - number of developers for a given level of experience
#sort the output table in ascending order by the level column
cur.execute('''SELECT
  t2.level,
  COUNT(*) AS "num_developers"
FROM
  "esmartdata_user" AS t1
INNER JOIN "esmartdata_developer" AS t2 ON t1.id = t2.user_id
GROUP BY
  t2.level
ORDER BY
  t2.level''')

for row in cur.fetchall():
    print(row)
print("\n")

conn.close()