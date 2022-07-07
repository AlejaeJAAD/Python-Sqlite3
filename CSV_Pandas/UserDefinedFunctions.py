#######################################################################################################################################
                                                    #USER DEFINED FUNCTIONS
#######################################################################################################################################
import datetime
import random
import sqlite3

conn = sqlite3.connect('app.db')
cur = conn.cursor()

#Create a SQLite database names 'app.db'
#Then create "instructor" table with the following columns
#id - integer
#first_name - text
#last_name - text
#add NOT NULL constraint to each column. Also add a primary key constraint with the AUTOINCREMENT option to the id column
#Then insert the following records into the "instructor" table
cur.executescript('''DROP TABLE IF EXISTS "instructor";
CREATE TABLE IF NOT EXISTS "instructor" (
  "id" integer NOT NULL,
  "first_name" text NOT NULL,
  "last_name" text NOT NULL,
  PRIMARY KEY("id" AUTOINCREMENT)
);

INSERT INTO
  "instructor" ("id", "first_name", "last_name")
VALUES
  (1, "Mike", "Nagelsman"),
  (2, "John", "Smith"),
  (3, "Sharon", "Johnson"),
  (4, "Paula", "Burke"),
  (5, "William", "Lopez"),
  (6, "James", "Simson"),
  (7, "Mason", "Robinson");''')

conn.commit()

#Assume that the first_name and last_name columns consist of at least 3 characters. We want to assign an email address to each user based on the first
#3 letters of the first name and the first 3 letters of the last name.
#The domain of all email addresses is 'esmartdata.org'
#Usin sqlite3 create a user-defined function that will extract instructor email addresses from two arguments (first_name, last_name) and which can be used
#in SQL statements under the EMAIL() name
#In response, display all data from the "instructor" table, additionally add a column with email addresses extracted by the SQL EMAIL() function
#TIP https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.create_function
def get_email(first_name, last_name):
    return (first_name[:3] + last_name[:3]).lower() + '@esmartdata.org'


conn.create_function('email', 2, get_email)
cur.execute(
    '''SELECT *, EMAIL("first_name", "last_name") FROM "instructor"'''
)

for row in cur.fetchall():
    print(row)
print("\n")

#We need to find out the age of the instructor based on the date of birth (birth_date column)
#Using the sqlite3 package and the datetime module, create a user_defined function that will calculate the age of the instructors based on one argument:
#birth_date, and which can be used in SQL statements under the AGE() name
#Disoplay all the data from the "instructor" table additionally displaying a column with the age determined by the AGE() function
#TIP https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.create_function
cur.executescript('''DROP TABLE IF EXISTS "instructor";
CREATE TABLE IF NOT EXISTS "instructor" (
  "id" integer NOT NULL,
  "first_name" text NOT NULL,
  "last_name" text NOT NULL,
  "birth_date" text NOT NULL,
  PRIMARY KEY("id" AUTOINCREMENT)
);

INSERT INTO
  "instructor" ("id", "first_name", "last_name", "birth_date")
VALUES
  (1, "Mike", "Nagelsman", "1990-03-15"),
  (2, "John", "Smith", "2005-06-21"),
  (3, "Sharon", "Johnson", "1999-03-10"),
  (4, "Paula", "Burke", "1999-04-12"),
  (5, "William", "Lopez", "1988-10-17"),
  (6, "James", "Simson", "1988-09-21"),
  (7, "Mason", "Robinson", "1978-11-24");''')
#We have added birth_column

conn.commit()

def get_age(birth_date):
    return datetime.date.today().year - int(birth_date.split('-')[0])

conn.create_function('age', 1, get_age)
cur.execute('''SELECT *, AGE("birth_date") FROM "instructor"''')

for row in cur.fetchall():
    print(row)
print("\n")

#We need to assign each instructor pseudo-randomly to one of the three groups. We will do this by assigining the appropiate group member of each instructor(1,2,3)
#Using the sqlite3 package and the random module, create a user-defined RANDOM_GROUP() function that will assign a group number to the instructor
#In response display all data from the "instructor" table, additionally displaying a column with pseudo-randomly generated group members designated by the
#SQL function RANDOM_GROUP()
#TIP https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.create_function
random.seed(10)


def get_random_group():
    return random.randint(1, 3)


conn.create_function('random_group', 0, get_random_group)
cur.execute('''SELECT *, RANDOM_GROUP() FROM "instructor"''')

for row in cur.fetchall():
    print(row)
print("\n")

#We need to assign each instructor pseudo-randomly to one of the three groups. We will do this by assigning the appropiate group member to the instructor (1,2,3)
#Using the sqlite3 package and the random module, create a user-defined function that will assign a group member to the instructor and that can be
#used in SQL statements under the name RANDOM_GROUP(). If we do not pass any argument to the RANDOM_GROUP() function, by default the function is to generate
#three random groups. Also add an option to pass an argument to the RANDOM-GROUP() function, which will allow you to set the number of groups
#(we assume that the value is greater than 1)
#In response display all data from the "instructor" table, additionally displaying a column with pseudo-randomly generated group numbers designated
#by the SQL function RANDOM_GROUP() without any argument and with an argument = 5
#TIP https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.create_function
def get_random_group_with_size(size):
    return random.randint(1, size)


conn.create_function('random_group', 1, get_random_group_with_size)
cur.execute(
    '''SELECT *, RANDOM_GROUP(), RANDOM_GROUP(5) FROM "instructor"'''
)

for row in cur.fetchall():
    print(row)
print("\n")

#Using the sqlite package create a user-defined function that will represent the data in CSV format and that can be used in SQL statements
#under the CSV() name. The CSV() function is expected to take any number of arguments.
#In response, display the columns: id, first_name, last_name from the table "instructor" in CSV format using the implemented SQL function called CSV()
#TIP https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.create_function
def get_csv(*args):
    return ','.join([str(arg) for arg in args])


conn.create_function('csv', -1, get_csv)
cur.execute(
    '''SELECT CSV("id", "first_name", "last_name") FROM "instructor"'''
)

for row in cur.fetchall():
    print(row[0])
print("\n")

conn.close()