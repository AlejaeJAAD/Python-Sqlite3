import sqlite3

conn = sqlite3.connect('esmartdata.sqlite3')
cur = conn.cursor()

cur.executescript('''DROP TABLE IF EXISTS "esmartdata_instructor";
CREATE TABLE IF NOT EXISTS "esmartdata_instructor" (
  "id" integer NOT NULL,
  "first_name" text NOT NULL,
  "last_name" text NOT NULL,
  "description" text NOT NULL,
  PRIMARY KEY("id" AUTOINCREMENT)
);''')

cur.executescript('''DROP TABLE IF EXISTS "esmartdata_course";
CREATE TABLE IF NOT EXISTS "esmartdata_course" (
  "id" integer NOT NULL,
  "title" text NOT NULL,
  "subtitle" text NOT NULL,
  "description" text NOT NULL,
  "category" text NOT NULL,
  "subcategory" text NOT NULL,
  "language" text NOT NULL,
  "length" text NOT NULL,
  "rating" real NOT NULL,
  "referral_link" text NOT NULL,
  "instructor_id" integer NOT NULL,
  PRIMARY KEY("id" AUTOINCREMENT),
  FOREIGN KEY("instructor_id") REFERENCES "esmartdata_instructor"("id")
  ON DELETE CASCADE ON UPDATE CASCADE
);''')

cur.execute('''INSERT INTO "esmartdata_instructor"
(
    "id",
    "first_name",
    "last_name",
    "description"
)
VALUES
(
    1,
    "Pawel",
    "Krakowiak",
    "Data Scientist/Python Developer/Securities Broker"
)''')

cur.execute('''INSERT INTO "esmartdata_instructor"
(
    "id",
    "first_name",
    "last_name",
    "description"
)
VALUES
(
    2,
    "takeITeasy",
    "Academy",
    "Akademia Programowania"
)''')

#######################################################################################################################################
                                                    #ONE TO MANY - MANY TO ONE#
#######################################################################################################################################

#Using the script load_esmartdata_course.sql load the data about courses to the table esmartdata_course
with open('load_esmartdata_course.sql', 'r', encoding='utf-8') as file:
    sql = file.read()

cur.executescript(sql)

#Print the numbers of courses stored
cur.execute('''SELECT COUNT(*) FROM "esmartdata_course"''')
n_rows = cur.fetchall()[0][0]
print(n_rows)
print("\n")

conn.commit()

#Create a query that will extract all records from the  esmartdata_instructor
cur.execute('''SELECT * FROM "esmartdata_instructor"''')
for row in cur.fetchall():
    print(row)
print("\n")
"""
cur.execute('''SELECT * FROM "esmartdata_course"''')
for row in cur.fetchall():
    print(row)
"""

#Create index
cur.execute('''DROP INDEX IF EXISTS 
"esmartdata_course_instructor_id_idx"''')
cur.execute('''CREATE INDEX IF NOT EXISTS 
"esmartdata_course_instructor_id_idx" 
ON "esmartdata_course" ("instructor_id")''')

#Query that extracts all the courses names from esmartdata_course
"""
cur.execute('''SELECT "title" FROM "esmartdata_course"''')

for row in cur.fetchall():
    print(row[0])
"""

#Query that extracts all the unique subcategories names
cur.execute('''SELECT DISTINCT "subcategory" FROM "esmartdata_course"''')

subcategories = sorted([row[0] for row in cur.fetchall()])
print(subcategories)
print("\n")

#Query that group the data from the table esmartdata_course
cur.execute('''SELECT
  "instructor_id",
  COUNT(*) AS "num_courses"
FROM
  "esmartdata_course"
GROUP BY
  "instructor_id"''')
for row in cur.fetchall():
    print(row)
print("\n")

#Query that joins esmertdata_course and esmertdata_instructor tables (LEFT JOIN), group the data by the instructor_id column of the esmartdata_course
#and count the number of courses for each instructor
cur.execute('''SELECT
  t1.instructor_id,
  t2.first_name,
  t2.last_name,
  COUNT(*) AS "num_courses"
FROM
  "esmartdata_course" AS t1
  LEFT JOIN "esmartdata_instructor" AS t2 ON t1.instructor_id = t2.id
GROUP BY
  t1.instructor_id''')

for row in cur.fetchall():
    print(row)

#Query that join esmartdata_course and esmartdata_instructor LEFT JOIN, group the data by the instructor_id
#of the esmartdata_course and calculate the average rounded to two decimal for each instructor
cur.execute('''SELECT
  t1.instructor_id,
  t2.first_name,
  t2.last_name,
  ROUND(AVG(rating), 2) AS "avg_rating"
FROM
  "esmartdata_course" AS t1
  LEFT JOIN "esmartdata_instructor" AS t2 ON t1.instructor_id = t2.id
GROUP BY
  t1.instructor_id''')

for row in cur.fetchall():
    print(row)
print("\n")

#Query that join esmartdata_course and esmartdata_instructor LEFT JOIN and retrieve all records that contain the string "Exer" in the course name
cur.execute('''SELECT
  t2.first_name,
  t2.last_name,
  t1.title,
  t1.subcategory
FROM
  "esmartdata_course" AS t1
  LEFT JOIN "esmartdata_instructor" AS t2 ON t1.instructor_id = t2.id
WHERE
  t1.title LIKE "%Exer%"''')

for row in cur.fetchall():
    print(row)
print("\n")

#Query that joins esmartdata_course and esmartdata_instructor LEFT JOIN and retrieve all records that contain in the course name "Python"
#and the lenguaje course is set to "eng"
cur.execute('''SELECT
  t2.first_name,
  t2.last_name,
  t1.title,
  t1.subcategory
FROM
  "esmartdata_course" AS t1
  LEFT JOIN "esmartdata_instructor" AS t2 ON t1.instructor_id = t2.id
WHERE
  t1.title LIKE "%Python%"
  AND t1.language = "eng"''')

for row in cur.fetchall():
    print(row)
print("\n")

#######################################################################################################################################
                                                    #MANY TO MANY#
#######################################################################################################################################
#Add learning path wich consists in different courses
#The table will have this attributes
#id - integer
#title - text
#subtitle - text
#url - text
cur.executescript('''DROP TABLE IF EXISTS "esmartdata_learningpath";
CREATE TABLE IF NOT EXISTS "esmartdata_learningpath" (
  "id" integer NOT NULL,
  "title" text NOT NULL,
  "subtitle" text NOT NULL,
  "url" text NOT NULL,
  PRIMARY KEY("id" AUTOINCREMENT)
);''')

#Create a table with esmartdata_course id and esmartdata_learningpath id
cur.executescript('''DROP TABLE IF EXISTS 
"esmartdata_learningpath_courses";
CREATE TABLE IF NOT EXISTS "esmartdata_learningpath_courses" (
  "id" integer NOT NULL,
  "learningpath_id" integer NOT NULL,
  "course_id" integer NOT NULL,
  PRIMARY KEY("id" AUTOINCREMENT),
  FOREIGN KEY("course_id") 
  REFERENCES "esmartdata_course"("id")
  ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY("learningpath_id") 
  REFERENCES "esmartdata_learningpath"("id")
  ON DELETE CASCADE ON UPDATE CASCADE
);''')

#Load data from esmartdata_learningpath.sql and show the data in console
with open(
        'load_esmartdata_learningpath.sql', 'r', encoding='utf-8'
) as file:
    sql = file.read()

cur.executescript(sql)

cur.execute(
    '''SELECT "id", "title", "url" FROM "esmartdata_learningpath"'''
)
rows = cur.fetchall()
for row in rows:
    print(row)
print("\n")

#Load data from esmartdata_learningpath_courses.sql and show the data in console
with open(
        'load_esmartdata_learningpath_courses.sql',
        'r',
        encoding='utf-8',
) as file:
    sql = file.read()

cur.executescript(sql)

cur.execute(
    '''SELECT * FROM "esmartdata_learningpath_courses"'''
)

#for row in cur.fetchall():
    #print(row)
#print("\n")

#Create index from esmartdata_learningpath_courses_learningpath_id_idx
cur.execute('''DROP INDEX IF EXISTS 
"esmartdata_learningpath_courses_learningpath_id_idx"''')
cur.execute('''CREATE INDEX IF NOT EXISTS 
"esmartdata_learningpath_courses_learningpath_id_idx" 
ON "esmartdata_learningpath_courses" ("learningpath_id")''')
#print("\n")

#Create index from esmartdata_learningpath_courses_course_id_idx
cur.executescript('''DROP INDEX IF EXISTS 
"esmartdata_learningpath_courses_course_id_idx";
CREATE INDEX IF NOT EXISTS 
"esmartdata_learningpath_courses_course_id_idx" 
ON "esmartdata_learningpath_courses" ("course_id");''')
#print("\n")

#Create index for esmartdat_learningpath_courses_learningpath_id_course_id_idx
#learningpath_id
#course_id
cur.execute('''DROP INDEX IF EXISTS 
"esmartdata_learningpath_courses_learningpath_id_course_id_idx"''')
cur.execute('''CREATE UNIQUE INDEX IF NOT EXISTS 
"esmartdata_learningpath_courses_learningpath_id_course_id_idx"
ON "esmartdata_learningpath_courses" ("learningpath_id", "course_id")''')

#Query that will display the names of all indexes from the esmardata_sqlite3 database
cur.execute(
    '''SELECT "name" FROM "sqlite_master" WHERE "type"="index"'''
)

for row in cur.fetchall():
    print(row[0])
print("\n")

#Query that will join the tables esmardata_course and esmardata_instructor LEFT JOIN and extract the top 10 courses (RATING COLUMN) from esmartdata_course
cur.execute('''SELECT
  t1.title,
  t1.rating,
  t2.first_name || ' ' || t2.last_name AS "instructor"
FROM
  "esmartdata_course" AS t1
  LEFT JOIN "esmartdata_instructor" AS t2 ON t1.instructor_id = t2.id
ORDER BY
  t1.rating DESC
LIMIT
  10''')

for row in cur.fetchall():
    print(row)
print("\n")

#Query that extract
#title column from esmartdata_learningpath assign alias "path_title"
#title column from esmartdata_course assign alias "couse_title"
#subcategory column from the esmartdata_course
#sort the output table by the path_title and course_title columns in ascending order. Limit the result to the first 10 records and print to the console
cur.execute('''SELECT
  t2.title AS "path_title",
  t3.title AS "course_title",
  t3.subcategory
FROM
  "esmartdata_learningpath_courses" AS t1
  LEFT JOIN "esmartdata_learningpath" AS t2 ON t1.learningpath_id = t2.id
  LEFT JOIN "esmartdata_course" AS t3 ON t1.course_id = t3.id
ORDER BY
  "path_title",
  "course_title"
LIMIT
  10''')
for row in cur.fetchall():
    print(row)
print("\n")

#Extract all names of the learning paths with the number of courses in each path
#Extract following
#title column from esmartdata_learningpath assign alias "path_title"
#num_courses calculated number of courses for a given path
cur.execute('''SELECT
  t2.title AS "path_title",
  COUNT(*) AS "num_courses"
FROM
  "esmartdata_learningpath_courses" AS t1
  LEFT JOIN "esmartdata_learningpath" AS t2 ON t1.learningpath_id = t2.id
  LEFT JOIN "esmartdata_course" AS t3 ON t1.course_id = t3.id
GROUP BY
  "path_title"
ORDER BY
  "num_courses" DESC''')

for row in cur.fetchall():
    print(row)
print("\n")

#Extract all names of the learning paths with names of the courses appearing in the path and name of the instructor
#title column esmartdata_learningpath table assign alias "path_title"
#title column esmartdata_course table assign alias "course_title"
#instructor - concatenation on the first_name and last_name columns with a space character esmartdata_instructor table
#limit the output for the first 10 records and print to the console
cur.execute('''SELECT
  t2.title as "path_title",
  t3.title as "course_title",
  t4.first_name || ' ' || t4.last_name AS "instructor"
FROM
  "esmartdata_learningpath_courses" AS t1
  LEFT JOIN "esmartdata_learningpath" AS t2 ON t1.learningpath_id = t2.id
  LEFT JOIN "esmartdata_course" AS t3 ON t1.course_id = t3.id
  LEFT JOIN "esmartdata_instructor" AS t4 ON t3.instructor_id = t4.id
LIMIT
  10''')

for row in cur.fetchall():
    print(row)
print("\n")

#Extract information about the number of courses for each instructor in all paths
#Query to get
#title column from esmartdata_learningpath table assign alias "path_title"
#instructor - concatenation of the first name and last name columns with a space character esmartdata_instructor table
#num_courses - number of courses per instructor in the given learning path
#sort the output in ascending order by the path_title and instructor columns and print it to the console
cur.execute('''SELECT
  t2.title as "path_title",
  t4.first_name || ' ' || t4.last_name AS "instructor",
  COUNT(*) AS "num_courses"
FROM
  "esmartdata_learningpath_courses" AS t1
  LEFT JOIN "esmartdata_learningpath" AS t2 ON t1.learningpath_id = t2.id
  LEFT JOIN "esmartdata_course" AS t3 ON t1.course_id = t3.id
  LEFT JOIN "esmartdata_instructor" AS t4 ON t3.instructor_id = t4.id
GROUP BY
  "path_title",
  "instructor"
ORDER BY
  "path_title",
  "instructor"''')

for row in cur.fetchall():
    print(row)
print("\n")

#Extract information about the number of courses at the category, sub-category and instructor levels
#Query to extract
#category column from the esmartdata_course table
#subcategory column from the esmartdata_course table
#instructor - concatenation of the first_name and last_name columns with a space character esmartdata_instructor table
#num_courses - number of courses per category, subcategory and instructor
cur.execute('''SELECT
  t1.category,
  t1.subcategory,
  t2.first_name || ' ' || t2.last_name AS "instructor",
  COUNT(*) AS "num_courses"
FROM
  "esmartdata_course" AS t1
  LEFT JOIN "esmartdata_instructor" AS t2 ON t1.instructor_id = t2.id
GROUP BY
  t1.category,
  t1.subcategory,
  "instructor"
ORDER BY
  "num_courses" DESC''')

for row in cur.fetchall():
    print(row)
print("\n")

#######################################################################################################################################
                                                    #MANY TO MANY - ADDITIONAL COLUMNS
#######################################################################################################################################

#












conn.close()