BEGIN TRANSACTION;

DROP TABLE IF EXISTS "app_user";
CREATE TABLE IF NOT EXISTS "app_user" (
  "id" integer NOT NULL,
  "first_name" text NOT NULL,
  "last_name" text NOT NULL,
  "age" integer NOT NULL,
  "country" text NOT NULL,
  "city" text NOT NULL,
  "email" text NOT NULL,
  PRIMARY KEY("id" AUTOINCREMENT)
);

DROP TABLE IF EXISTS "app_thread";
CREATE TABLE IF NOT EXISTS "app_thread" (
  "id" integer NOT NULL,
  "title" text NOT NULL,
  "creator_id" integer NOT NULL,
  PRIMARY KEY("id" AUTOINCREMENT),
  FOREIGN KEY("creator_id") REFERENCES "app_user"("id")
  ON DELETE CASCADE ON UPDATE CASCADE
);

DROP TABLE IF EXISTS "app_comment";
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
);

DROP TABLE IF EXISTS "app_group";
CREATE TABLE IF NOT EXISTS "app_group" (
  "id" integer NOT NULL,
  "name" text NOT NULL,
  PRIMARY KEY("id" AUTOINCREMENT)
);

DROP TABLE IF EXISTS "app_group_user";
CREATE TABLE IF NOT EXISTS "app_group_user" (
  "id" integer NOT NULL,
  "group_id" integer NOT NULL,
  "user_id" integer NOT NULL,
  PRIMARY KEY("id" AUTOINCREMENT),
  FOREIGN KEY("group_id") REFERENCES "app_group"("id")
  ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY("user_id") REFERENCES "app_user"("id")
  ON DELETE CASCADE ON UPDATE CASCADE  
);

COMMIT;