#!/usr/bin/env python

import psycopg2

# connect to the database
db = psycopg2.connect("dbname=news")
c = db.cursor()

qn1 = "What are the most popular three articles of all time?"
query1 = """SELECT articles.title, count(*) AS views
        FROM articles JOIN log
            ON concat('/article/', articles.slug) = log.path
        WHERE log.status = '200 OK'
        GROUP BY articles.title
        ORDER BY views DESC LIMIT 3;"""

print(qn1)
c.execute(query1)
results = c.fetchall()
for title, views in results:
    print("{} -- {} views").format(title, views)
print("")

qn2 = "Who are the most popular article authors of all time?"
query2 = """SELECT authors.name, count(*) AS views
            FROM authors JOIN articles
                ON authors.id = articles.author
                JOIN log ON concat('/article/', articles.slug) = log.path
            WHERE log.status = '200 OK'
            GROUP BY authors.name
            ORDER BY views DESC;"""

print(qn2)
c.execute(query2)
results = c.fetchall()
for title, views in results:
    print("{} -- {} views").format(title, views)
print("")

qn3 = "On which days did more than 1% of requests lead to errors?"
query3 = """SELECT day, erp FROM (SELECT date(time) AS day,
            round(((count(status) filter (WHERE log.status != '200 OK')*100.00)
            /count(method)),2) AS erp FROM log GROUP BY 1)
            AS op WHERE erp > 1.0;"""

print(qn3)
c.execute(query3)
results = c.fetchall()
for title, erp in results:
    print("{} -- {} % errors").format(title, erp)
print("")

db.close()
