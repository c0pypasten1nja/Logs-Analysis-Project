#!/usr/bin/env python
# Database code for Logs Analysis Project

import psycopg2

# connect to the database
db = psycopg2.connect("dbname=news")
c = db.cursor()

qn1 = "What are the most popular three articles of all time?"
query1 = """select articles.title, count(*) as views
        from articles join log
            on concat('/article/', articles.slug) = log.path
        where log.status = '200 OK'
        group by articles.title
        order by views desc limit 3;"""

print(qn1)
c.execute(query1)
results = c.fetchall()
for title, views in results:
    print "{} -- {} views".format(title, views)
print ""

qn2 = "Who are the most popular article authors of all time?"
query2 = """select authors.name, count(*) as views
            from authors join articles
                on authors.id = articles.author
                join log on concat('/article/', articles.slug) = log.path
            where log.status = '200 OK'
            group by authors.name
            order by views desc;"""

print(qn2)
c.execute(query2)
results = c.fetchall()
for title, views in results:
    print "{} -- {} views".format(title, views)
print ""

qn3 = "On which days did more than 1% of requests lead to errors?"
query3 = """select day, erp from (select date(time) as day,
            round(((count(status) filter (where log.status != '200 OK')*100.00)
            /count(method)),2) as erp from log group by 1)
            as op where erp > 1.0;"""

print(qn3)
c.execute(query3)
results = c.fetchall()
for title, erp in results:
    print "{} -- {} % errors".format(title, erp)
print ""

db.close()
