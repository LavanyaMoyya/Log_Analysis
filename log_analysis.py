#!/usr/bin/env python3

import psycopg2
database_name = "news"


def three_popular_articles(query_1):
    """What are the most popular three articles of all time?"""
query_1 = """
    SELECT articles.title, COUNT(*) AS count
    FROM articles
    JOIN log
    ON log.path = concat('/article/%', articles.slug)
    GROUP BY articles.title
    ORDER BY count DESC
    LIMIT 3;
"""

db = psycopg2.connect('dbname=' + database_name)
c = db.cursor()
c.execute(query_1)
results = c.fetchall()
""" Print the Results"""
print('\n The Results are:')
print("\n1. What are the most popular three articles of all time?")
print("\n************************************")
print(' TOP THREE ARTICLES BY PAGE VIEWS :')
print("************************************")
count = 1
for i in results:
    number = '(' + str(count) + ') "'
    title = i[0]
    views = '" ==>' + str(i[1]) + " views"
    print(number + title + views)
    count += 1
    db.close()


def most_popular_authors(query_2):
    """ Who are the most popular article authors of all time?"""
query_2 = """
        SELECT authors.name, COUNT(*) AS count
        FROM authors
        JOIN articles
        ON authors.id = articles.author
        JOIN log
        ON log.path = concat('/article/%', articles.slug)
        GROUP BY authors.name
        ORDER BY count DESC
        LIMIT 3;
    """

"""Connect to the Database and Executing the query"""
db = psycopg2.connect('dbname=' + database_name)
c = db.cursor()
c.execute(query_2)
results = c.fetchall()
""" Print the Results"""
print("\n2. Who are the most popular article authors of all time?")
print("\n************************************")
print(' TOP THREE AUTHORS BY VIEWS :')
print("************************************")
count = 1
for i in results:
    number = '(' + str(count) + ') "'
    title = str(i[0])
    views = '" ==>' + str(i[1]) + " views"
    print(number + title + views)
    count += 1
    db.close()


def get_error_percentage(query_3):
    """ On which days did more than 1% of requests lead to errors"""
query_3 = """
        SELECT total.day,
          ROUND(((errors.error_requests*1.0) / total.requests), 3) AS Percent
        FROM (
          SELECT date_trunc('day', time) "day", count(*) AS Error_Requests
          FROM log
          WHERE status LIKE '404%'   
          GROUP BY day
        ) AS errors
        JOIN (
          SELECT date_trunc('day', time) "day", count(*) AS Requests
          FROM log
          GROUP BY day
          ) AS total
        ON total.day = errors.day
        WHERE (ROUND(((errors.Error_Requests*1.0) / total.Requests), 3) > 0.01)
        ORDER BY Percent DESC;
    """
"""Connect to the Database and Executing the query"""
db = psycopg2.connect('dbname=' + database_name)
c = db.cursor()
c.execute(query_3)
results = c.fetchall()
""" Print the Results"""
print("\n3. On which days did more than 1% of requests lead to errors")
print("\n************************************")
print(' DAYS WITH MORE THAN 1% ERRORS:')
print("************************************")
count = 1
for i in results:
    date = i[0].strftime('%B, %d, %Y')
    per = str(round(i[1]*100, 1))
    print(date + " ==> " + per + "%" + " errors")
    count += 1
    db.close()

if __name__ == '__main__':
    three_popular_articles = three_popular_articles(query_1)
    most_popular_authors = most_popular_authors(query_2)
    get_error_percentage = get_error_percentage(query_3)
