#!/usr/bin/env python

"""
logs.py--implementation of the Logs Analysis Project.

The application connects to the News db add print out answers
to the following 3 questions -
    1. What are the most popular three articles of all time?
    2. Who are the most popular article authors of all time?
    3. On which days did more than 1% of requests lead to errors?
"""

import psycopg2


def connect(database_name="news"):
    """
    Function connect() to the PostgreSQL database.
    Returns a database connection and a cursor.
    """
    try:
        db = psycopg2.connect("dbname={}".format(database_name))
        cursor = db.cursor()
        return db, cursor
    except psycopg2.OperationalError as e:
        print('Unable to connect! -- {0}').format(e)


def popular_article():
    """
    Function popular_articles() return a table with the 3 most popular articles
    of all times in descending of the number of views.
    """
    DB, cursor = connect()
    query = '''
                SELECT articles.title,
                Count(log.path) AS popular
                FROM   log,
                       articles
                WHERE  log.path = concat('/article/', articles.slug)
                GROUP  BY articles.title
                ORDER  BY popular DESC
                LIMIT  3
            '''
    cursor.execute(query)
    most = cursor.fetchall()
    DB.close()
    return most


def popular_authors():
    """
    Funtion popoular_authors() return a table with the most viewed articles by
    each author of all times in descending order of the number of the views per
    author.
    """
    DB, cursor = connect()
    query = '''
                SELECT DISTINCT authors.name,
                Count(log.path) AS popular
                FROM   log,
                       authors
                       left join articles
                              ON authors.id = articles.author
                WHERE  log.path = concat('/article/', articles.slug)
                GROUP  BY authors.name
                ORDER  BY popular DESC
            '''
    cursor.execute(query)
    most = cursor.fetchall()
    DB.close()
    return most


def failed_dates():
    """
    function failed_dates() return a table with the days of more than
    1% HTTP errors i.e <> '200 OK', sorted by error rate descending.
    """
    DB, cursor = connect()
    query = '''
                SELECT To_char(Date_trunc('day', TIME), 'MON-DD-YYYY') AS day,
                       To_char(SUM(CASE
                                     WHEN Position('200' IN status) != 1 THEN 1
                                     ELSE 0
                                   END) * 100.00 / Count(status), '90D00')
                                   ||'%'
                       AS failrate
                FROM   log
                GROUP  BY day
                HAVING SUM(CASE
                             WHEN Position('200' IN status) != 1 THEN 1
                             ELSE 0
                           END) * 100.00 / Count(status) > 1.00
                ORDER  BY failrate DESC
            '''
    cursor.execute(query)
    faildates = cursor.fetchall()
    DB.close()
    return faildates


def print_report (table, title, header1, header2):
    '''
    Function print_report() is used to print the answers
    to each of the questions. It gets as input the table each query returns.
    The question and the name of the
    headers to be used in the report. Then it format and print the report.
    '''
    print ""
    print title
    print ""
    for row in table:
        print str(row[0]) + " - " + str(row[1]) + " " + header2
    print ""


# Print 1st queary
print_report(popular_article(),
             "What are the most popular three articles of all time?",
             "Article", "Views")


# print 2nd query
print_report(popular_authors(),
             "Who are the most popular article authors of all time?",
             "Authors", "Views")

# cprint 3rd query
print_report(failed_dates(),
             "On which days more than 1% of requests led to errors?",
             "Date", "Errors")
