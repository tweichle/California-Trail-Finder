# ### MySQL
#
# #### [MySQL Database (Community Server) Download](https://dev.mysql.com/downloads/mysql/)
#
# Online resources:
# - [MySQL Reference Manual](http://dev.mysql.com/doc/)
# - www.MySQL.com
# - www.Oracle.com
#
# #### [MySQL Connector/Python Download](https://dev.mysql.com/downloads/connector/python/) OR
#
# Install "MySQL Connector" driver
#     - pip install mysql-connector-python
#
# [Online Manual](http://dev.mysql.com/doc/connector-python/en/connector-python.html)
#

# #### Open MySQL using terminal and enter password
# /usr/local/mysql/bin/mysql -u root -p

import mysql.connector
from mysql.connector import errorcode
from secret import mySQL_password # Note: secret.py file

# Create or get a MySQL connection object
cnx = mysql.connector.connect(user='root', password=mySQL_password,
                              host='127.0.0.1')
print(cnx)

# Instantiates and returns a cursor using C Extension
cursor = cnx.cursor()

# Define database name as global variable
DB_NAME = 'trails'

# Create database
def create_database(cursor):
    try:
        cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print('Failed creating database: {}'.format(err))
        exit(1)


try:
    cursor.execute('USE {}'.format(DB_NAME))
except mysql.connector.Error as err:
    print('Database {} does not exist'.format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        print('Database {} created successfully'.format(DB_NAME))
        cnx.database = DB_NAME
    else:
        print(err)
        exit(1)
