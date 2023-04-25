import creds
import mysql.connector
from mysql.connector import errorcode


def get_connection(user, password, host, db):
    try:
        cnx = mysql.connector.connect(user=user, password=password,
                                          host=host,
                                          database=db)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        return cnx