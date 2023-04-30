import creds
import threading
import main
from my_utils import get_connection
import time


def get_pending_neighborhood(connection):
    sql = """SELECT id
    FROM neighborhoods
    where last_updated < DATE_ADD(date(now()), INTERVAL -7 DAY) and status = 'pending'
            limit 1"""
    cursor = connection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows

def set_processing(connection, id):
    sql = """update neighborhoods set status = 'processing' where id = %s and status = 'pending'""" % id
    cursor = connection.cursor()
    cursor.execute(sql)
    count = cursor.rowcount
    cursor.close()
    return count


def threader():
    cnx = get_connection(creds.user, creds.password, creds.host, creds.database)
    while 1:
        if threading.active_count() < 3:
            rows = get_pending_neighborhood(cnx)
            if len(rows) == 1:
                id = rows[0][0]
                count = set_processing(cnx, id)
                if count == 1:
                    cnx.commit()
                    t = threading.Thread(target=main.main, args=[id])
                    t.start()
            elif len(rows) > 1:
                print("Bad query")
                quit()
            else:
                cnx.close()
                time.sleep(3600)  #wait an hour to try again
                cnx = get_connection(creds.user, creds.password, creds.host, creds.database)
        else:
            time.sleep(300)  # wait 5 minutes to try another thread

threader()
quit()