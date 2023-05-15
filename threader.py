import creds
import threading
import main
from my_utils import get_connection
from datetime import datetime
import time
from zoneinfo import ZoneInfo
from plot_addresses import core


WAIT_FOR_THREADS = 300
WAIT_FOR_NEIGHBORHOODS = 3600


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


def get_process_status(connection):
    sql = """select status from process_list where name = 'Padctn Scraper'"""
    cursor = connection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows[0][0]


def threader():
    cnx = get_connection(creds.aws_user, creds.aws_pass, creds.aws_host, creds.aws_database)
    print(cnx)
    while 1:
        if not cnx:
            cnx = get_connection(creds.aws_user, creds.aws_pass, creds.aws_host, creds.aws_database)
        if not cnx.is_connected():
            cnx.reconnect()
        process_status = get_process_status(cnx)
        if process_status.lower() == 'paused':
            print("Processed paused")
            us_central_dt = datetime.now(tz=ZoneInfo("America/Chicago"))
            print(us_central_dt)
            print("Process status:", process_status)
            print("Number of threads:", threading.active_count())
            print("Sleeping", WAIT_FOR_THREADS, "seconds")
            time.sleep(WAIT_FOR_THREADS)  # wait 5 minutes to try another thread
        else:
            if threading.active_count() < 3:
                rows = get_pending_neighborhood(cnx)
                if len(rows) == 1:
                    id = rows[0][0]
                    count = set_processing(cnx, id)
                    if count == 1:
                        cnx.commit()
                        us_central_dt = datetime.now(tz=ZoneInfo("America/Chicago"))
                        print(us_central_dt)
                        print("Processing neighborhood:", id)
                        t = threading.Thread(target=main.main, args=[id])
                        t.start()
                elif len(rows) > 1:
                    print("Bad query")
                    quit()
                else:
                    cnx.close()
                    us_central_dt = datetime.now(tz=ZoneInfo("America/Chicago"))
                    print(us_central_dt)
                    print("No neighborhoods to update. Sleeping", WAIT_FOR_NEIGHBORHOODS, "seconds")
                    time.sleep(WAIT_FOR_NEIGHBORHOODS)  #wait an hour to try again
                    cnx = get_connection(creds.aws_user, creds.aws_pass, creds.aws_host, creds.aws_database)
            else:
                print("Creating updated map")
                core.main()
                us_central_dt = datetime.now(tz=ZoneInfo("America/Chicago"))
                print(us_central_dt)
                print("Process status:", process_status)
                print("Number of threads:", threading.active_count())
                print("Sleeping", WAIT_FOR_THREADS, "seconds")
                # 5 minutes might be too much but who cares?
                time.sleep(WAIT_FOR_THREADS)  # wait 5 minutes to try another thread


threader()
quit()