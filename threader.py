from sys import platform
# import creds
# not really platform specific, but i have a local version on mac and prod in linux so this is an easy way to
# change credentials for a local dev environment
if platform == "linux" or platform == "linux2":
    import getAWSCreds
elif platform == "darwin":
    import creds_info as getAWSCreds
elif platform == "win32":
    import creds_info as getAWSCreds

import threading
import main
from my_utils import get_connection
from datetime import datetime
import time
try:
    from zoneinfo import ZoneInfo
except:
    from backports.zoneinfo import ZoneInfo


WAIT_FOR_THREADS = 60
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

def update_all_pending(connection):
    sql = """update neighborhoods set status = 'pending' where id > 0"""
    cursor = connection.cursor()
    cursor.execute(sql)
    cursor.close()

def threader():
    creds_json = getAWSCreds.secretjson
    cnx = get_connection(creds_json["username"], creds_json["password"], creds_json["host"], creds_json["dbname"])
    print(cnx)
    update_all_pending(cnx)
    while 1:
        if not cnx:
            cnx = get_connection(creds_json["username"], creds_json["password"], creds_json["host"]
                                 , creds_json["dbname"])
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
                    us_central_dt = datetime.now(tz=ZoneInfo("America/Chicago"))
                    print(us_central_dt)
                    print("No neighborhoods to update. Sleeping", WAIT_FOR_NEIGHBORHOODS, "seconds")
                    time.sleep(WAIT_FOR_NEIGHBORHOODS)  #wait an hour to try again
                    update_all_pending(cnx) #in case something went wrong in the middle of one of them it's not stuck processing
            else:
                us_central_dt = datetime.now(tz=ZoneInfo("America/Chicago"))
                print(us_central_dt)
                print("Process status:", process_status)
                print("Number of threads:", threading.active_count())
                print("Sleeping", WAIT_FOR_THREADS, "seconds")
                time.sleep(WAIT_FOR_THREADS)  # wait 5 minutes to try another thread

print("about to start main in the threader")
if __name__ == '__main__':
    print("starting threader")
    threader()
    print("exiting threader")
    exit()