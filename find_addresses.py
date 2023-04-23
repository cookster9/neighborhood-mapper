import creds
import mysql.connector
from mysql.connector import errorcode

def get_update_Set(connection, start_id, limit):
    sql = "select guid, add_number, streetname from tn_davidson_addresses \
        where guid > '%s' and padctn_id is null \
        order by guid \
        limit %d" % (start_id, limit)
    cursor = connection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows


def update_address(connection, row_in):
    sql = "select distinct padctn_id from real_estate_info_scrape where trim(location) like concat('%%','%s','%%') " \
          "and (trim(location) like concat('%%','%s',' %%') or trim(location) like concat('%%','%s'));" % (row_in[1], row_in[2], row_in[2])
    cursor = connection.cursor()
    cursor.execute(sql)

    rows = cursor.fetchall()
    if len(rows) == 1:
        for row in rows:
            sql = "update tn_davidson_addresses set padctn_id = %s where guid = '%s'" % (row[0], row_in[0])
            cursor.execute(sql)
    # else:
    #     #  nothing for now
    #     print("not found")
    return

def main():
      try:
            cnx = mysql.connector.connect(user=creds.user, password=creds.password,
                                          host=creds.host,
                                          database=creds.database)
      except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                  print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                  print("Database does not exist")
            else:
                  print(err)
      else:
          start_id = "{CFF"
          cont = True
          while(cont):
              update_list = get_update_Set(cnx, start_id, 1000)
              cont = False
              for index, row in enumerate(update_list, start=0):
                  cont = True
                  start_id = row[0]
                  if index == 1:
                      print(row[0])
                  update_address(cnx, row)
              cnx.commit()
      return

main()
quit()


