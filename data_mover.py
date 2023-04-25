import creds
import mysql.connector
from mysql.connector import errorcode
from my_utils import get_connection


def get_update_Set(connection, start_id, limit):
    sql = """SELECT `real_estate_info_scrape`.`padctn_id`,
    `real_estate_info_scrape`.`map_parcel`,
    `real_estate_info_scrape`.`mailing_address`,
    `real_estate_info_scrape`.`sale_price`,
    `real_estate_info_scrape`.`property_use`,
    `real_estate_info_scrape`.`zone`,
    `real_estate_info_scrape`.`neighborhood`,
    `real_estate_info_scrape`.`location`,
    `real_estate_info_scrape`.`sale_date`,
    `real_estate_info_scrape`.`year_week`,
    id
    FROM real_estate_info_scrape
            where id > %d
            order by id
            limit %d""" % (start_id, limit)
    cursor = connection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows


def insert_rows(connection, update_list):
    cursor = connection.cursor()
    for row in update_list:
        out_id=row[10]
        sql = """INSERT INTO `real_estate_info_scrape`
        (
        `padctn_id`,
        `map_parcel`,
        `mailing_address`,
        `sale_price`,
        `property_use`,
        `zone`,
        `neighborhood`,
        `location`,
        `sale_date`,
        `year_week`)
        VALUES
        (
        '%s',
        '%s',
        '%s',
        '%s',
        '%s',
        '%s',
        '%s',
        '%s',
        '%s',
        '%s')""" % (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
        cursor.execute(sql)
    cursor.close()
    return out_id


def main():
    source_cnx = get_connection(creds.user, creds.password, creds.host, creds.database)
    print("got source connect")

    dest_cnx = get_connection(creds.dest_user, creds.dest_pass, creds.dest_host, creds.dest_database)
    print("got dest connect")

    update_list =['init']
    last_row=0
    while len(update_list) > 0:
        print(".", end="")
        update_list = get_update_Set(source_cnx, last_row, 500)
        if len(update_list) > 0:
            last_row = insert_rows(dest_cnx, update_list)
            dest_cnx.commit()
    return

main()
quit()