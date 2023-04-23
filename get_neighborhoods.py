# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import requests
import creds
import mysql.connector
from mysql.connector import errorcode
from lxml import html
from time import sleep
from requests_html import HTMLSession
import traceback

# 		https://davidson-tn-citizen.comper.info/template.aspx?propertyID=07116007400
url_base_1 = 'http://www.padctn.org/prc/property/'
url_base_2 = '/card/1'
table = 'neighborhoods'

def get_url_from_padctn(padctn_id):
    home_id = padctn_id
    full_url = url_base_1 + home_id + url_base_2
    print(full_url)
    for j in range(10):
        try:
            get_response = requests.get(full_url)
            tree = html.fromstring(get_response.content)
            comp_url_xpath = '//*[@id="propertyMapHolder"]/div[2]/a[1]/@href'
            comp_url_value = tree.xpath(comp_url_xpath)[0]
        except:
            print("Waiting to try again")
            sleep(60)
        else:
            # print(get_response.status_code)
            # print(contents.content)

            return comp_url_value

    print("Got lost or locked out")
    quit()

def get_info_from_id(map_parcel, neighborhood, padctn_id):
    skip = 0
    full_url = get_url_from_padctn(str(padctn_id))
    full_url = full_url.replace('http://', 'https://')
    for j in range(10):

        try:
            s = HTMLSession()
            response = s.get(full_url)
            response.html.render()
            description_title_xpath = '//*[@id="myPropertyInfo"]/div/div[3]/div/ul[2]/li[3]/@title'
            out_dict = {"id": int(neighborhood.strip()), "description": response.html.xpath(description_title_xpath)[0]}

        except Exception:
            # print("Waiting to try again")
            # sleep(60)
            skip = 1
            traceback.print_exc()
            print("skipping")
            response.session.close()
            return {}, skip
        else:
            # print(get_response.status_code)
            # print(contents.content)
            response.session.close()
            return out_dict, skip


    print("Got lost or locked out")
    quit()

def insert_values(insert_dict, connection):
    cursor = connection.cursor()

    columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in insert_dict.keys())
    values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in insert_dict.values())

    sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % (table, columns, values)
    print(sql)

    cursor.execute(sql)
    cursor.close()
    # print(insert_dict)


def update_values(insert_dict, connection):
    cursor = connection.cursor()
    sql = "update %s set description = '%s' where id = %d;" % \
              (table, insert_dict["description"], int(insert_dict["id"]))

    print("updating with:")
    print(sql)

    cursor.execute(sql)

    if cursor.rowcount == 0:
        insert_values(insert_dict, connection)
        print("inserted ", insert_dict["id"])
    elif cursor.rowcount == 1:
        print("updated ", insert_dict["id"])
    else:
        print("updated more than one record - bad")

    cursor.close()
    return


def get_update_Set(connection):
    sql = "select neighborhood, replace(replace(map_parcel,' ',''),'.','') as map_parcel_parse, padctn_id " \
          "from (select map_parcel, neighborhood, padctn_id, ROW_NUMBER() OVER (partition by neighborhood order by sale_Date desc) rn " \
          "from real_estate_info_scrape where padctn_id in (select padctn_id from tn_davidson_addresses)) r " \
          "where r.rn < 6 and neighborhood <> '' and neighborhood not in (select id from neighborhoods);"
    cursor = connection.cursor()
    cursor.execute(sql)
    out_list = cursor.fetchall()
    print(len(out_list))
    cursor.close()
    return out_list


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
        update_list = get_update_Set(cnx)
        last_neighborhood = 0
        for row in update_list: # range(range_min, range_max):
            id_in = str(row[1]) # str(i)
            current_neighborhood = row[0]
            if last_neighborhood != current_neighborhood:
                print(current_neighborhood)
                last_neighborhood = current_neighborhood
                skip = 0
                info_dict, skip = get_info_from_id(id_in, current_neighborhood, row[2])
                if skip == 0:
                    update_values(info_dict, cnx)
                    cnx.commit()
        cnx.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
    exit()
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
