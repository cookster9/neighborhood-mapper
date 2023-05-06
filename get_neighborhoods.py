# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import requests
import creds
from lxml import html
from time import sleep
from requests_html import HTMLSession
import traceback
from my_utils import get_connection

# 		https://davidson-tn-citizen.comper.info/template.aspx?propertyID=07116007400
url_base_1 = 'http://www.padctn.org/prc/property/'
url_base_2 = '/card/1'
table = 'neighborhoods'

def get_url_from_padctn(padctn_id):
    home_id = padctn_id
    full_url = url_base_1 + home_id + url_base_2
    for j in range(10):
        try:
            get_response = requests.get(full_url)
            tree = html.fromstring(get_response.content)
            comp_url_xpath = '//*[@id="propertyMapHolder"]/div[2]/a[1]/@href'
            comp_url_value = tree.xpath(comp_url_xpath)[0]
            print("in get url func: ", comp_url_value, full_url)
        except:
            print("Waiting to try again")
            sleep(60)
        else:
            # print(get_response.status_code)
            # print(contents.content)

            return comp_url_value

    print("Got lost or locked out")
    quit()

def get_info_from_id(neighborhood, padctn_id):
    skip = 0
    full_url = get_url_from_padctn(str(padctn_id))
    full_url = full_url.replace('http://', 'https://')
    try:
        s = HTMLSession()
        response = s.get(full_url)
        response.html.render(wait=1.0)
        description_title_xpath = '//*[@id="myPropertyInfo"]/div/div[3]/div/ul[2]/li[3]/@title'
        print("In get info func: ",  response.html.xpath(description_title_xpath))
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
    sql = """select neighborhood, padctn_id
          from real_estate_info_scrape
          where neighborhood <> '' and neighborhood not in (select id from neighborhoods)
          order by neighborhood;"""
    cursor = connection.cursor()
    cursor.execute(sql)
    out_list = cursor.fetchall()
    print(len(out_list))
    cursor.close()
    return out_list


def main():

    cnx = get_connection(user=creds.aws_user, password=creds.aws_pass, host=creds.aws_host, db=creds.aws_database)

    update_list = get_update_Set(cnx)
    last_neighborhood = 0
    done = 0
    for row in update_list: # range(range_min, range_max):
        current_neighborhood = row[0]
        if last_neighborhood != current_neighborhood:
            done = 0
        if done == 0:
            print(current_neighborhood)
            last_neighborhood = current_neighborhood
            skip = 0
            print("going into info func: ", current_neighborhood, row[1])
            info_dict, skip = get_info_from_id(current_neighborhood, row[1])
            if skip == 0:
                update_values(info_dict, cnx)
                cnx.commit()
                done = 1
    cnx.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
    exit()
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
