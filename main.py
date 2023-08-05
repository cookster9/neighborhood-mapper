# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import requests
import getAWSCreds
from lxml import html
from time import sleep
from datetime import date
from my_utils import get_connection
import sys
from sys import platform
# import creds
# not really platform specific, but i have a local version on mac and prod in linux so this is an easy way to
# change credentials for a local dev environment
if platform == "linux" or platform == "linux2":
    import getAWSCreds
elif platform == "darwin":
    import creds_info as getAWSCreds
elif platform == "win32":
    import local_creds as getAWSCreds

url_base_1 = 'http://www.padctn.org/prc/property/'
url_base_2 = '/card/1'
table = 'real_estate_info_scrape'


def get_info_from_id(id, connection, neighborhood_id):
    home_id = id
    full_url = url_base_1 + home_id + url_base_2
    print(full_url)
    for j in range(10):
        try:
            get_response = requests.get(full_url)
            tree = html.fromstring(get_response.content)
            map_parcel_xpath = '// *[ @ id = "propertyOverview"] / ul / li[1] / text()'
            mailing_address_xpath = '//*[@id="propertyOverview"]/div[4]/ul/li[1]/text()'
            sale_date_xpath = '//*[@id="propertyOverview"]/div[4]/ul/li[6]/text()'
            sale_price_xpath = '//*[@id="propertyOverview"]/div[4]/ul/li[7]/text()'
            property_use_xpath = '//*[@id="content"]/div/div[4]/div[1]/ul/li[7]/text()'
            sq_ft_xpath='//*[@id="content"]/div/div[4]/div[2]/div/div[1]/ul/li[3]/text()'
            zone_xpath = '//*[@id="content"]/div/div[4]/div[1]/ul/li[8]/text()'
            neighborhood_xpath = '//*[@id="content"]/div/div[4]/div[1]/ul/li[9]/text()'
            location_xpath = '//*[@id="propertyOverview"]/ul/li[2]/text()'
            location = tree.xpath(location_xpath)[0].strip()

            sale_date_value, sale_date_year_week = parse_date(tree.xpath(sale_date_xpath)[0])


            address_id = get_address(home_id, connection, neighborhood_id, location)

            out_dict = {"padctn_id": home_id, "map_parcel": tree.xpath(map_parcel_xpath)[0],
                        "mailing_address": tree.xpath(mailing_address_xpath)[0],
                        "sale_date": sale_date_value, "sale_price": tree.xpath(sale_price_xpath)[0].strip(),
                        "property_use": tree.xpath(property_use_xpath)[0].strip(), "zone": tree.xpath(zone_xpath)[0],
                        "neighborhoods_id": neighborhood_id, "location": location,
                        "year_week": sale_date_year_week, "tn_davidson_addresses_id": address_id
                        ,"square_footage": tree.xpath(sq_ft_xpath)[0].strip()
                        }
        except Exception as e:
            print(e)
            print("Waiting to try again")
            sleep(60)
        else:
            # print(get_response.status_code)
            # print(contents.content)

            return out_dict

    print("Got lost or locked out")
    quit()

def get_address(padctn_id, cnx, neighborhood_id, location):
    sql = """select tn_davidson_addresses_id 
    from real_estate_info_scrape 
    where padctn_id = {0} 
    and tn_davidson_addresses_id is not null
    order by sale_date desc
    limit 1""".format(padctn_id)
    cursor = cnx.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    if len(rows) > 0:
        return rows[0][0]
    else:
        neighborhood_latitude = get_neighborhood_lat(cnx, neighborhood_id)
        sql = """select id
            from tn_davidson_addresses t
            where '{0}' like concat('%',add_number,'%')
            and '{0}' like concat('%',streetname,'%')
            order by abs({1}-latitude)
            limit 1
            ;
            """.format(location, neighborhood_latitude)
        cursor = cnx.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        if len(rows) > 0:
            return rows[0][0]
        else:
            return 'NULL'

def get_neighborhood_lat(cnx, neighborhood_id):
    sql = """select coalesce(latitude,0) latitude
        from neighborhoods 
        where id = {0} 
        """.format(neighborhood_id)
    cursor = cnx.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    if len(rows) > 0:
        return rows[0][0]
    else:
        return 0

def parse_date(date_in):
    date_out = date_in.strip()
    date_week = ''
    # mm_dd_yyyy to yyyy-mm-dd
    if date_out != '':
        month = date_out[0:2]
        day = date_out[3:5]
        year = date_out[6:10]
        date_out = year+'-'+month+'-'+day
        date_tuple = date(int(year), int(month), int(day)).isocalendar()
        date_week = str(date_tuple[0])+str(date_tuple[1]).strip().rjust(2, '0')
    return date_out, date_week


def insert_values(insert_dict, connection):
    cursor = connection.cursor()

    address = insert_dict["tn_davidson_addresses_id"]
    if address is None or address == 'NULL':
        address == None
        # insert_dict["tn_davidson_addresses_id"] = address

    del insert_dict["tn_davidson_addresses_id"]
    columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in insert_dict.keys())
    values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in insert_dict.values())
    # values = values + ', ' + address
    sql = """INSERT INTO {0} ( {1}, tn_davidson_addresses_id ) 
    VALUES ( {2}, {3});""".format(table, columns, values, address)

    cursor.execute(sql)
    cursor.close()
    # print(insert_dict)


def update_values(insert_dict, connection):
    cursor = connection.cursor()


    # columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in insert_dict.keys())
    # values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in insert_dict.values())
    sql = ''
    if insert_dict["sale_date"] == '' or insert_dict["sale_date"] == 'null':
        sql = """update {0} 
        set location = '{2}'
        ,square_footage = '{3}'
        where padctn_id = {1};""".format(table, insert_dict["padctn_id"], insert_dict["location"], insert_dict["square_footage"])
    else:
        sql = """update {0} 
                set location = '{3}'
                ,square_footage = '{4}'
                where padctn_id = {1} and sale_date = '{2}';""".format(table, insert_dict["padctn_id"],insert_dict["sale_date"], insert_dict["location"], insert_dict["square_footage"])

    cursor.execute(sql)

    if cursor.rowcount == 0:
        cursor.close()
        found = get_existing(insert_dict, connection)
        if found == 0:
            insert_values(insert_dict, connection)
            # print("inserted ", insert_dict["padctn_id"])
    else:
        cursor.close()

    return

def get_existing(insert_dict, connection):
    sql = ''
    if insert_dict["sale_date"] == '' or insert_dict["sale_date"] == 'null':
        sql = "select id from %s where padctn_id = %s and sale_Date is null" % \
              (table, insert_dict["padctn_id"])
    else:
        sql = "select id from %s where padctn_id = %s and sale_Date= '%s'" % \
          (table, insert_dict["padctn_id"], insert_dict["sale_date"])
    cursor = connection.cursor()
    cursor.execute(sql)
    cursor.fetchall()
    found_return = cursor.rowcount
    cursor.close()
    return found_return


def get_update_Set(connection, id):
    sql = "select padctn_id from ( \
            select padctn_id, neighborhoods_id, ROW_NUMBER() OVER (partition by padctn_id order by sale_date desc) rn from %s) r1 \
            where rn = 1 and neighborhoods_id = %s" % (table, id)
    cursor = connection.cursor()
    cursor.execute(sql)
    id_list = []
    for (row) in cursor:
        id_list.append(row[0])
    cursor.close()
    return id_list

def update_last_updated(connection, id):
    sql="""update neighborhoods set last_updated = now(), status = 'pending' where id = %s""" % id
    cursor = connection.cursor()
    cursor.execute(sql)

def main(neighborhood_id):
    creds_json = getAWSCreds.secretjson
    cnx = get_connection(creds_json["username"], creds_json["password"], creds_json["host"], creds_json["dbname"])

    update_list = get_update_Set(cnx, neighborhood_id)
    blank_count = 0  #count number of blanks in a row to try to figure out where the end is
    for update_id in update_list: # range(range_min, range_max):
        id_in = str(update_id) # str(i)
        info_dict = get_info_from_id(id_in, cnx, neighborhood_id)
        if info_dict["map_parcel"].strip() != '':
            print(info_dict)
            blank_count = 0
            update_values(info_dict, cnx)
            cnx.commit()
        else:
            blank_count = blank_count + 1
        if blank_count > 1000:
            print("Found a bunch of blanks in a row - maybe done here:")
            print(id_in)
            break
    update_last_updated(cnx, neighborhood_id)
    cnx.commit()
    cnx.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main(sys.argv[1])
    exit()
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
