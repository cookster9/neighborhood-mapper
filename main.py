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

def get_info_from_id(id, connection, neighborhood_id):
    home_id = id
    full_url = url_base_1 + home_id + url_base_2
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
    # # Example of a parameterized query
    # query = "SELECT * FROM users WHERE username = %s AND password = %s"
    # user_input = ("user_input_username", "user_input_password")
    #
    # cursor.execute(query, user_input)
    # result = cursor.fetchall()

    sql = """select tn_davidson_addresses_id 
    from real_estate_info_scrape 
    where padctn_id = %s 
    and tn_davidson_addresses_id is not null
    order by sale_date desc
    limit 1"""
    user_input = (padctn_id,)
    cursor = cnx.cursor()
    cursor.execute(sql, user_input)
    rows = cursor.fetchall()
    if len(rows) > 0:
        return rows[0][0]
    else:
        neighborhood_latitude = get_neighborhood_lat(cnx, neighborhood_id)
        sql = """select id
            from tn_davidson_addresses t
            where %s like concat('%',add_number,'%')
            and %s like concat('%',streetname,'%')
            order by abs(%s-latitude)
            limit 1
            ;
            """
        user_input = (location, location, neighborhood_latitude)
        cursor = cnx.cursor()
        cursor.execute(sql, user_input)
        rows = cursor.fetchall()
        cursor.close()
        if len(rows) > 0:
            return rows[0][0]
        else:
            return 'NULL'

def get_neighborhood_lat(cnx, neighborhood_id):
    sql = """select coalesce(latitude,0) latitude
        from neighborhoods 
        where id = %s
        """
    user_input = (neighborhood_id,)
    cursor = cnx.cursor()
    cursor.execute(sql, user_input)
    rows = cursor.fetchall()
    cursor.close()
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
        address = None
        # insert_dict["tn_davidson_addresses_id"] = address

    del insert_dict["tn_davidson_addresses_id"]
    columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in insert_dict.keys())
    values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in insert_dict.values())
    # values = values + ', ' + address
    sql = """INSERT INTO real_estate_info_scrape ( {0}, tn_davidson_addresses_id ) 
    VALUES ( {1}, ?);"""
    # too hard to parameterize with mysql

    sql = sql.format(columns, values)
    user_input = (address,)
    cursor.execute(sql, user_input)
    cursor.close()
    # print(insert_dict)


def update_values(insert_dict, connection):
    cursor = connection.cursor()

    address = insert_dict["tn_davidson_addresses_id"]
    if address is None or address == 'NULL':
        address = None
    sql = ''
    if insert_dict["sale_date"] == '' or insert_dict["sale_date"] == 'null':
        sql = """update real_estate_info_scrape
        set location = %s
        ,square_footage = %s
        ,tn_davidson_addresses_id = %s
        where padctn_id = %s;"""
        user_input = (insert_dict["location"], insert_dict["square_footage"], address, insert_dict["padctn_id"])
    else:
        sql = """update real_estate_info_scrape
                set location = %s
                ,square_footage = %s
                ,tn_davidson_addresses_id = %s
                where padctn_id = %s and sale_date = %s;"""
        user_input = (insert_dict["location"], insert_dict["square_footage"], address, insert_dict["padctn_id"],insert_dict["sale_date"])
    cursor.execute(sql, user_input)

    if cursor.rowcount == 0:
        cursor.close()
        found = get_existing(insert_dict, connection)
        if found == 0:
            insert_values(insert_dict, connection)
            print("inserted ", insert_dict["padctn_id"])
    else:
        cursor.close()

    return

def get_existing(insert_dict, connection):
    sql = ''
    if insert_dict["sale_date"] == '' or insert_dict["sale_date"] == 'null':
        sql = "select id from real_estate_info_scrape where padctn_id = %s and sale_Date is null"
        user_input = (insert_dict["padctn_id"],)
    else:
        sql = "select id from real_estate_info_scrape where padctn_id = %s and sale_Date= %s"
        user_input = (insert_dict["padctn_id"], insert_dict["sale_date"])
    cursor = connection.cursor()
    cursor.execute(sql, user_input)
    cursor.fetchall()
    found_return = cursor.rowcount
    cursor.close()
    return found_return


def get_update_set(connection, id):
    sql = """select padctn_id from (
            select padctn_id, neighborhoods_id, property_use, ROW_NUMBER() OVER (partition by padctn_id order by sale_date desc) rn 
            from real_estate_info_scrape) r1
            where rn = 1 and neighborhoods_id = %s
            and property_use in ('SINGLE FAMILY','RESIDENTIAL CONDO');"""
    user_input = (id,)
    cursor = connection.cursor()
    cursor.execute(sql, user_input)
    id_list = []
    for (row) in cursor:
        id_list.append(row[0])
    cursor.close()
    return id_list

def update_last_updated(connection, id):
    sql="""update neighborhoods set last_updated = now(), status = 'pending' where id = %s"""
    user_input = (id,)
    cursor = connection.cursor()
    cursor.execute(sql, user_input)
    cursor.close()

def main(neighborhood_id):
    creds_json = getAWSCreds.secretjson
    cnx = get_connection(creds_json["username"], creds_json["password"], creds_json["host"], creds_json["dbname"])

    update_list = get_update_set(cnx, neighborhood_id)
    blank_count = 0  #count number of blanks in a row to try to figure out where the end is
    for update_id in update_list: # range(range_min, range_max):
        id_in = str(update_id) # str(i)
        info_dict = get_info_from_id(id_in, cnx, neighborhood_id)
        if info_dict["map_parcel"].strip() != '':
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
