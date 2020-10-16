from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import requests
import random
from fake_useragent import UserAgent
import time
import urllib.parse
import json
import pandas as pd
import string
import mysql.connector
from mysql.connector import errorcode
from secret import mySQL_password # Note: secret.py file

# Create or get a MySQL connection object
cnx = mysql.connector.connect(user='root', password=mySQL_password,
                              host='127.0.0.1',
                              database='trails')
print(cnx)

# Instantiates and returns a cursor using C Extension
cursor = cnx.cursor()

### Exploratory Data Analysis on `trail_urls_ca` table
sql = "SELECT COUNT(*) FROM trails.trail_urls_ca"
cursor.execute(sql)
result = cursor.fetchall()

for row in result:
    print(row[0])


sql = "SELECT COUNT(DISTINCT trail_id) FROM trails.trail_urls_ca"
cursor.execute(sql)
result = cursor.fetchall()

for row in result:
    print(row[0])


sql = "SELECT COUNT(DISTINCT url) FROM trails.trail_urls_ca"
cursor.execute(sql)
result = cursor.fetchall()

for row in result:
    print(row[0])


sql = "SELECT COUNT(DISTINCT city_id) FROM trails.trail_urls_ca"
cursor.execute(sql)
result = cursor.fetchall()

for row in result:
    print(row[0])


sql = "SELECT COUNT(DISTINCT city) FROM trails.trail_urls_ca"
cursor.execute(sql)
result = cursor.fetchall()

for row in result:
    print(row[0])


sql = "SELECT COUNT(DISTINCT trail_name) FROM trails.trail_urls_ca"
cursor.execute(sql)
result = cursor.fetchall()

for row in result:
    print(row[0])


sql = """
SELECT
    trail_name,
    COUNT(trail_name)
FROM
    trails.trail_urls_ca
GROUP BY trail_name
HAVING COUNT(trail_name) > 1
"""
cursor.execute(sql)
result = cursor.fetchall()

for row in result:
    print(row)


sql = """
SELECT *
FROM
    trails.trail_urls_ca
WHERE trail_name = 'Black Mountain Trail'
"""
cursor.execute(sql)
result = cursor.fetchall()

for row in result:
    print(row)


#### Retrieve the trail_name and url from the `trail_urls_ca` table
sql = "SELECT trail_name, url FROM trails.trail_urls_ca"
cursor.execute(sql)
trail_name_urls_list_sql = [i[0:2] for i in cursor.fetchall()]

len(trail_name_urls_list_sql)
trail_name_urls_list_sql[0:5]


#### Retrieve ONLY the trail url and trail_id from the `trail_urls_ca` table that aren't already in `trail_info` table
sql = """
SELECT urls.url, urls.trail_id, info.trail_id
FROM trails.trail_urls_ca urls
LEFT JOIN trails.trail_info info
ON urls.trail_id = info.trail_id
WHERE info.trail_id IS NULL"""
cursor.execute(sql)
trail_urls_trail_id_list_sql = [i[0:3] for i in cursor.fetchall()]

len(trail_urls_trail_id_list_sql)
trail_urls_trail_id_list_sql[0:5]

# Note: The URLs above reroute to a different trail website.  The trail_id associated with the different trail website is actually already contained in the `trail_info` table.


#### Retrieve ONLY the trail url from the `trail_urls_ca` table that aren't already in `trail_info` table
sql = """
SELECT urls.url
FROM trails.trail_urls_ca urls
LEFT JOIN trails.trail_info info
ON urls.trail_id = info.trail_id
WHERE info.trail_id IS NULL"""
cursor.execute(sql)
trail_urls_list_sql = [i[0] for i in cursor.fetchall()]

len(trail_urls_list_sql)
trail_urls_list_sql[0:5]

# Note: The URLs above reroute to a different trail website.  The trail_id associated with the different trail website is actually already contained in the `trail_info` table.


#### [How to Not Get Caught While Webscraping](https://medium.com/datadriveninvestor/how-to-not-get-caught-while-web-scraping-88097b383ab8)
def get_proxies():
    proxy_url = 'https://free-proxy-list.net/'
    response = requests.get(proxy_url)
    page_html = response.text
    page_soup = BeautifulSoup(page_html, 'html.parser')
    containers = page_soup.find_all('div', {'class': 'table-responsive'})[0]
    ip_index = [8*k for k in range(80)]
    proxies = set()

    for i in ip_index:
        ip = containers.find_all('td')[i].text
        port = containers.find_all('td')[i+1].text
        anonymity = containers.find_all('td')[i+4].text
        https = containers.find_all('td')[i+6].text

        if https == 'yes' and anonymity == 'elite proxy':
            print('\nIP Address: {}'.format(ip))
            print('Port: {}'.format(port))
            print('Anonymity: {}'.format(anonymity))
            print('https: {}'.format(https))
            proxy = ip + ':' + port
            proxies.add(proxy)

    return proxies


def check_proxies():
    working_proxies = set()
    proxies = get_proxies()
    print('\n', proxies)
    test_url = 'https://httpbin.org/ip'
    for i in proxies:
        print('\nTrying to connect with proxy: {}'.format(i))
        try:
            response = requests.get(test_url, proxies={'http': i, 'https': i}, timeout=5)
            print(response.json())
            print('This proxy is added to the working list')
            working_proxies.add(i)

        except:
            print('Skipping. Connnection error')

    return working_proxies

working_proxies = check_proxies()
working_proxies


# Drop table if exists
# sql = 'DROP TABLE IF EXISTS trail_info'
# cursor.execute(sql)

# Store CREATE statements in Python dictionary TABLES
TABLES = {}

TABLES['trail_info'] = (
    "CREATE TABLE `trail_info` ("
    "  `state` VARCHAR(21) NOT NULL,"
    "  `city` VARCHAR(50) NULL,"
    "  `trail_name` VARCHAR(100) NOT NULL,"
    "  `trail_id` INT(11) NOT NULL,"
    "  `trail_difficulty` VARCHAR(9) NOT NULL,"
    "  `stars` DECIMAL(2,1) NOT NULL,"
    "  `num_reviews` INT(11) NOT NULL,"
    "  `trail_region` VARCHAR(100) NOT NULL,"
    "  `distance` VARCHAR(20) NOT NULL,"
    "  `duration_mins` FLOAT NULL,"
    "  `elevation_gain` VARCHAR(20) NOT NULL,"
    "  `route_type` VARCHAR(15) NOT NULL,"
    "  `trail_tags_str` VARCHAR(255) NOT NULL,"
    "  PRIMARY KEY (`trail_id`)"
    ") ENGINE=InnoDB")

# Create tables by iterating over the items of the TABLES dictionary
for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print('Creating table {}: '.format(table_name), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print('already exists')
        else:
            print(err.msg)
    else:
        print('OK')

#cursor.close()
#cnx.close()


working_proxies = {'83.97.23.90:18080'}
#working_proxies = {'83.97.23.90:18080', '13.59.75.183:3838'}
working_proxies


def parse_meta_data(trail_urls_list):

    trails_added_mySQL = 0
    trail_counter = 0
    proxy_counter = 0
    max_trails_per_proxy = 100
    n_times_proxy_used = 0

    global working_proxies
    working_proxies_list = list(working_proxies)

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('log-level=3')
    options.add_argument('--no-sandbox')
    options.add_argument('start-maximized')
    options.add_argument('disable-infobars')
    options.add_argument('--disable-extensions')
    options.page_load_strategy = 'none'

    for trail_url in trail_urls_list:

        if n_times_proxy_used == max_trails_per_proxy:
            proxy_counter += 1
            n_times_proxy_used = 0

        cannot_connect_proxy = True
        while cannot_connect_proxy:

            prox = working_proxies_list[proxy_counter]
            options.add_argument('--proxy-server={}'.format(prox))
            ua = UserAgent()
            userAgent = ua.random
            options.add_argument('user-agent={}'.format(userAgent))

            try:
                # Creates a new ChromeDriver instance of the chrome driver
                driver = webdriver.Chrome(options=options)
                driver.get(trail_url)
                soup = BeautifulSoup(driver.page_source)
                #print('Soup:', soup)
                time.sleep(random.randint(3, 6))
                driver.quit()
                cannot_connect_proxy = False
                n_times_proxy_used += 1
                print('\nProxy selected: {}'.format(prox))

                #if n_times_proxy_used == max_trails_per_proxy:
                #    proxy_counter += 1
                #    n_times_proxy_used = 0

            except Exception:
                driver.quit()
                working_proxies.remove(prox)
                print('\n{} proxy removed from the set of proxy'.format(prox))
                cannot_connect_proxy = True
                proxy_counter += 1
                n_times_proxy_used = 0

                # Update proxy list if you run out of proxies
                if len(working_proxies) < 3:
                    working_proxies = check_proxies()
                    working_proxies_list = list(working_proxies)
                    proxy_counter = 0

        trail_counter += 1

        print('Trail Counter:', trail_counter)
        print('Proxy Counter:', proxy_counter)

        header_bottom_container = soup.find('div', id='header-bottom-container')
        #print(header_bottom_container)
        if trail_url[0:34] == 'https://www.trailwebsite.com':
            state = header_bottom_container.find_all('span')[2].get_text()
        else:
            state = header_bottom_container.find_all('span')[0].get_text()

        header = soup.find('div', id='title-and-menu-box')

        drp = header.find('div', {'data-react-class': 'TrailDetailsCard'})['data-react-props']
        trail_details = json.loads(drp)
        #print(trail_details['trail'])
        city = trail_details['trail'].get('city_name')
        duration_mins = trail_details['trail'].get('duration_minutes')
        trail_id = trail_details['trail'].get('trail_id')

        trail_name = header.findChild('h1').get_text()
        print(trail_name)
        difficulty = header.findChild('span').get_text()
        stars = float(header.find('meta', itemprop='ratingValue')['content'])
        num_reviews = int(header.find('meta', itemprop='reviewCount')['content'])
        trail_region = header.findChild('a').get_text()

        try:
            distance = soup.select('span.distance-icon')[0].get_text(strip=True).replace('Length', '')
        except:
            distance = None
        try:
            elevation_gain = soup.select('span.elevation-icon')[0].get_text(strip=True).replace('Elevation gain', '')
        except:
            elevation_gain = None
        try:
            route_type = soup.select('span.route-icon')[0].get_text(strip=True).replace('Route type', '')
        except:
            route_type = None

        tags = soup.select('section.tag-cloud')[0].findChildren('h3')
        trail_tags = []
        for tag in tags:
            trail_tags.append(tag.get_text())

        # user_reviews = []
        # users = soup.select('div.feed-user-content.rounded')
        # for user in users:
        #     if user.find('span', itemprop='author') != None:
        #         user_name = user.find('span', itemprop='author').text
        #         user_name = user_name.replace('.', '')
        #         try:
        #             rating = user.find('span', itemprop='reviewRating').findChildren('meta')[0]['content']
        #             user_reviews.append({user_name: rating})
        #         except:
        #             pass

        row_data = {}
        row_data['state'] = state
        row_data['city'] = city
        row_data['trail_name'] = trail_name
        row_data['trail_id'] = trail_id
        row_data['trail_difficulty'] = difficulty
        row_data['stars'] = stars
        row_data['num_reviews'] = num_reviews
        row_data['trail_region'] = trail_region
        row_data['distance'] = distance
        row_data['duration_mins'] = duration_mins
        row_data['elevation_gain'] = elevation_gain
        row_data['route_type'] = route_type
        #row_data['trail_tags'] = trail_tags
        row_data['trail_tags_str'] = ' '.join([string.capwords(elem).replace(' ', '') for elem in trail_tags])
        #row_data['reviews'] = user_reviews

        values = tuple(row_data.values())
        #print(values)

        # Execute given statement using given parameters
        # Note: If the record is a duplicate, then the IGNORE keyword tells MySQL to discard it silently without generating an error
        cursor.execute('''
            INSERT IGNORE INTO trail_info (state, city, trail_name, trail_id, trail_difficulty, stars, num_reviews, trail_region, distance, duration_mins, elevation_gain, route_type, trail_tags_str)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', values)
        rowcount = cursor.rowcount

        if rowcount == 1:
            trails_added_mySQL += 1
        cnx.commit()

    print('\nTotal trails added to mySQL:', trails_added_mySQL)


trail_urls_list_sql_100 = trail_urls_list_sql[0:100]
len(trail_urls_list_sql_100)

# Scrape data from the trail URLs
get_scrape_time_start = time.time()
parse_meta_data(trail_urls_list_sql_100)
minutes = (time.time() - get_scrape_time_start)/60
print('Get scrape time: {} hrs: {} mins'.format(int(minutes // 60), round(minutes % 60)))


def parse_meta_data_same_proxy(trail_urls_list, proxy):

    trails_added_mySQL = 0
    trail_counter = 0
    #proxy_counter = 0
    #max_trails_per_proxy = 10
    #n_times_proxy_used = 0

    #global working_proxies
    #working_proxies_list = list(working_proxies)

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('log-level=3')
    options.add_argument('--no-sandbox')
    options.add_argument('start-maximized')
    options.add_argument('disable-infobars')
    options.add_argument('--disable-extensions')
    options.page_load_strategy = 'none'
    prox = proxy
    options.add_argument('--proxy-server={}'.format(prox))
    print('\nProxy selected: {}'.format(prox))
    ua = UserAgent()
    userAgent = ua.random
    options.add_argument('user-agent={}'.format(userAgent))

    for trail_url in trail_urls_list:

        driver = webdriver.Chrome(options=options)
        driver.get(trail_url)
        soup = BeautifulSoup(driver.page_source)
        #print('Soup:', soup)
        driver.quit()

        trail_counter += 1

        print('\nTrail Counter:', trail_counter)
        #print('Proxy Counter:', proxy_counter)

        header_bottom_container = soup.find('div', id='header-bottom-container')
        #print(header_bottom_container)
        if trail_url[0:34] == 'https://www.trailwebsite.com:
            try:
                state = header_bottom_container.find_all('span')[2].get_text()
            except:
                state = None
        else:
            try:
                state = header_bottom_container.find_all('span')[0].get_text()
            except:
                state = None

        header = soup.find('div', id='title-and-menu-box')
        if header == None:
            driver.quit()
            continue

        drp = header.find('div', {'data-react-class': 'TrailDetailsCard'})['data-react-props']
        trail_details = json.loads(drp)
        #print(trail_details['trail'])
        city = trail_details['trail'].get('city_name')
        duration_mins = trail_details['trail'].get('duration_minutes')
        trail_id = trail_details['trail'].get('trail_id')

        trail_name = header.findChild('h1').get_text()
        print(trail_name)
        difficulty = header.findChild('span').get_text()
        stars = float(header.find('meta', itemprop='ratingValue')['content'])
        num_reviews = int(header.find('meta', itemprop='reviewCount')['content'])
        trail_region = header.findChild('a').get_text()

        try:
            distance = soup.select('span.distance-icon')[0].get_text(strip=True).replace('Length', '')
        except:
            distance = None
        try:
            elevation_gain = soup.select('span.elevation-icon')[0].get_text(strip=True).replace('Elevation gain', '')
        except:
            elevation_gain = None
        try:
            route_type = soup.select('span.route-icon')[0].get_text(strip=True).replace('Route type', '')
        except:
            route_type = None

        tags = soup.select('section.tag-cloud')[0].findChildren('h3')
        trail_tags = []
        for tag in tags:
            trail_tags.append(tag.get_text())

        # user_reviews = []
        # users = soup.select('div.feed-user-content.rounded')
        # for user in users:
        #     if user.find('span', itemprop='author') != None:
        #         user_name = user.find('span', itemprop='author').text
        #         user_name = user_name.replace('.', '')
        #         try:
        #             rating = user.find('span', itemprop='reviewRating').findChildren('meta')[0]['content']
        #             user_reviews.append({user_name: rating})
        #         except:
        #             pass

        row_data = {}
        row_data['state'] = state
        row_data['city'] = city
        row_data['trail_name'] = trail_name
        row_data['trail_id'] = trail_id
        row_data['trail_difficulty'] = difficulty
        row_data['stars'] = stars
        row_data['num_reviews'] = num_reviews
        row_data['trail_region'] = trail_region
        row_data['distance'] = distance
        row_data['duration_mins'] = duration_mins
        row_data['elevation_gain'] = elevation_gain
        row_data['route_type'] = route_type
        #row_data['trail_tags'] = trail_tags
        row_data['trail_tags_str'] = ' '.join([string.capwords(elem).replace(' ', '') for elem in trail_tags])
        #row_data['reviews'] = user_reviews

        values = tuple(row_data.values())
        print(values)

        # Execute given statement using given parameters
        # Note: If the record is a duplicate, then the IGNORE keyword tells MySQL to discard it silently without generating an error
        cursor.execute('''
            INSERT IGNORE INTO trail_info (state, city, trail_name, trail_id, trail_difficulty, stars, num_reviews, trail_region, distance, duration_mins, elevation_gain, route_type, trail_tags_str)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', values)
        rowcount = cursor.rowcount

        if rowcount == 1:
            trails_added_mySQL += 1
        cnx.commit()

    print('\nTotal trails added to mySQL:', trails_added_mySQL)


trail_urls_list_sql_10149 = trail_urls_list_sql[0:10149]
len(trail_urls_list_sql_10149)

parse_meta_data_same_proxy(trail_urls_list_sql_10149, '83.97.23.90:18080')
