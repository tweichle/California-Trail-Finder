from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
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


# Retrieve the city URLs from the database
#sql = "SELECT url FROM trails.city_urls"
sql = "SELECT url FROM trails.city_urls WHERE state = 'California'"
cursor.execute(sql)
city_urls_list_sql = [i[0] for i in cursor.fetchall()]

len(city_urls_list_sql)
city_urls_list_sql[0:5]


sql = "SELECT state, city, url FROM trails.city_urls WHERE state = 'California'"
cursor.execute(sql)
state_city_urls_list_sql = [i[0:3] for i in cursor.fetchall()]

state_city_urls_list_sql[0:5]


# Drop table if exists
sql = 'DROP TABLE IF EXISTS trail_urls_ca'
cursor.execute(sql)

# Store CREATE statements in Python dictionary TABLES
TABLES = {}

TABLES['trail_urls_ca'] = (
    "CREATE TABLE `trail_urls_ca` ("
    "  `city_id` INT(11) NOT NULL,"
    "  `city` VARCHAR(50) NULL,"
    "  `trail_id` INT(11) NOT NULL,"
    "  `trail_name` VARCHAR(100) NULL,"
    "  `url` VARCHAR(255) NOT NULL,"
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


def get_trail_urls(city_urls_list):

    options = webdriver.ChromeOptions()
    options.add_argument('start-maximized')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)

    for url in city_urls_list:

        # Creates a new ChromeDriver instance of the chrome driver
        driver = webdriver.Chrome()
        driver.get(url)
        soup = BeautifulSoup(driver.page_source)

        header_bottom_container = soup.find('div', id='header-bottom-container')
        if header_bottom_container == None:
            driver.quit()
            continue

        while True:
            try:
                driver.execute_script('return arguments[0].scrollIntoView(true);', WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="stick-bar-parent"]/div[2]/div[2]/div/button'))))
                time.sleep(5)
                WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="stick-bar-parent"]/div[2]/div[2]/div/button'))).click()
            except TimeoutException:
                break

        drp = soup.find('div', {'data-react-class': 'Location'})['data-react-props']
        location_details = json.loads(drp)
        location_object = location_details['locationData'].get('shareObject')
        city_id = location_object.get('id')
        city = location_object.get('name')

        trail_ids = [i.get_attribute('data-id') for i in driver.find_elements_by_css_selector('div.styles-module__trailCard___2oHiP')]
        trail_names = [i.get_attribute('title') for i in driver.find_elements_by_css_selector('div.styles-module__trailCard___2oHiP > a')]
        urls = [i.get_attribute('href') for i in driver.find_elements_by_css_selector('div.styles-module__trailCard___2oHiP > a')]

        trail_list = [{'trail_id': trail_id, 'trail_name': trail_name, 'url': url} for trail_id, trail_name, url in zip(trail_ids, trail_names, urls)]

        driver.quit()

        print('\nCity: {}; Trails: {}'.format(city, len(urls)))

        trails_added = 0
        for trail in trail_list:
            row_data = {}
            row_data['city_id'] = city_id
            row_data['city'] = city
            row_data['trail_id'] = list(trail.values())[0]
            row_data['trail_name'] = list(trail.values())[1]
            row_data['url'] = list(trail.values())[2]

            values = tuple(row_data.values())
            #print('Values:', values)

            # Execute given statement using given parameters
            # Note: If the record is a duplicate, then the IGNORE keyword tells MySQL to discard it silently without generating an error
            cursor.execute('''
                INSERT IGNORE INTO trail_urls_ca (city_id, city, trail_id, trail_name, url)
                VALUES (%s, %s, %s, %s, %s)''', values)
            rowcount = cursor.rowcount

            if rowcount == 1:
                trails_added += 1
        print('Trails added:', trails_added)

        # Commit current transaction
        cnx.commit()


# # California Trail URLs
city_urls_list_sql_100 = city_urls_list_sql[0:100]
len(city_urls_list_sql_100)

# Get the trail URLs from cities
get_url_time_start = time.time()
get_trail_urls(city_urls_list_sql_100)
minutes = (time.time() - get_url_time_start)/60
print('Get Trail URL time: {} hrs: {} mins'.format(int(minutes // 60), round(minutes % 60)))


city_urls_list_sql_200 = city_urls_list_sql[100:200]
len(city_urls_list_sql_200)

# Get the trail URLs from cities
get_url_time_start = time.time()
get_trail_urls(city_urls_list_sql_200)
minutes = (time.time() - get_url_time_start)/60
print('Get Trail URL time: {} hrs: {} mins'.format(int(minutes // 60), round(minutes % 60)))


city_urls_list_sql_300 = city_urls_list_sql[200:300]
len(city_urls_list_sql_300)

# Get the trail URLs from cities
get_url_time_start = time.time()
get_trail_urls(city_urls_list_sql_300)
minutes = (time.time() - get_url_time_start)/60
print('Get Trail URL time: {} hrs: {} mins'.format(int(minutes // 60), round(minutes % 60)))


city_urls_list_sql_400 = city_urls_list_sql[300:400]
len(city_urls_list_sql_400)

# Get the trail URLs from cities
get_url_time_start = time.time()
get_trail_urls(city_urls_list_sql_400)
minutes = (time.time() - get_url_time_start)/60
print('Get Trail URL time: {} hrs: {} mins'.format(int(minutes // 60), round(minutes % 60)))


city_urls_list_sql_500 = city_urls_list_sql[400:500]
len(city_urls_list_sql_500)

# Get the trail URLs from cities
get_url_time_start = time.time()
get_trail_urls(city_urls_list_sql_500)
minutes = (time.time() - get_url_time_start)/60
print('Get Trail URL time: {} hrs: {} mins'.format(int(minutes // 60), round(minutes % 60)))


city_urls_list_sql_600 = city_urls_list_sql[500:600]
len(city_urls_list_sql_600)

# Get the trail URLs from cities
get_url_time_start = time.time()
get_trail_urls(city_urls_list_sql_600)
minutes = (time.time() - get_url_time_start)/60
print('Get Trail URL time: {} hrs: {} mins'.format(int(minutes // 60), round(minutes % 60)))


city_urls_list_sql_700 = city_urls_list_sql[600:700]
len(city_urls_list_sql_700)

# Get the trail URLs from cities
get_url_time_start = time.time()
get_trail_urls(city_urls_list_sql_700)
minutes = (time.time() - get_url_time_start)/60
print('Get Trail URL time: {} hrs: {} mins'.format(int(minutes // 60), round(minutes % 60)))


city_urls_list_sql_800 = city_urls_list_sql[700:800]
len(city_urls_list_sql_800)

# Get the trail URLs from cities
get_url_time_start = time.time()
get_trail_urls(city_urls_list_sql_800)
minutes = (time.time() - get_url_time_start)/60
print('Get Trail URL time: {} hrs: {} mins'.format(int(minutes // 60), round(minutes % 60)))


city_urls_list_sql_900 = city_urls_list_sql[800:900]
len(city_urls_list_sql_900)

# Get the trail URLs from cities
get_url_time_start = time.time()
get_trail_urls(city_urls_list_sql_900)
minutes = (time.time() - get_url_time_start)/60
print('Get Trail URL time: {} hrs: {} mins'.format(int(minutes // 60), round(minutes % 60)))


city_urls_list_sql_1000 = city_urls_list_sql[900:1000]
len(city_urls_list_sql_1000)

# Get the trail URLs from cities
get_url_time_start = time.time()
get_trail_urls(city_urls_list_sql_1000)
minutes = (time.time() - get_url_time_start)/60
print('Get Trail URL time: {} hrs: {} mins'.format(int(minutes // 60), round(minutes % 60)))


city_urls_list_sql_1100 = city_urls_list_sql[1000:1100]
len(city_urls_list_sql_1100)

# Get the trail URLs from cities
get_url_time_start = time.time()
get_trail_urls(city_urls_list_sql_1100)
minutes = (time.time() - get_url_time_start)/60
print('Get Trail URL time: {} hrs: {} mins'.format(int(minutes // 60), round(minutes % 60)))


city_urls_list_sql_1200 = city_urls_list_sql[1100:1200]
len(city_urls_list_sql_1200)

# Get the trail URLs from cities
get_url_time_start = time.time()
get_trail_urls(city_urls_list_sql_1200)
minutes = (time.time() - get_url_time_start)/60
print('Get Trail URL time: {} hrs: {} mins'.format(int(minutes // 60), round(minutes % 60)))


city_urls_list_sql_1300 = city_urls_list_sql[1200:1300]
len(city_urls_list_sql_1300)

# Get the trail URLs from cities
get_url_time_start = time.time()
get_trail_urls(city_urls_list_sql_1300)
minutes = (time.time() - get_url_time_start)/60
print('Get Trail URL time: {} hrs: {} mins'.format(int(minutes // 60), round(minutes % 60)))


sql = "SELECT COUNT(*) FROM trails.trail_urls_ca"
cursor.execute(sql)
result = cursor.fetchall()

for row in result:
    print(row[0])
