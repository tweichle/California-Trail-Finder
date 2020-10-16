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

# Drop table if exists
sql = 'DROP TABLE IF EXISTS city_urls'
cursor.execute(sql)

# Store CREATE statements in Python dictionary TABLES
TABLES = {}

TABLES['city_urls'] = (
    "CREATE TABLE `city_urls` ("
    "  `state` VARCHAR(15) NOT NULL,"
    "  `city_id` INT(11) NOT NULL,"
    "  `city` VARCHAR(50) NULL,"
    "  `url` VARCHAR(255) NOT NULL,"
    "  PRIMARY KEY (`city_id`)"
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


def get_city_urls(states_for_url):

    options = webdriver.ChromeOptions()
    options.add_argument('start-maximized')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)

    for st in states_for_url:
        base = 'https://www.trailwebsite.com/'
        if st == 'hawaii':
            path = f'{st}/cities'
        else:
            path = f'us/{st}/cities'

        url = urllib.parse.urljoin(base, path)

        # Creates a new ChromeDriver instance of the chrome driver
        driver = webdriver.Chrome()
        driver.get(url)
        soup = BeautifulSoup(driver.page_source)

        while True:
            try:
                driver.execute_script('return arguments[0].scrollIntoView(true);', WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="stick-bar-parent"]/div/div[3]/div/button'))))
                time.sleep(5)
                WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="stick-bar-parent"]/div/div[3]/div/button'))).click()
            except TimeoutException:
                break

        city_ids = [i.get_attribute('data-id') for i in driver.find_elements_by_css_selector('div.styles-module__trailCard___2oHiP')]
        cities = [i.get_attribute('title') for i in driver.find_elements_by_css_selector('div.styles-module__trailCard___2oHiP > a')]
        urls = [i.get_attribute('href') for i in driver.find_elements_by_css_selector('div.styles-module__trailCard___2oHiP > a')]

        city_list = [{'city_id': city_id, 'city': city, 'url': url} for city_id, city, url in zip(city_ids, cities, urls)]

        driver.quit()

        print('\nState: {}; Cities: {}'.format(string.capwords(st.replace('-', ' ')).replace('Dc', 'DC'), len(urls)))

        cities_added = 0
        for city in city_list:
            row_data = {}
            row_data['state'] = string.capwords(st.replace('-', ' ')).replace('Dc', 'DC')
            row_data['city_id'] = list(city.values())[0]
            row_data['city'] = list(city.values())[1]
            row_data['url'] = list(city.values())[2]

            values = tuple(row_data.values())
            #print('Values:', values)

            # Execute given statement using given parameters
            # Note: If the record is a duplicate, then the IGNORE keyword tells MySQL to discard it silently without generating an error
            cursor.execute('''
                INSERT IGNORE INTO city_urls (state, city_id, city, url)
                VALUES (%s, %s, %s, %s)''', values)
            rowcount = cursor.rowcount

            if rowcount == 1:
                cities_added += 1
        print('Cities added:', cities_added)

        # Commit current transaction
        cnx.commit()


# states_for_url = ['rhode-island'] # for testing
# # Get the city URLs from states
# get_url_time_start = time.time()
# get_city_urls(states_for_url)
# minutes = (time.time() - get_url_time_start)/60
# print('Get City URL time: {} hrs: {} mins'.format(int(minutes // 60), round(minutes % 60)))

states_for_url = ['california']
# Get the city URLs from states
get_url_time_start = time.time()
get_city_urls(states_for_url)
minutes = (time.time() - get_url_time_start)/60
print('Get City URL time: {} hrs: {} mins'.format(int(minutes // 60), round(minutes % 60)))


sql = "SELECT COUNT(*) FROM trails.city_urls"
cursor.execute(sql)
result = cursor.fetchall()

for row in result:
    print(row[0])


sql = "SELECT COUNT(*) FROM trails.city_urls where state = 'California'"
cursor.execute(sql)
result = cursor.fetchall()

for row in result:
    print(row[0])
