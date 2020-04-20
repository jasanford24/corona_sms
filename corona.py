"""
US Covid-19 Statistic Webscraper
"""
import logging
from multiprocessing import Pool
from time import localtime, sleep
from datetime import date, timedelta

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from sqlalchemy import create_engine

from corona_accounts import Account, emergency

logging.basicConfig(filename='corona.log',
                    filemode='w',
                    format='%(asctime)s - %(message)s',
                    level=logging.INFO)

# For collection on Laptop
BROWSER = '/Applications/Brave Browser.app/Contents/MacOS/Brave Browser'
DRIVER = '/Users/noumenari/Documents/Python Projects/chromedriver'

# For collection on Raspberry Pi
# BROWSER = "/usr/bin/chromium-browser"
# DRIVER = "chromedriver"

MAIN_URL = 'https://coronavirus.1point3acres.com/en'
MAIN_XPATH_ROOT = '//*[@id="__next"]/div/div[9]'
COUNTY_DIV = 3

BAY_URL = 'https://projects.sfchronicle.com/2020/coronavirus-map/'
DATABASE = 'sqlite:///corona-database.db'

EIGHT = 20 * 60 * 60
DAY = 24 * 60 * 60
DATA_TYPES = {'state_cases': 'uint32',
              'state_deaths': 'uint32',
              'state_recovered': 'uint32',
              'county_cases': 'uint32',
              'county_deaths': 'uint32',
              'county_recovered': 'uint32',
              'state': 'string',
              'county': 'string',
              'date': 'string', }


class WebsiteChanged(Exception):
    pass


def fix_root_div(driver):
    xpath_list = list(MAIN_XPATH_ROOT)
    for div in range(1, 100):
        xpath_list[-2] = str(div)
        try:
            states = driver.find_elements_by_xpath(
                ''.join(xpath_list))[0].text.split('\n')
            if len(states) > 100:
                emergency(f'MAIN_XPATH_ROOT changed to {div}')
                return states, ''.join(xpath_list)
        except IndexError:
            emergency(f'MAIN_XPATH_ROOT changed to unknown.')
            raise WebsiteChanged('MAIN_XPATH_ROOT changed.')


def fix_county_div(driver):
    for div in range(1, 100):
        xpath = f'{MAIN_XPATH_ROOT}/div[{div}]/div/span[1]/div'
        try:
            if driver.find_element_by_xpath(xpath).text.strip() == 'New York':
                emergency(f'COUNTY_DIV changed to {div}')
                return div
        except NoSuchElementException:
            pass
    emergency(f'COUNTY_DIV changed to unknown.')
    raise WebsiteChanged('COUNTY_DIV changed.')


def create_driver():
    options = Options()
    options.headless = True
    options.add_argument('--incognito')
    options.binary_location = BROWSER
    driver_path = DRIVER
    return webdriver.Chrome(options=options, executable_path=driver_path)


def collect_main_data(_):
    """
    Collects data from: https://coronavirus.1point3acres.com/en
    and stores it in a pandas dataframe.
    """
    global MAIN_XPATH_ROOT
    global COUNTY_DIV
    logging.info('collect_main_data started.')
    with create_driver() as main_driver:
        main_driver.get(MAIN_URL)

        try:
            states = main_driver.find_elements_by_xpath(
                MAIN_XPATH_ROOT)[0].text.split('\n')
            if len(states) < 100:
                raise IndexError
        except IndexError:
            states, MAIN_XPATH_ROOT = fix_root_div(main_driver)

        state_data_list = [
            x.strip().replace(',', '') for x in states
            if x[0] != '+' and x[0] != '-' and x[-1] != '%'
        ]

        #  Starts collection when it reads "New York." First state on the list.
        for x in range(len(state_data_list)):
            if state_data_list[x] == 'New York':
                state_data_list = state_data_list[x:]
                break
        try:
            header = main_driver.find_elements_by_xpath(
                MAIN_XPATH_ROOT + '/header')[0].text.split('\n')
        except IndexError as err:
            logging.info(f'Website changed xpath: {MAIN_URL}')
            emergency(f'Website changed xpath: {MAIN_URL}')
            raise WebsiteChanged(err)

        for ind, col in enumerate(header):
            if col.lower() == 'fatality rate' or col.lower() == 'fatality':
                header.pop(ind)
        header_size = len(header)

        #  Stores collected state data in a Pandas dataframe
        state_data_df = pd.DataFrame([
            state_data_list[x:x + header_size:]
            for x in range(0, len(state_data_list), header_size)
        ],
            columns=header)

        #  Creates an empty dataframe for county data
        main_data = pd.DataFrame(columns=[
            'state', 'county', 'county_cases',
            'county_deaths', 'county_recovered'
        ])

        xpath = f'{MAIN_XPATH_ROOT}/div[{COUNTY_DIV}]/div/span[1]/div'
        try:
            if main_driver.find_element_by_xpath(
                    xpath).text.strip() != 'New York':
                COUNTY_DIV = fix_county_div(main_driver)
        except NoSuchElementException:
            COUNTY_DIV = fix_county_div(main_driver)

        #  Loops through each state on the website and stores county data
        for x in range(COUNTY_DIV, len(state_data_df) + COUNTY_DIV):

            # Clicks "Show More" button to show all county data
            drop_down = main_driver.find_element_by_xpath(
                f'{MAIN_XPATH_ROOT}/div[{x}]/div/span[1]')
            main_driver.execute_script('arguments[0].click();', drop_down)

            # Collects county data and transforms it.
            counties = main_driver.find_elements_by_xpath(
                f'{MAIN_XPATH_ROOT}/div[{x}]/div[2]')[0].text.split('\n')
            county_data_list = [
                y.strip().replace(',', '') for y in counties
                if y[0] != '+' and y[0] != '-' and y[-1] != '%'
            ]

            # Close county data to increase performance
            main_driver.execute_script('arguments[0].click();', drop_down)

            df_to_append = pd.DataFrame([
                county_data_list[y:y + header_size:]
                for y in range(0, len(county_data_list), header_size)
            ],
                columns=[
                'county', 'county_cases', 'county_deaths',
                'county_recovered'
            ])
            df_to_append['state'] = state_data_df['Location'][
                x - COUNTY_DIV]
            df_to_append['state_cases'] = state_data_df['Cases'][
                x - COUNTY_DIV]
            df_to_append['state_deaths'] = state_data_df['Deaths'][
                x - COUNTY_DIV]
            df_to_append['state_recovered'] = state_data_df['Recovered'][
                x - COUNTY_DIV]
            main_data = main_data.append(df_to_append)

    logging.info('collect_data finished.')
    return main_data


def collect_bay_area(_):
    """
    Collects data from: https://projects.sfchronicle.com/2020/coronavirus-map/
    Website provides a Bay Area count similar to how NYC is handled in the
    main data that is a more accurate representation of San Francisco.
    """
    logging.info('collect_bay_area beginning.')
    with create_driver() as bay_area_driver:
        bay_area_driver.get(BAY_URL)

        try:
            bay_area_data = bay_area_driver.find_elements_by_xpath(
                '//*[@id="gatsby-focus-wrapper"]/main/div[2]/section/div[1]'
            )[0].text.split('\n')
        except IndexError as err:
            logging.info(f'Website changed xpath: {BAY_URL}')
            emergency(f'Website changed xpath: {BAY_URL}')
            raise WebsiteChanged(err)

        # Slightly faster than list comprehension
        bay_area_data_parsed = list(
            map(lambda x: x.replace(',', ''),
                bay_area_data[1::3]))

    logging.info('collect_bay_area finished.')
    return bay_area_data_parsed


def add_bay_area(main_data, bay_area):
    logging.info('Adding Bay Area data to main dataframe.')
    temp_df = main_data[main_data['county'] ==
                        'San Francisco'].reset_index(drop=True)
    temp_df['county'] = 'Bay Area'
    temp_df['county_cases'] = bay_area[0]
    temp_df['county_deaths'] = bay_area[1]
    return temp_df


def calculate_time(seven=False):
    hour = (60 * 60) * localtime()[3]
    minute = 60 * localtime()[4]
    second = localtime()[5]
    time_to_sleep = EIGHT - (hour + minute + second)

    if time_to_sleep < 0 and seven:
        return DAY + time_to_sleep
    return time_to_sleep


def to_sleep(seven=False):
    if seven:
        logging.info('Sleeping until 7:58pmEST.')
        seven = calculate_time(True) - 120
        if seven > 0:
            sleep(seven)
    else:
        logging.info('Sleeping until 8:00pmEST.')
        eight = calculate_time()
        if eight > 0:
            sleep(eight)


def collect_data():
    logging.info('Beginning async multiprocessing.')
    with Pool() as p:
        data = p.map_async(collect_main_data, [1])
        bay = p.map_async(collect_bay_area, [1])
        p.close()
        p.join()
    logging.info('Async multiprocessing finished.')

    # collects data from multiprocess pool
    main_data = data.get()[0].reset_index(drop=True)
    bay_area = bay.get()[0]

    # Appends bay area data to main_data.
    main_data = main_data.append(
        add_bay_area(main_data, bay_area),
        ignore_index=True)
    main_data["date"] = str(date.today())

    main_data = main_data.astype(DATA_TYPES)

    return main_data


def main_db_parse(main_data, test=False):
    logging.info('Starting database engine.')
    engine = create_engine('sqlite:///corona-database.db')
    with engine.connect() as connection:
        accounts = tuple(connection.execute('SELECT * FROM Accounts'))

    # Read only previous day data from database
    sql = f'SELECT * FROM cases WHERE date="{date.today() - timedelta(1)}"'
    prior_data = pd.read_sql_query(sql, con=engine)
    prior_data = prior_data.astype(DATA_TYPES)

    # Loops through accounts one by one storing relevant data.
    logging.info('Beginning creation of individual data.')
    for account in accounts:
        recipient = Account(*account)
        recipient.set_data(main_data, prior_data)
        logging.info('Sending message to: ' + recipient.number)
        print(recipient) if test else recipient.send_sms()
    if not test:
        main_data.to_sql(name='cases', if_exists='append',
                         con=engine, index=False)

    # Closes database connection.
    engine.dispose()
    logging.info('Database engine closed.')
    if not test:
        main()


def main(test=False):
    if not test:
        to_sleep(seven=True)

    main_data = collect_data()

    if not test:
        to_sleep(seven=False)

    main_db_parse(main_data, test=True) if test else main_db_parse(main_data)


if __name__ == '__main__':
    main(test=True)
