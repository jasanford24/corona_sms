"""
US Covid-19 Statistic Webscraper
"""
import logging
import sys
from time import localtime, sleep
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import create_engine

from corona_accounts import Account

logging.basicConfig(filename='corona.log',
                    filemode='w',
                    format='%(asctime)s - %(message)s',
                    level=logging.INFO)


EIGHT = 20 * 60 * 60
DAY = 24 * 60 * 60
URL = ('https://raw.githubusercontent.com/nytimes/'
       'covid-19-data/master/us-counties.csv')


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
    logging.info('Collecting data.')
    df = pd.read_csv(URL)
    yesterdata = df[df['date'] ==
                    f'{date.today()-timedelta(1)}'].reset_index(drop=True)
    prior = df[df['date'] ==
               f'{date.today()-timedelta(2)}'].reset_index(drop=True)
    if not len(yesterdata):
        logging.info('Sleeping for 2 hours.')
        sleep(3600 - localtime()[5])
        collect_data()
    logging.info('Data collected.')
    return yesterdata, prior


def main_db_parse(test=True):
    logging.info('Starting database engine.')
    engine = create_engine('sqlite:///corona-database.db')
    with engine.connect() as connection:
        accounts = tuple(connection.execute('SELECT * FROM Accounts'))

    logging.info('Database engine closed.')
    engine.dispose()

    data, prior = collect_data()

    # Loops through accounts one by one storing relevant data.
    logging.info('Beginning creation of individual data.')
    for account in accounts:
        recipient = Account(*account)
        recipient.set_data(data, prior)
        logging.info('Sending message to: ' + recipient.number)
        print(recipient) if test else recipient.send_sms()

    # Closes database connection.
    if not test:
        main(test=False)


def main(test=True):
    main_db_parse() if test else main_db_parse(test=False)


if __name__ == '__main__':
    try:
        if sys.argv[1].lower() == "run":
            main(test=False)
        else:
            main()
    except IndexError:
        main()
