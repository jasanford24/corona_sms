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
from corona_data_collection import collect_data

logging.basicConfig(filename='corona.log',
                    filemode='w',
                    format='%(asctime)s - %(message)s',
                    level=logging.INFO)


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


def main_db_parse(main_data, test=True):
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
        main(test=False)


def main(test=True):
    if not test:
        to_sleep(seven=True)

    main_data = collect_data(DATA_TYPES)

    if not test:
        to_sleep(seven=False)

    main_db_parse(main_data) if test else main_db_parse(main_data, test=False)


if __name__ == '__main__':
    try:
        if sys.argv[1].lower() == "run":
            main(test=False)
        else:
            main()
    except IndexError:
        main()
