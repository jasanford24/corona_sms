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

TWO = 14 * 60 * 60
DAY = 24 * 60 * 60

URL = ('https://raw.githubusercontent.com/nytimes/'
       'covid-19-data/master/us-counties.csv')


def calculate_time():
    hour = (60 * 60) * localtime()[3]
    minute = 60 * localtime()[4]
    second = localtime()[5]
    time_to_sleep = TWO - (hour + minute + second)

    if time_to_sleep < 0:
        return DAY + time_to_sleep
    return time_to_sleep


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
    return yesterdata, prior, df


def main(test=True):
    if not test:
        sleep(calculate_time())

    data, prior, full = collect_data()

    engine = create_engine('sqlite:///corona-database.db')
    full.to_sql(name='cases', if_exists='replace', con=engine, index=False)

    with engine.connect() as connection:
        accounts = tuple(connection.execute('SELECT * FROM Accounts'))
    engine.dispose()

    # Loops through accounts one by one storing relevant data.
    logging.info('Creating individual data.')
    for account in accounts:
        recipient = Account(*account)
        recipient.set_data(data, prior)
        logging.info('Sending message to: ' + recipient.number)
        print(recipient) if test else recipient.send_sms()

    if not test:
        main(test=False)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1].lower() == "run":
            main(test=False)
        else:
            main()
    else:
        main()
