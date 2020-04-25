from os import environ
from twilio.rest import Client

twilioCli = Client(environ.get('TWILIO_USER'),
                   environ.get('TWILIO_PASS'))
TWIL_NUMB = environ.get('TWILIO_NUMBER')
MY_NUMB = environ.get('MY_NUMBER')


class Account:
    def __init__(self, number, state, county):
        self.number = number
        self.state = state
        self.county = county

    def set_data(self, data, prior):
        self.date = data['date'][0]
        self.total_cases = sum(data['cases'])
        self.total_deaths = sum(data['deaths'])
        self.total_new_deaths = self.total_deaths - sum(prior['deaths'])

        self.state_case_count = sum(data[data['state'] == self.state]['cases'])
        self.state_death_count = sum(
            data[data['state'] == self.state]['deaths'])
        self.state_new_deaths = self.state_death_count - \
            sum(prior[prior['state'] == self.state]['deaths'])

        self.county_case_count = sum(
            data[(data['state'] == self.state) &
                 (data['county'] == self.county)]['cases'])
        self.county_death_count = sum(
            data[(data['state'] == self.state) &
                 (data['county'] == self.county)]['deaths'])
        self.county_new_deaths = self.county_death_count - \
            sum(prior[(prior['state'] == self.state) & (
                prior['county'] == self.county)]['deaths'])
        self._build_message()

    def _build_message(self):
        message = 'US Covid-19'
        message += f'\n{self.date}'
        message += f'\nCases: {self.total_cases:,}'
        message += f'\nDeaths: {self.total_deaths:,}'

        if self.total_new_deaths:
            message += f' (+{self.total_new_deaths:,})'

        message += f'\n{self.state}'
        message += f'\nCases: {self.state_case_count:,}'
        message += f'\nDeaths: {self.state_death_count:,}'

        if self.state_new_deaths:
            message += f' (+{self.state_new_deaths:,})'

        message += f'\n{self.county}'
        message += f'\nCases: {self.county_case_count:,}'
        message += f'\nDeaths: {self.county_death_count:,}'

        if self.county_new_deaths:
            message += f' (+{self.county_new_deaths:,})'
        self.message = message

    def send_sms(self, message=False):
        if message:
            twilioCli.messages.create(body=message,
                                      from_=TWIL_NUMB,
                                      to=self.number)
        else:
            twilioCli.messages.create(body=self.message,
                                      from_=TWIL_NUMB,
                                      to=self.number)

    def __repr__(self):
        return self.message + '\n'


# Threw this in here since I already call the login credentials.
def emergency(error):
    twilioCli.messages.create(body=error,
                              from_=TWIL_NUMB,
                              to=MY_NUMB)
