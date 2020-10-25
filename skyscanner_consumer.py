import os
import requests
import datetime
import json
from time import sleep
from kafka import KafkaProducer

API_KEY = os.getenv('API_SKYSCANNER')
ENDPOINT = 'https://rapidapi.p.rapidapi.com/apiservices/browseroutes/v1.0/'
ORIGIN = 'ATL-sky'
DESTINATION_AIRPORTS_US = ['LAX-sky', 'ORD-sky', 'DFW-sky', 'DEN-sky', 'JFK-sky', 'SFO-sky', 'SEA-sky', 'LAS-sky']

#Return date used as departure date in the query. 
#Parameter 'day_delta' -> day offset from todays date
def date(months_delta=0):
    date = datetime.datetime.today() + datetime.timedelta(weeks=4*months_delta)
    return datetime.datetime.strftime(date, '%Y-%m-%d')

#Return time in the same format as Skyscanners API
def time_now():
    time = datetime.datetime.now()
    return datetime.datetime.strftime(time, '%Y-%m-%dT%H:%M:%S')

def days_between(start_date, end_date):
    try:
        start_date  = datetime.datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S')
        end_date  = datetime.datetime.strptime(end_date,'%Y-%m-%dT%H:%M:%S' ) + datetime.timedelta(days=1)
        days_between = end_date - start_date
        return days_between.days
    except:
        print('Failed to calculate days between')
        return -1


#Parse endpoint for browsing flight prices. Origin place is hardcoded to Arlanda.
def get_endpoint(market, currency, locale, origin, destination, months_offset):  
    return ENDPOINT + market + '/' + currency + '/' + locale + '/' + origin + '/' + destination + '/' + date(months_offset)

def carrier_id_to_name(carrier_list, id):
    for carrier_dict in carrier_list:
        if carrier_dict['CarrierId'] == id:
            return carrier_dict['Name']
    return 'NULL'

def get_flight_info(market='US', currency='USD', locale='en-US', origin='ATL-sky', destination='JFK-sky', months_offset=0):
    try:
        url = get_endpoint(market, currency, locale, origin, destination, months_offset)
        headers = {
            'x-rapidapi-host': 'skyscanner-skyscanner-flight-search-v1.p.rapidapi.com',
            'x-rapidapi-key': API_KEY,
            'accept' : 'application/json',
            }
        response = requests.request('GET', url, headers=headers)
        return response.text
    except:
        print('API call failed')
        return None

def get_route_messages(months_offset=0):
    origin = ORIGIN
    list_of_route_messages = []
    for destination in DESTINATION_AIRPORTS_US:
        response = get_flight_info(origin=origin, destination=destination, months_offset=months_offset)
        if(response == None):
            continue
        quotes_json = json.loads(response)
        quotes = quotes_json['Quotes']
        carriers = quotes_json['Carriers']
        time = time_now()
        for quote in quotes:
            message = {}
            if quote['Direct']:
                departure_date = quote['OutboundLeg']['DepartureDate']
                days_to_departure = days_between(time, departure_date)
                min_price = float(quote['MinPrice'])
                message['query_date'] = time.split('T')[0]
                message['days_to_departure'] = days_to_departure
                message['origin'] = origin
                message['destination'] = destination
                message['min_price'] = min_price
                message['unique_count'] = 1
                message['cached_timestamp'] = quote['QuoteDateTime']
                list_of_route_messages.append(message)
        sleep(2)
    return list_of_route_messages

def main():
    list_of_routes = get_route_messages()
    for route in list_of_routes:
        print(route)      


if __name__ == '__main__':
    main()