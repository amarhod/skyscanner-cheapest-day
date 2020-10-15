import os
import requests
import datetime
import json
from time import sleep
from kafka import KafkaProducer

API_KEY = os.getenv('API_SKYSCANNER')
ENDPOINT = 'https://rapidapi.p.rapidapi.com/apiservices/browseroutes/v1.0/'
#DESTINATION_AIRPORTS_US = ['LAX-sky', 'ORD-sky', 'DFW-sky', 'DEN-sky', 'JFK-sky', 'SFO-sky', 'SEA-sky', 'LAS-sky']
DESTINATION_AIRPORTS_US = ['LAX-sky', 'ORD-sky']

#Return date used as departure date in the query. 
#Parameter 'day_delta' -> day offset from todays date
def date(day_delta):
    date = datetime.datetime.today() + datetime.timedelta(days=day_delta)
    return datetime.datetime.strftime(date, '%Y-%m-%d')

#Return time in the same format as Skyscanners API
def time_now():
    time = datetime.datetime.now()
    return datetime.datetime.strftime(time, '%Y-%m-%dT%H:%M:%S')

def days_delta(start_date, end_date):
    try:
        start_date  = datetime.datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S')
        end_date  = datetime.datetime.strptime(end_date,'%Y-%m-%dT%H:%M:%S' ) + datetime.timedelta(days=1)
        days_between = end_date - start_date
        return days_between.days
    except:
        print('Failed to calculate days between')
        return -1


#Parse endpoint for browsing flight prices. Origin place is hardcoded to Arlanda.
def get_endpoint(market, currency, locale, origin, destination, days_offset):  
    return ENDPOINT + market + '/' + currency + '/' + locale + '/' + origin + '/' + destination + '/' + date(days_offset)

def carrier_id_to_name(carrier_list, id):
    for carrier_dict in carrier_list:
        if carrier_dict['CarrierId'] == id:
            return carrier_dict['Name']
    return 'NULL'

def get_flight_info(market='US', currency='USD', locale='en-US', origin='ATL-sky', destination='JFK-sky', days_offset=0):
    try:
        url = get_endpoint(market, currency, locale, origin, destination, days_offset)
        headers = {
            'x-rapidapi-host': 'skyscanner-skyscanner-flight-search-v1.p.rapidapi.com',
            'x-rapidapi-key': API_KEY,
            'accept' : 'application/json',
            }
        response = requests.request('GET', url, headers=headers)
        return response.text
    except:
        print('API call failed')
        return 'FAILED'

def get_route_messages():
    origin = 'ATL-sky'
    for destination in DESTINATION_AIRPORTS_US:
        response = get_flight_info(origin=origin, destination=destination)
        if(response == 'FAILED'):
            continue
        quotes_json = json.loads(response)
        quotes = quotes_json['Quotes']
        carriers = quotes_json['Carriers']
        time = time_now()
        list_of_route_messages = []
        print(f'\nFLIGHTS FROM {origin} TO {destination}\n')

        for quote in quotes:
            message = {}
            if quote['Direct']:
                departure_date = quote['OutboundLeg']['DepartureDate']
                days_to_departure = days_delta(time, departure_date)
                min_price = quote['MinPrice']
                message['days_to_departure'] = days_to_departure
                message['min_price'] = min_price
                message['carrier'] = carrier_id_to_name(carriers, quote['OutboundLeg']['CarrierIds'][0])      
                message['query_time'] = time
                list_of_route_messages.append(message)
        sleep(5)
    return list_of_route_messages


def main():
    origin = 'ATL-sky'
    for destination in DESTINATION_AIRPORTS_US:
        response = get_flight_info(origin=origin, destination=destination)
        if(response == 'FAILED'):
            continue
        quotes_json = json.loads(response)
        quotes = quotes_json['Quotes']
        carriers = quotes_json['Carriers']
        time = time_now()
        list_of_route_messages = []
        print(f'\n\nFLIGHTS FROM {origin} TO {destination}\n')
        for quote in quotes:
            if quote['Direct']:
                departure_date = quote['OutboundLeg']['DepartureDate']
                time_to_departure = days_delta(time, departure_date)
                min_price = quote['MinPrice']
                print('Date: ' + departure_date)
                print('Lowest price: ' + str(min_price))
                print('Carrier: ' + carrier_id_to_name(carriers, quote['OutboundLeg']['CarrierIds'][0]))
                print('Days to departure: ' + str(time_to_departure) + '\n')            
        sleep(5)        
        

if __name__ == '__main__':
    main()