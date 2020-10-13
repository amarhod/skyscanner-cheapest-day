import os
import requests
import datetime
import json
from kafka import KafkaProducer

API_KEY = os.getenv('API_SKYSCANNER')
ENDPOINT = "https://rapidapi.p.rapidapi.com/apiservices/browseroutes/v1.0/SE/SEK/en-US/ARN-sky/"



def todays_date():
    date = datetime.datetime.today()
    return datetime.datetime.strftime(date, '%Y-%m-%d')

def todays_time():
    time = datetime.datetime.now()
    return datetime.datetime.strftime(time, '%Y-%m-%dT%H:%M:%S')

def get_endpoint(destination="JFK-sky"):  
    return ENDPOINT + destination + '/' + todays_date()

def carrier_id_to_name(carrier_list, id):
    for carrier_dict in carrier_list:
        if carrier_dict['CarrierId'] == id:
            return carrier_dict['Name']
    return id


def main():
    #producer = KafkaProducer(bootstrap_servers='localhost:1234')
    url = get_endpoint()
    headers = {
        'x-rapidapi-host': "skyscanner-skyscanner-flight-search-v1.p.rapidapi.com",
        'x-rapidapi-key': API_KEY
        }
    response = requests.request("GET", url, headers=headers)

    quotes_json = json.loads(response.text)
    #print(quotes_json['Quotes'])
    quotes = quotes_json['Quotes']
    carriers = quotes_json['Carriers']
    #print(carriers)
    time_now = todays_time()
    for quote in quotes:
        print("Min price: " + str(quote['MinPrice']))
        print("Carrier: " + carrier_id_to_name(carriers, quote['OutboundLeg']['CarrierIds'][0]))
        print("Date: " + str(quote['OutboundLeg']['DepartureDate']).replace('T', ' '))
        print("Time to departure (min): " + str((datetime.datetime.strptime(quote['OutboundLeg']['DepartureDate'],'%Y-%m-%dT%H:%M:%S') 
                                            - datetime.datetime.strptime(time_now,'%Y-%m-%dT%H:%M:%S')).total_seconds()*60.0)+'\n')

if __name__ == "__main__":
    main()