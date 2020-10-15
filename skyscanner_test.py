import os
import requests
import datetime
import json
from time import sleep
from kafka import KafkaProducer

API_KEY = os.getenv('API_SKYSCANNER')
ENDPOINT = "https://rapidapi.p.rapidapi.com/apiservices/browseroutes/v1.0/US/USD/en-US/"
#DESTINATION_AIRPORTS_US = ["LAX-sky", "ORD-sky", "DFW-sky", "DEN-sky", "JFK-sky", "SFO-sky", "SEA-sky", "LAS-sky"]
DESTINATION_AIRPORTS_US = ["LAX-sky", "ORD-sky"]

#Return date used as departure date in the query. 
#Parameter "day_delta" -> day offset from todays date
def date(day_delta):
    date = datetime.datetime.today() + datetime.timedelta(days=day_delta)
    return datetime.datetime.strftime(date, '%Y-%m-%d')

#Return time in the same format as Skyscanners API
def time_now():
    time = datetime.datetime.now()
    return datetime.datetime.strftime(time, '%Y-%m-%dT%H:%M:%S')

def days_difference(start_date, end_date):
    try:
        start_date  = datetime.datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S')
        end_date  = datetime.datetime.strptime(end_date,'%Y-%m-%dT%H:%M:%S' )
        days_between = end_date - start_date
        #print(days_between)
        return days_between
    except:
        print("Failed to calculate days between")


#Parse endpoint for browsing flight prices. Origin place is hardcoded to Arlanda.
def get_endpoint(origin, destination, days_offset):  
    return ENDPOINT + origin + '/' + destination + '/' + date(days_offset)
def carrier_id_to_name(carrier_list, id):
    for carrier_dict in carrier_list:
        if carrier_dict['CarrierId'] == id:
            return carrier_dict['Name']
    return id

def get_flight_info(origin="ATL-sky", destination="JFK-sky", days_offset=0):
    url = get_endpoint(origin, destination, days_offset)
    headers = {
        'x-rapidapi-host': "skyscanner-skyscanner-flight-search-v1.p.rapidapi.com",
        'x-rapidapi-key': API_KEY,
        'accept' : "application/json",
        }
    response = requests.request("GET", url, headers=headers)
    return response.text

def main():
    #producer = KafkaProducer(bootstrap_servers='localhost:1234')
    origin = "ATL-sky"
    for destination in DESTINATION_AIRPORTS_US:
        response = get_flight_info(origin=origin, destination=destination)
        quotes_json = json.loads(response)
        #print(quotes_json['Quotes'])
        quotes = quotes_json['Quotes']
        carriers = quotes_json['Carriers']
        #print(carriers)
        time = time_now()
        print(f"\n\nFLIGHTS FROM {origin} TO {destination}\n")
        for quote in quotes:
            #departure_date = datetime.datetime.strptime(quote['OutboundLeg']['DepartureDate'],'%Y-%m-%dT%H:%M:%S')
            departure_date = quote['OutboundLeg']['DepartureDate']
            time_to_departure = days_difference(time, departure_date)
            min_price = quote['MinPrice']
            print("Date: " + departure_date)
            print("Lowest price: " + str(min_price))
            print("Carrier: " + carrier_id_to_name(carriers, quote['OutboundLeg']['CarrierIds'][0]))
            print("Days to departure: " + str(time_to_departure) + '\n')            
        sleep(5)

if __name__ == "__main__":
    main()