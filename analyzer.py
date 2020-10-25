from collections import Counter
from cassandra.cluster import Cluster

DAYS_TIMEFRAME = 30
KEYSPACE = "skyscanner_test"
ORIGIN = 'ATL-sky'
DESTINATION_AIRPORTS_US = ['LAX-sky', 'ORD-sky', 'DFW-sky', 'DEN-sky', 'JFK-sky', 'SFO-sky', 'SEA-sky', 'LAS-sky']

def query_cassandra(session, origin, destination):
    query = session.execute(
        """SELECT * FROM quotes WHERE origin=%s AND destination=%s 
        ALLOW FILTERING"""
        ,(origin, destination))
    return query

def calculate_avarage_price(rows):
    count = 0
    sum = 0.0
    for row in rows:
        sum += row.min_price
        count += 1
    return (sum/count, count)

def cheapest_day_route(route_prices):
    if(len(route_prices) == 0):
            return None
    cheapest_day = route_prices[0]
    unique_route_count = 0
    for day_price in route_prices:
        unique_route_count += day_price[2]
        if(day_price[1] < cheapest_day[1]):
            cheapest_day = day_price
    cheapest_day = (cheapest_day[0], cheapest_day[1], unique_route_count)
    return cheapest_day


#From a list of lists (where each list is a specific route wi)
def cheapest_day_global(routes):
    best_day_contenders = []
    max_day = 0
    best_day = 0
    total_routes = 0
    for route_prices in routes:
        cheapest_day = cheapest_day_route(route_prices)
        if(cheapest_day != None):
            best_day_contenders.append(cheapest_day[0])
            total_routes += cheapest_day[2]
    if(len(best_day_contenders) == 0):
        print("Failed to find the best day")
        return None
    #print("Best DBD (days before departure) contenders: ", best_day_contenders)
    counter = Counter(best_day_contenders)
    best_day = counter.most_common(1)[0][0]
    return (best_day, total_routes) 


def print_best_time():
    cluster = Cluster()
    session = cluster.connect(KEYSPACE)
    best_routes = []
    #Query Cassandra for each route
    for destination in DESTINATION_AIRPORTS_US:
        routes_avg_price = []
        rows = query_cassandra(session, ORIGIN, destination)
        row_list = []
        for row in rows:
            row_list.append(row)
        #Create a list for each day_to_departure within the timeframe desired
        for day in range(1, DAYS_TIMEFRAME+1):
            same_departure_day = [flight for flight in row_list if flight.days_to_departure == day]
            if(len(same_departure_day) == 0):
                continue
            avg_price = calculate_avarage_price(same_departure_day)
            routes_avg_price.append((day, avg_price[0], avg_price[1]))
        best_routes.append(routes_avg_price)
    ideal_day = cheapest_day_global(best_routes)

    if(ideal_day != None):
        print("Based on " + str(ideal_day[1]) + " unique routes and dates collected,")
        print("the best day to buy a ticket before departure is: " + str(ideal_day[0]) + " days before.")


if __name__ =="__main__":
    print_best_time()