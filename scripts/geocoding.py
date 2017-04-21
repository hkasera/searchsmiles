import json
import googlemaps

gmaps = googlemaps.Client(key='KEY')

def get(address):
    return gmaps.geocode(address)

if __name__ == "__main__":
    geocode_result = gmaps.geocode('Schwarzspanierstrasse 15/1/2, 1090 Vienna')
    print json.dumps(geocode_result, indent=4)
