import json
import googlemaps
# gmaps = googlemaps.Client(key='AIzaSyDXCGlMeZVAr_gz0NP-gdBO7xCxRkW809I')
gmaps = googlemaps.Client(key='AIzaSyDI97bhuph4FFcOyBS5c2CLf197qC1DnXw')

def get(address):
    return gmaps.geocode(address)

if __name__ == "__main__":
    geocode_result = gmaps.geocode('Schwarzspanierstrasse 15/1/2, 1090 Vienna')
    print json.dumps(geocode_result, indent=4)
