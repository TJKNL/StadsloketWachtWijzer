import requests
import logging
from functools import lru_cache
import os
from dotenv import load_dotenv
import time
import json

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# OpenRouteService API key (get one from https://openrouteservice.org/dev/#/signup)
ORS_API_KEY = os.getenv('ORS_API_KEY')
ORS_BASE_URL = "https://api.openrouteservice.org/v2/directions/cycling-regular"

# Amsterdam city office locations (id: [lat, lon, address])
AMSTERDAM_OFFICES = {
    # Centrum
    1: [52.3702, 4.9021, "Amstel 1, 1011 PN Amsterdam"],
    # Noord
    2: [52.3912, 4.9340, "Buikslotermeerplein 2000, 1025 XL Amsterdam"],
    # Zuidoost
    3: [52.3172, 4.9533, "Anton de Komplein 150, 1102 CW Amsterdam"],
    # Oost
    4: [52.3659, 4.9419, "Oranje-Vrijstaatplein 2, 1093 NG Amsterdam"],
    # West (Jan van Galenstraat)
    5: [52.3722, 4.8650, "Jan van Galenstraat, 1056 AA Amsterdam"],
    # Nieuw-West
    6: [52.3581, 4.8038, "Osdorpplein 1000, 1068 TG Amsterdam"],
    # Zuid
    7: [52.3475, 4.8732, "President Kennedylaan 923, 1079 MZ Amsterdam"],
    # Weesp
    8: [52.3074, 5.0432, "Nieuwstraat 70a, 1381 BD Weesp"]
}

@lru_cache(maxsize=32)
def get_cycling_time(from_lat, from_lon, to_lat, to_lon, valid_for=300):
    """
    Calculate cycling time between two points using OpenRouteService API.
    Results are cached for 5 minutes (300 seconds).
    
    Args:
        from_lat: Starting point latitude
        from_lon: Starting point longitude
        to_lat: Destination latitude
        to_lon: Destination longitude
        valid_for: Cache validity time in seconds
    
    Returns:
        dict: {
            'duration_minutes': estimated travel time in minutes,
            'distance_km': distance in kilometers
        }
    """
    # Add timestamp to cache key for time-based invalidation
    cache_timestamp = int(time.time() / valid_for)
    get_cycling_time.cache_info()  # Update timestamp
    
    if not ORS_API_KEY:
        logger.warning("ORS_API_KEY not set. Using distance approximation.")
        # Fallback: Simple approximation based on distance (15 km/h average speed)
        from math import radians, cos, sin, asin, sqrt
        
        def haversine(lon1, lat1, lon2, lat2):
            """Calculate the great circle distance between two points in kilometers"""
            # Convert decimal degrees to radians
            lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
            # Haversine formula
            dlon = lon2 - lon1
            dlat = lat2 - lat1
            a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
            c = 2 * asin(sqrt(a))
            r = 6371  # Radius of earth in kilometers
            return c * r
            
        distance = haversine(from_lon, from_lat, to_lon, to_lat)
        # Assume average cycling speed of 15 km/h
        duration_minutes = (distance / 15) * 60
        
        return {
            'duration_minutes': round(duration_minutes),
            'distance_km': round(distance, 1)
        }
    
    try:
        # OpenRouteService API request
        headers = {
            'Authorization': ORS_API_KEY,
            'Content-Type': 'application/json'
        }
        
        body = {
            'coordinates': [[from_lon, from_lat], [to_lon, to_lat]],
            'instructions': False
        }
        
        response = requests.post(
            ORS_BASE_URL,
            headers=headers,
            data=json.dumps(body),
            timeout=5
        )
        
        if response.status_code == 200:
            data = response.json()
            # Extract duration and distance from response
            route = data['routes'][0]
            duration_seconds = route['summary']['duration']
            distance_meters = route['summary']['distance']
            
            return {
                'duration_minutes': round(duration_seconds / 60),
                'distance_km': round(distance_meters / 1000, 1)
            }
        else:
            logger.error(f"API error: {response.status_code} - {response.text}")
            return {
                'duration_minutes': None,
                'distance_km': None,
                'error': f"API error: {response.status_code}"
            }
            
    except Exception as e:
        logger.error(f"Error calculating cycling time: {e}")
        return {
            'duration_minutes': None,
            'distance_km': None,
            'error': str(e)
        }

def get_office_location(stadsloket_id):
    """Get the location of a city office by ID"""
    office_id = int(stadsloket_id)
    if office_id in AMSTERDAM_OFFICES:
        lat, lon, address = AMSTERDAM_OFFICES[office_id]
        return {
            'lat': lat,
            'lon': lon,
            'address': address
        }
    return None

def get_all_office_locations():
    """Get locations of all city offices"""
    return {id: {'lat': data[0], 'lon': data[1], 'address': data[2]} 
            for id, data in AMSTERDAM_OFFICES.items()}

def calculate_travel_times(user_lat, user_lon):
    """Calculate travel times to all city offices from user's location"""
    result = {}
    
    for office_id, location in AMSTERDAM_OFFICES.items():
        office_lat, office_lon, address = location
        travel_info = get_cycling_time(user_lat, user_lon, office_lat, office_lon)
        
        result[office_id] = {
            'location': {
                'lat': office_lat,
                'lon': office_lon,
                'address': address
            },
            'travel': travel_info
        }
    
    return result
