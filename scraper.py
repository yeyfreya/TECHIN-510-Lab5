import os
import re
import json
import datetime
from zoneinfo import ZoneInfo
import html
import requests

from db import get_db_conn

URL = 'https://visitseattle.org/events/page/'
URL_LIST_FILE = './data/links.json'
URL_DETAIL_FILE = './data/data.json'

# Default Seattle coordinates
SEATTLE_LAT = '47.6062'
SEATTLE_LON = '-122.3321'

def list_links():
    res = requests.get(URL + '1/')
    last_page_no = int(re.findall(r'bpn-last-page-link"><a href=".+?/page/(\d+?)/.+" title="Navigate to last page">', res.text)[0])

    links = []
    for page_no in range(1, last_page_no + 1):
        res = requests.get(URL + str(page_no) + '/')
        links.extend(re.findall(r'<h3 class="event-title"><a href="(https://visitseattle.org/events/.+?/)" title=".+?">.+?</a></h3>', res.text))

    if not os.path.exists(os.path.dirname(URL_LIST_FILE)):
        os.makedirs(os.path.dirname(URL_LIST_FILE))
    
    json.dump(links, open(URL_LIST_FILE, 'w'))

def get_geolocation(location_name):
    headers = {'User-Agent': 'Mozilla/5.0'}
    params = {'q': location_name, 'format': 'json'}
    res = requests.get("https://nominatim.openstreetmap.org/search.php", headers=headers, params=params)
    location_data = res.json()
    if location_data:
        return {'latitude': location_data[0]['lat'], 'longitude': location_data[0]['lon']}
    else:
        # Return default Seattle coordinates if geolocation fails
        return {'latitude': SEATTLE_LAT, 'longitude': SEATTLE_LON}

def get_weather_details(lat, lon):
    if lat is None or lon is None:
        lat, lon = SEATTLE_LAT, SEATTLE_LON  # Use Seattle's coordinates if any are None
    weather_url = f"https://api.weather.gov/points/{lat},{lon}"
    weather_res = requests.get(weather_url)
    weather_data = weather_res.json()
    forecast_url = weather_data['properties']['forecast']
    forecast_res = requests.get(forecast_url)
    forecast_data = forecast_res.json()

    forecast_today = forecast_data['properties']['periods'][0]
    return {
        'condition': forecast_today['shortForecast'],
        'temperature': forecast_today['temperature'],
        'windSpeed': forecast_today.get('windSpeed', None)
    }

def get_detail_page():
    links = json.load(open(URL_LIST_FILE, 'r'))
    data = []
    for link in links:
        try:
            row = {}
            res = requests.get(link)
            row['title'] = html.unescape(re.findall(r'<h1 class="page-title" itemprop="headline">(.+?)</h1>', res.text)[0])
            datetime_venue = re.findall(r'<h4><span>.*?(\d{1,2}/\d{1,2}/\d{4})</span> \| <span>(.+?)</span></h4>', res.text)[0]
            row['date'] = datetime.datetime.strptime(datetime_venue[0], '%m/%d/%Y').replace(tzinfo=ZoneInfo('America/Los_Angeles')).isoformat()
            row['venue'] = datetime_venue[1].strip()
            metas = re.findall(r'<a href=".+?" class="button big medium black category">(.+?)</a>', res.text)
            row['category'] = html.unescape(metas[0])
            row['location'] = metas[1]
            
            geolocation = get_geolocation(row['venue'] + ', ' + row['location'] + ', Seattle')
            row['geolocation'] = geolocation
            
            weather_details = get_weather_details(geolocation['latitude'], geolocation['longitude'])
            row['weather'] = weather_details
            
            data.append(row)
        except IndexError as e:
            print(f'Error: {e}')
            print(f'Link: {link}')
        except KeyError as e:
            print(f'Weather API KeyError: {e}')
            print(f'Link: {link}')
    json.dump(data, open(URL_DETAIL_FILE, 'w'))

def insert_to_pg():
    q = '''
    CREATE TABLE IF NOT EXISTS events (
        url TEXT PRIMARY KEY,
        title TEXT,
        date TIMESTAMP WITH TIME ZONE,
        venue TEXT,
        category TEXT,
        location TEXT,
        latitude NUMERIC(10, 7),
        longitude NUMERIC(10, 7),
        weather_condition TEXT,
        temperature INTEGER,
        windSpeed TEXT 
    );
    '''
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(q)
    
    urls = json.load(open(URL_LIST_FILE, 'r'))
    data = json.load(open(URL_DETAIL_FILE, 'r'))



    for url, row in zip(urls, data):
        weather = row.get('weather', {})
        weather_condition = weather.get('condition', None)
        temperature = weather.get('temperature', None)
        wind_speed = weather.get('windSpeed', None)

        q = '''
        INSERT INTO events (url, title, date, venue, category, location, latitude, longitude, weather_condition, temperature, windSpeed)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (url) DO NOTHING;
        '''
        cur.execute(q, (
            url, 
            row['title'], 
            row['date'], 
            row['venue'], 
            row['category'], 
            row['location'],
            row.get('geolocation', {}).get('latitude', None), 
            row.get('geolocation', {}).get('longitude', None), 
            weather_condition, 
            temperature, 
            wind_speed 
        ))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    list_links()
    get_detail_page()
    insert_to_pg()
    print("Data inserted successfully!")
