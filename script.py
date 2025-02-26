import paramiko
import json
import pandas as pd
import time
import requests
import math
import sys
from dotenv import load_dotenv
import os

# Carregar variables del fitxer .env
load_dotenv()

# Variables de configuració
hostname = os.getenv("HOSTNAME")
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")
remote_file = os.getenv("REMOTE_FILE")
airports_csv = os.getenv("AIRPORTS_CSV")


def fetch_aircraft_data():
    """Reutilitza SSH per llegir l'arxiu remot aircraft.json i processa les dades."""
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname, username=username, password=password)
        sftp = ssh.open_sftp()
        with sftp.open(remote_file, 'r') as f:
            data = json.load(f)
        sftp.close()
        ssh.close()
    except Exception as e:
        print("Error en obtenir les dades:", e)
        return None

    aircraft_list = data.get("aircraft", [])
    if not aircraft_list:
        print("No s'ha detectat cap avió")
        return None

    # Filtrar avions amb dades vàlides
    filtered = [
        ac for ac in aircraft_list 
        if (ac.get("lat") is not None and ac.get("lon") is not None and 
            ac.get("altitude") is not None and ac.get("speed") is not None and
            ac.get("track") is not None)
    ]
    if not filtered:
        print("No s'ha detectat cap avió amb dades vàlides")
        return None

    df = pd.DataFrame(filtered)
    df = df[['hex', 'lat', 'lon', 'speed', 'track']].rename(columns={
        'hex': 'icao24',
        'lat': 'latitude',
        'lon': 'longitude'
    })
    # Afegir columnes addicionals
    df['icao_airport'] = None
    df['city'] = None
    df['country'] = None

    # Consultar l'API per obtenir l'estimated departure airport per cada avió
    df['icao_airport'] = df['icao24'].apply(get_est_departure_airport)
    
    # Fer el merge amb airports.csv per obtenir ciutat i país
    try:
        airports_df = pd.read_csv(airports_csv)
        df = df.merge(airports_df[['ICAO', 'City', 'Country']], left_on='icao_airport', right_on='ICAO', how='left')
        df = df.rename(columns={'City': 'city', 'Country': 'country'}).drop(columns=['ICAO'])
    except Exception as e:
        print("Error en llegir o processar l'arxiu airports.csv:", e)
    
    return df

def get_est_departure_airport(icao24):
    """Consulta l'API d'OpenSky per obtenir l'aeroport de sortida estimat."""
    # Actualitzar temps en cada trucada
    now = int(time.time())
    now_1hago = now - 20000
    url = f"https://opensky-network.org/api/flights/aircraft?icao24={icao24}&begin={now_1hago}&end={now}"
    try:
        time.sleep(1)  # Esperar 1 segon per evitar massa peticions
        response = requests.get(url, timeout=10)
        flights = response.json()
        if isinstance(flights, list) and flights:
            return flights[0].get('estDepartureAirport', None)
    except Exception as e:
        print(f"Error per {icao24}: {e}")
    return None

def update_positions(df):
    """
    Actualitza la latitud i longitud de cada avió per un desplaçament 
    corresponent a 1 segon, tenint en compte la velocitat (en nusos) i el track.
    """
    updated_latitudes = []
    updated_longitudes = []
    
    for index, row in df.iterrows():
        # Convertir la velocitat de nusos a m/s (1 knot ≈ 0.514444 m/s)
        speed_m_s = row['speed'] * 0.514444
        # Convertir el track (rumb) a radians
        track_rad = math.radians(row['track'])
        # Desplaçament en 1 segon (en metres)
        displacement = speed_m_s
        
        # Delta latitud: 1 grau de latitud ≈ 111320 metres
        delta_lat = displacement * math.cos(track_rad) / 111320
        # Delta longitud: tenir en compte la latitud actual
        delta_lon = displacement * math.sin(track_rad) / (111320 * math.cos(math.radians(row['latitude'])))
        
        new_lat = row['latitude'] + delta_lat
        new_lon = row['longitude'] + delta_lon
        
        updated_latitudes.append(new_lat)
        updated_longitudes.append(new_lon)
    
    df['latitude'] = updated_latitudes
    df['longitude'] = updated_longitudes
    return df

# Temps per reactualitzar les dades remotes (cada 60 segons)
refresh_interval = 60
last_refresh = time.time()

# Obtenir les dades inicials
df = fetch_aircraft_data()
if df is None:
    sys.exit(0)

print("Iniciant actualització de posicions (prem Ctrl+C per aturar)...")
try:
    while True:
        # Cada minut, reactualitzar l'arxiu aircraft.json
        if time.time() - last_refresh >= refresh_interval:
            print("\n--- Reactualitzant dades remotes ---")
            new_df = fetch_aircraft_data()
            if new_df is not None:
                df = new_df
            last_refresh = time.time()
        
        # Actualitzar la posició (cada segon)
        df = update_positions(df)
        # Imprimir el DataFrame amb les columnes rellevants
        print(df[['icao24', 'latitude', 'longitude', 'speed', 'track', 'city', 'country']])
        print("-" * 80)
        time.sleep(1)
except KeyboardInterrupt:
    print("Finalitzat per l'usuari.")
