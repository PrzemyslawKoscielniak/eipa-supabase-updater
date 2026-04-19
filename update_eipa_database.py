import os
import requests
from supabase import create_client, Client

# Pobieranie kluczy z bezpiecznych zmiennych środowiskowych GitHuba
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Podmień na prawdziwe linki do API EIPA
URL_STATIONS = "https://eipa.udt.gov.pl/reader/export-data/station/caa0415cda844600a751d29560d65318"
URL_POINTS = "https://eipa.udt.gov.pl/reader/export-data/point/caa0415cda844600a751d29560d65318"
URL_POOLS = "https://eipa.udt.gov.pl/reader/export-data/pool/caa0415cda844600a751d29560d65318"
URL_OPERATORS = "https://eipa.udt.gov.pl/reader/export-data/operator/caa0415cda844600a751d29560d65318"

# --- 2. POBIERANIE DANYCH Z API ---
print("Pobieranie najnowszych danych z EIPA...")
raw_stations = requests.get(URL_STATIONS).json()
stations_data = raw_stations.get('data', []) if isinstance(raw_stations, dict) else raw_stations

raw_points = requests.get(URL_POINTS).json()
points_data = raw_points.get('data', []) if isinstance(raw_points, dict) else raw_points

# NOWE: Pobieranie baz i operatorów
raw_pools = requests.get(URL_POOLS).json()
pools_data = raw_pools.get('data', []) if isinstance(raw_pools, dict) else raw_pools

raw_operators = requests.get(URL_OPERATORS).json()
operators_data = raw_operators.get('data', []) if isinstance(raw_operators, dict) else raw_operators

# --- 3. PRZETWARZANIE DANYCH ---
print("Filtrowanie stacji > 300kW i mapowanie operatorów...")

# Tworzenie słowników dla szybkiego wyszukiwania (hash maps)
stations_dict = {s.get('id'): s for s in stations_data if s.get('id')}
pools_dict = {p.get('id'): p.get('operator_id') for p in pools_data if p.get('id')}
operators_dict = {o.get('id'): o.get('name') for o in operators_data if o.get('id')}

fast_chargers = []

for point in points_data:
    max_power = 0
    solutions = point.get('charging_solutions', [])
    for sol in solutions:
        p = sol.get('power', 0)
        if p > max_power:
            max_power = p

    if max_power > 300:
        point_id = point.get('id')
        station_id = point.get('station_id')
        
        parent_station = stations_dict.get(station_id, {})
        city = parent_station.get('location', {}).get('city', 'Nieznane')
        
        # NOWA LOGIKA: Wyciąganie nazwy operatora
        pool_id = parent_station.get('pool_id')
        operator_id = pools_dict.get(pool_id)
        operator_name = operators_dict.get(operator_id, 'Nieznany operator')
        
        # Kod (EVSE ID)
        code = point.get('code') or parent_station.get('code', '')
        
        lat = parent_station.get('latitude', 0)
        lng = parent_station.get('longitude', 0)
        
        fast_chargers.append({
            'point_id': point_id,
            'station_id': station_id,
            'code': code,
            'station_name': city,
            'operator_name': operator_name,  # NOWE POLE
            'power': max_power,
            'lat': lat,
            'lng': lng,
            'is_available': False # lub zachowanie poprzedniego stanu, jeśli tak robicie
        })

# --- 4. WYSYŁANIE DO SUPABASE (UPSERT) ---
print(f"Znaleziono {len(fast_chargers)} szybkich punktów ładowania. Wysyłam do bazy...")

if fast_chargers:
    try:
        response = supabase.table('charging_stations').upsert(fast_chargers, on_conflict='point_id').execute()
        print("✅ Sukces! Baza danych została zaktualizowana.")
    except Exception as e:
        print(f"❌ Wystąpił błąd podczas wysyłania do bazy: {e}")
else:
    print("Brak stacji do aktualizacji.")
