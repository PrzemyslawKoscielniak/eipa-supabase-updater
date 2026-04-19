import os
import sys
import requests
from supabase import create_client, Client

# Pobieranie kluczy
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Linki do Eipa
URL_STATIONS = "https://eipa.udt.gov.pl/reader/export-data/station/caa0415cda844600a751d29560d65318"
URL_POINTS = "https://eipa.udt.gov.pl/reader/export-data/point/caa0415cda844600a751d29560d65318"
URL_POOLS = "https://eipa.udt.gov.pl/reader/export-data/pool/caa0415cda844600a751d29560d65318"
URL_OPERATORS = "https://eipa.udt.gov.pl/reader/export-data/operator/caa0415cda844600a751d29560d65318"

# --- BEZPIECZNA FUNKCJA POBIERANIA ---
def fetch_data(url, name):
    print(f"Pobieranie: {name}...")
    try:
        # Timeout chroni przed zawieszeniem, raise_for_status łapie blokady serwera
        resp = requests.get(url, timeout=30)
        resp.raise_for_status() 
        data = resp.json()
        return data.get('data', []) if isinstance(data, dict) else data
    except Exception as e:
        print(f"❌ Błąd podczas pobierania {name} z EIPA: {e}")
        return []

try:
    # 1. POBIERANIE
    stations_data = fetch_data(URL_STATIONS, "Stacje")
    points_data = fetch_data(URL_POINTS, "Punkty")
    pools_data = fetch_data(URL_POOLS, "Bazy (Pools)")
    operators_data = fetch_data(URL_OPERATORS, "Operatorzy")

    # 2. BEZPIECZNE TWORZENIE SŁOWNIKÓW
    print("Mapowanie relacji...")
    stations_dict = {}
    for s in stations_data:
        if isinstance(s, dict) and s.get('id'):
            stations_dict[s.get('id')] = s

    pools_dict = {}
    for p in pools_data:
        if isinstance(p, dict) and p.get('id'):
            pools_dict[p.get('id')] = p.get('operator_id')

    operators_dict = {}
    for o in operators_data:
        if isinstance(o, dict) and o.get('id'):
            operators_dict[o.get('id')] = o.get('name')

    # 3. PRZETWARZANIE STACJI > 300kW
    print("Filtrowanie najszybszych ładowarek...")
    fast_chargers = []

    for point in points_data:
        if not isinstance(point, dict):
            continue

        max_power = 0
        for sol in point.get('charging_solutions', []):
            p = sol.get('power', 0)
            if p > max_power:
                max_power = p

        if max_power > 300:
            point_id = point.get('id')
            station_id = point.get('station_id')
            
            parent_station = stations_dict.get(station_id, {})
            city = parent_station.get('location', {}).get('city', 'Nieznane')
            
            # Nowa logika szukania nazwy
            pool_id = parent_station.get('pool_id')
            operator_id = pools_dict.get(pool_id)
            operator_name = operators_dict.get(operator_id, 'Nieznany operator')
            
            code = point.get('code') or parent_station.get('code', '')
            
            fast_chargers.append({
                'point_id': point_id,
                'station_id': station_id,
                'code': code,
                'station_name': city,
                'operator_name': operator_name, # Mamy operatora!
                'power': max_power,
                'lat': parent_station.get('latitude', 0),
                'lng': parent_station.get('longitude', 0),
                'is_available': False
            })

    # 4. WYSYŁANIE DO SUPABASE
    print(f"Znaleziono {len(fast_chargers)} szybkich punktów. Wysyłam...")
    if fast_chargers:
        try:
            supabase.table('charging_stations').upsert(fast_chargers, on_conflict='point_id').execute()
            print("✅ Sukces! Baza została zaktualizowana.")
        except Exception as e:
            # Jeśli wyskoczy błąd tutaj, na 99% zapomniałeś dodać kolumny w SQL!
            print(f"❌ Błąd bazy danych (Czy dodałeś kolumnę 'operator_name'?): {e}")
    else:
        print("Brak stacji do aktualizacji.")

except Exception as e:
    print(f"❌ Krytyczny błąd skryptu: {e}")
    sys.exit(1)
