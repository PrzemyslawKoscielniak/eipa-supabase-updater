import os
import requests
from supabase import create_client, Client

# Konfiguracja Supabase pobrana z bezpiecznego sejfu GitHuba
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

eipa_url = os.environ.get("EIPA_DYNAMIC_URL")

print("Pobieranie danych dynamicznych z EIPA...")
headers = {'User-Agent': 'Mozilla/5.0'}
response = requests.get(eipa_url, headers=headers)
dynamic_data = response.json()

# Budujemy słownik dostępności (point_id: True/False)
# Zakładamy, że punkt jest dostępny gdy: availability == 1 (urządzenie działa) oraz status == 1 (wtyczka jest wolna)
available_points = {}
for item in dynamic_data:
    point_id = item.get('point_id')
    status_obj = item.get('status', {})
    
    is_free = (status_obj.get('availability') == 1 and status_obj.get('status') == 1)
    available_points[point_id] = is_free

print("Pobieranie ID stacji z bazy Supabase...")
db_response = supabase.table('fast_stations').select('point_id').execute()
our_stations = db_response.data

print("Rozpoczynam aktualizację statusów w bazie...")
updated_count = 0
for station in our_stations:
    p_id = station['point_id']
    is_available = available_points.get(p_id, False)
    
    # Aktualizujemy konkretny wiersz w Supabase
    supabase.table('fast_stations').update({'is_available': is_available}).eq('point_id', p_id).execute()
    updated_count += 1

print(f"Zakończono sukcesem! Zaktualizowano {updated_count} stacji na mapie.")
