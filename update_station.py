import os
import requests
from supabase import create_client, Client

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

eipa_url = os.environ.get("EIPA_DYNAMIC_URL")

print("Pobieranie danych dynamicznych z EIPA...")
# Dodałem nagłówki udające prawdziwą przeglądarkę, żeby ominąć podstawowe blokady botów
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*'
}

response = requests.get(eipa_url, headers=headers)

# --- SEKCJA DETEKTYWISTYCZNA ---
print(f"Status odpowiedzi serwera EIPA: {response.status_code}")

if response.status_code != 200:
    print("BŁĄD! Serwer EIPA odrzucił połączenie. Oto co napisał:")
    print(response.text[:500])
    exit(1)

try:
    dynamic_data = response.json()
    print(f"Sukces! Pobrano {len(dynamic_data)} rekordów z EIPA.")
except Exception as e:
    print("BŁĄD! EIPA zwróciła dane, ale to nie jest JSON. Oto początek pliku:")
    print(response.text[:500])
    exit(1)
# ------------------------------

print("Pobieranie ID stacji z bazy Supabase...")
db_response = supabase.table('fast_stations').select('point_id').execute()
our_stations = db_response.data

print("Rozpoczynam aktualizację statusów w bazie...")
available_points = {}

# UWAGA: Struktura danych EIPA mogła schować listę w kluczu "data" tak jak w plikach statycznych!
dane_do_przetworzenia = dynamic_data.get('data', []) if isinstance(dynamic_data, dict) else dynamic_data

for item in dane_do_przetworzenia:
    point_id = item.get('point_id')
    status_obj = item.get('status', {})
    is_free = (status_obj.get('availability') == 1 and status_obj.get('status') == 1)
    available_points[point_id] = is_free

updated_count = 0
for station in our_stations:
    p_id = station['point_id']
    is_available = available_points.get(p_id, False)
    supabase.table('fast_stations').update({'is_available': is_available}).eq('point_id', p_id).execute()
    updated_count += 1

print(f"Zakończono sukcesem! Zaktualizowano {updated_count} stacji na mapie.")
