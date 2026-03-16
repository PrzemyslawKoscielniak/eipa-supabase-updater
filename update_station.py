import os
import cloudscraper
from supabase import create_client, Client

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

eipa_url = os.environ.get("EIPA_DYNAMIC_URL")

print("Pobieranie danych dynamicznych z EIPA (omijanie Cloudflare)...")
scraper = cloudscraper.create_scraper(
    browser={
        'browser': 'chrome',
        'platform': 'windows',
        'desktop': True
    }
)

response = scraper.get(eipa_url)

print(f"Status odpowiedzi serwera EIPA: {response.status_code}")

if response.status_code != 200:
    print("BŁĄD! Cloudflare nadal blokuje. Oto odpowiedź:")
    print(response.text[:500])
    exit(1)

try:
    dynamic_data = response.json()
    dane_do_przetworzenia = dynamic_data.get('data', []) if isinstance(dynamic_data, dict) else dynamic_data
    print(f"Sukces! Pobrano {len(dane_do_przetworzenia)} rekordów z EIPA.")
except Exception as e:
    print("BŁĄD! To nie jest JSON. Oto początek pliku:")
    print(response.text[:500])
    exit(1)

print("Pobieranie ID stacji z bazy Supabase...")
db_response = supabase.table('charging_stations').select('point_id').execute()
our_stations = db_response.data

print("Rozpoczynam aktualizację statusów i cen w bazie...")
points_data = {}

for item in dane_do_przetworzenia:
    point_id = item.get('point_id')
    status_obj = item.get('status', {})
    is_free = (status_obj.get('availability') == 1 and status_obj.get('status') == 1)
    
    # NOWOŚĆ: Wyciąganie danych o cenie
    prices = item.get('prices', [])
    price_info = "Brak danych"
    if prices and len(prices) > 0:
        price_val = prices[0].get('price')
        price_unit = prices[0].get('unit', 'kWh')
        if price_val:
            # Składamy ładny tekst, np. "1.20 PLN/kWh"
            price_info = f"{price_val} PLN/{price_unit}"
            
    points_data[point_id] = {
        'is_available': is_free,
        'price': price_info
    }

updated_count = 0
for station in our_stations:
    p_id = station['point_id']
    # Pobieramy dane dla punktu (jeśli go nie ma w dynamicznych, to ustawiamy jako zajęty i bez ceny)
    data = points_data.get(p_id, {'is_available': False, 'price': 'Brak danych'})
    
    # Aktualizujemy oba pola w Supabase
    supabase.table('charging_stations').update({
        'is_available': data['is_available'],
        'price': data['price']
    }).eq('point_id', p_id).execute()
    
    updated_count += 1

print(f"Zakończono sukcesem! Zaktualizowano {updated_count} stacji na mapie (status + cena).")
