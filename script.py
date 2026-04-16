import asyncio
import aiohttp
import async_timeout
from datetime import date, timedelta
from itertools import product
import time
import csv

# --- KONFIG ---
START_DATE = date(2026, 4, 16)
END_DATE = date(2026, 12, 31)

ORIGINS = {
    "LCJ": "Łódź",
    "WMI": "Warszawa Modlin",
    "WAW": "Warszawa Chopin",
    "KTW": "Katowice",
    "KRK": "Kraków",
    "POZ": "Poznań"
}

DESTINATIONS = {
    "STN": "London Stansted",
    "LTN": "London Luton",
    "EDI": "Edinburgh",
    "AGP": "Malaga",
    "SVQ": "Sewilla",
    "PMI": "Palma de Mallorca",
    "TFS": "Teneryfa Południe",
    "BGY": "Bergamo",
    "FCO": "Rzym Fiumicino",
    "CIA": "Rzym Ciampino",
    "NAP": "Neapol",
    "BVA": "Paryż Beauvais",
    "ATH": "Ateny",
    "CHQ": "Chania",
    "DBV": "Dubrownik",
    "TGD": "Podgorica",
}

ADULTS = 1
LANG = "pl-pl"
MARKET = "pl-pl"

CONCURRENCY = 8
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3
BACKOFF_BASE = 1.5

OUTPUT_CSV = 'ryanair_async.csv'
CSV_HEADERS = [
    "origin_iata","origin_city","destination_iata","destination_city",
    "outbound_date","inbound_date","total_price","link"
]

# --- FUNKCJE ---
def daterange(start_date, end_date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)

def make_link(origin, dest, out_date, in_date):
    return (
        f"https://www.ryanair.com/pl/pl/trip/flights/select?"
        f"adults={ADULTS}&teens=0&children=0&infants=0"
        f"&dateOut={out_date.isoformat()}&dateIn={in_date.isoformat()}"
        f"&isReturn=true"
        f"&originIata={origin}&destinationIata={dest}"
    )

def write_row_csv(row):
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writerow(row)

async def fetch_farfnd(session, origin, dest, out_date, in_date):
    url = "https://services-api.ryanair.com/farfnd/3/roundTripFares"
    params = {
        "departureAirportIataCode": origin,
        "arrivalAirportIataCode": dest,
        "outboundDepartureDateFrom": out_date.isoformat(),
        "outboundDepartureDateTo": out_date.isoformat(),
        "inboundDepartureDateFrom": in_date.isoformat(),
        "inboundDepartureDateTo": in_date.isoformat(),
        "language": LANG,
        "market": MARKET,
        "limit": 10
    }

    headers = {"User-Agent": "Mozilla/5.0"}
    backoff = 1.0

    for _ in range(MAX_RETRIES):
        try:
            async with async_timeout.timeout(REQUEST_TIMEOUT):
                async with session.get(url, params=params, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        fares = data.get("fares") or data.get("roundTripFares") or []

                        prices = []
                        for f in fares:
                            outp = f.get("outbound", {}).get("price", {}).get("value")
                            inp = f.get("inbound", {}).get("price", {}).get("value")
                            if outp and inp:
                                prices.append(outp + inp)

                        if prices:
                            return min(prices)
                        return None

        except:
            await asyncio.sleep(backoff)
            backoff *= BACKOFF_BASE

    return None

async def worker(sema, session, origin, dest, out_date, in_date):
    async with sema:
        price = await fetch_farfnd(session, origin, dest, out_date, in_date)
        if price:
            row = {
                "origin_iata": origin,
                "origin_city": ORIGINS.get(origin, ""),
                "destination_iata": dest,
                "destination_city": DESTINATIONS.get(dest, ""),
                "outbound_date": out_date.isoformat(),
                "inbound_date": in_date.isoformat(),
                "total_price": price,
                "link": make_link(origin, dest, out_date, in_date)
            }
            write_row_csv(row)

async def main():
    # CSV header
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()

    sema = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        tasks = []

        for origin, dest in product(ORIGINS.keys(), DESTINATIONS.keys()):
            for out_date in daterange(START_DATE, END_DATE):
                for trip_len in range(2, 6):  # np. 2–5 dni
                    in_date = out_date + timedelta(days=trip_len)
                    if in_date > END_DATE:
                        continue

                    tasks.append(
                        worker(sema, session, origin, dest, out_date, in_date)
                    )

        await asyncio.gather(*tasks)

# --- START ---
if __name__ == "__main__":
    t0 = time.time()
    asyncio.run(main())
    print("Koniec. Czas:", time.time() - t0, "s")
