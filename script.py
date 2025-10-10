import asyncio
import aiohttp
import async_timeout
import pandas as pd
from datetime import date, timedelta
from itertools import product
from tqdm import tqdm
import time
import csv
import os

# --- KONFIG ---
START_DATE = date(2025, 11, 1)
END_DATE = date(2025, 12, 31)

ORIGINS = {"LCJ": "Łódź", "WMI": "Warszawa Modlin", "WAW": "Warszawa Chopin", "KTW": "Katowice", "KRK": "Kraków", "POZ": "Poznań"}
DESTINATIONS = {
    "STN": "London Stansted", "EDI": "Edinburgh", "AGP": "Malaga", "ALC": "Alicante",
    "CDG": "Paryż", "ATH": "Ateny", "BGY": "Bergamo", "LTN": "London Luton", "BCN": "Barcelona", "TFS": "Teneryfa Południe", "BVA": "Paryż Beauvais", "DBV": "Chorwacja Dubrovnik", "MLA": "Malta",
}

ADULTS = 1
LANG = "pl-pl"
MARKET = "pl-pl"

CONCURRENCY = 8
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3
BACKOFF_BASE = 1.5

OUTPUT_CSV = 'ryanair_nov_full_async.csv'
CSV_HEADERS = ["origin_iata","origin_city","destination_iata","destination_city",
               "outbound_date","inbound_date","total_price","link"]

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
        f"&isConnectedFlight=false&discount=0&promoCode=&isReturn=true"
        f"&originIata={origin}&destinationIata={dest}"
        f"&tpAdults={ADULTS}&tpTeens=0&tpChildren=0&tpInfants=0"
        f"&tpStartDate={out_date.isoformat()}&tpEndDate={in_date.isoformat()}"
        f"&tpDiscount=0&tpPromoCode=&tpOriginIata={origin}&tpDestinationIata={dest}"
    )

def write_row_csv(row):
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writerow(row)

async def fetch_farfnd(session, origin, dest, out_date, in_date, proxy=None):
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
    headers = {"User-Agent": "Mozilla/5.0 (compatible; AsyncScraper/1.0)"}
    backoff = 1.0
    for attempt in range(1, MAX_RETRIES+1):
        try:
            async with async_timeout.timeout(REQUEST_TIMEOUT):
                async with session.get(url, params=params, headers=headers, proxy=proxy) as resp:
                    if resp.status == 200:
                        try:
                            data = await resp.json(content_type=None)
                        except:
                            return None
                        fares = data.get("fares") or data.get("roundTripFares") or []
                        prices = []
                        for f in fares:
                            try:
                                outp = f.get("outbound", {}).get("price", {}).get("value")
                                inp = f.get("inbound", {}).get("price", {}).get("value")
                                if outp is not None and inp is not None:
                                    prices.append(outp + inp)
                            except:
                                continue
                        if prices:
                            return {"price": min(prices)}
                        return None
                    elif resp.status in (429, 500, 502, 503, 521, 522):
                        await asyncio.sleep(backoff)
                        backoff *= BACKOFF_BASE
                        continue
                    else:
                        return None
        except asyncio.TimeoutError:
            await asyncio.sleep(backoff)
            backoff *= BACKOFF_BASE
            continue
        except Exception:
            await asyncio.sleep(backoff)
            backoff *= BACKOFF_BASE
            continue
    return None

async def worker_task(sema, session, origin, dest, out_date, in_date, proxy=None):
    async with sema:
        result = await fetch_farfnd(session, origin, dest, out_date, in_date, proxy)
        if result:
            link = make_link(origin, dest, out_date, in_date)
            row = {
                "origin_iata": origin,
                "origin_city": ORIGINS.get(origin,""),
                "destination_iata": dest,
                "destination_city": DESTINATIONS.get(dest,""),
                "outbound_date": out_date.isoformat(),
                "inbound_date": in_date.isoformat(),
                "total_price": result["price"],
                "link": link
            }
            write_row_csv(row)
            return row
        return None

async def main():
    
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
    
    out_dates = list(daterange(START_DATE, END_DATE))
    connector = aiohttp.TCPConnector(limit_per_host=CONCURRENCY)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT+5)
    sema = asyncio.Semaphore(CONCURRENCY)
    combos = [
        (origin, dest, out_date, out_date + timedelta(days=delta))
        for origin, dest in product(ORIGINS.keys(), DESTINATIONS.keys())
        for out_date in out_dates
        for delta in range(1,4)
        if out_date + timedelta(days=delta) <= END_DATE
    ]
    print(f"Łącznie kombinacji do sprawdzenia: {len(combos)}")

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [worker_task(sema, session, o, d, od, id) for o,d,od,id in combos]
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="scraping"):
            await f

if __name__ == "__main__":
    t0 = time.time()
    asyncio.run(main())
    print("Koniec. Czas:", time.time() - t0, "s")









