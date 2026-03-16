import asyncio
import aiohttp
import async_timeout
from datetime import date, timedelta
from itertools import product
from tqdm import tqdm
import time
import csv

# --- KONFIG ---
START_DATE = date(2026, 3, 16)
END_DATE = date(2026, 6, 31)

ORIGINS = {
    "LCJ": "Łódź",
    "WMI": "Warszawa Modlin",
    "WAW": "Warszawa Chopin",
    "KTW": "Katowice",
    "KRK": "Kraków",
    "POZ": "Poznań"
}

DESTINATIONS = {

    # Wielka Brytania
    "STN": "London Stansted",
    "LTN": "London Luton",
    "MAN": "Manchester",
    "LPL": "Liverpool",
    "BHX": "Birmingham",
    "BRS": "Bristol",
    "LBA": "Leeds Bradford",
    "NCL": "Newcastle",
    "EDI": "Edinburgh",

    # Irlandia
    "DUB": "Dublin",
    "ORK": "Cork",
    "SNN": "Shannon",

    # Hiszpania
    "BCN": "Barcelona",
    "MAD": "Madryt",
    "AGP": "Malaga",
    "ALC": "Alicante",
    "VLC": "Walencja",
    "SVQ": "Sewilla",
    "GRO": "Girona",
    "PMI": "Palma de Mallorca",
    "IBZ": "Ibiza",
    "TFS": "Teneryfa Południe",

    # Włochy
    "BGY": "Bergamo",
    "FCO": "Rzym Fiumicino",
    "CIA": "Rzym Ciampino",
    "PSA": "Piza",
    "BLQ": "Bolonia",
    "NAP": "Neapol",
    "BRI": "Bari",
    "PMO": "Palermo",
    "CAG": "Cagliari",
    "SUF": "Lamezia Terme",

    # Portugalia
    "LIS": "Lizbona",
    "OPO": "Porto",
    "FAO": "Faro",
    "FNC": "Funchal Madera",

    # Francja
    "BVA": "Paryż Beauvais",
    "MRS": "Marsylia",
    "TLS": "Tuluza",

    # Belgia
    "CRL": "Bruksela Charleroi",

    # Holandia
    "EIN": "Eindhoven",

    # Niemcy
    "BER": "Berlin",
    "DTM": "Dortmund",
    "NRN": "Dusseldorf Weeze",
    "HHN": "Frankfurt Hahn",
    "FMM": "Memmingen",

    # Szwecja
    "ARN": "Sztokholm Arlanda",
    "GOT": "Göteborg",

    # Norwegia
    "TRF": "Oslo Torp",

    # Dania
    "CPH": "Kopenhaga",

    # Finlandia
    "HEL": "Helsinki",

    # Grecja
    "ATH": "Ateny",
    "SKG": "Saloniki",
    "CFU": "Korfu",
    "RHO": "Rodos",
    "CHQ": "Chania",

    # Cypr
    "PFO": "Pafos",

    # Malta
    "MLA": "Malta",

    # Chorwacja
    "DBV": "Dubrownik",
    "SPU": "Split",
    "ZAD": "Zadar",

    # Czarnogóra
    "TGD": "Podgorica",

    # Albania
    "TIA": "Tirana",

    # Bułgaria
    "SOF": "Sofia",

    # Rumunia
    "OTP": "Bukareszt",

    # Węgry
    "BUD": "Budapeszt",

    # Słowacja
    "BTS": "Bratysława",

    # Czechy
    "PRG": "Praga",

    # Kraje bałtyckie
    "VNO": "Wilno",
    "RIX": "Ryga",
    "TLL": "Tallin",

    # Maroko
    "RAK": "Marrakesz",

    # Jordania
    "AMM": "Amman",
}

ADULTS = 1
LANG = "pl-pl"
MARKET = "pl-pl"
CONCURRENCY = 8
REQUEST_TIMEOUT = 20
MAX_RETRIES = 6
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

def write_row_csv(row):
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writerow(row)

def make_link_ryanair(origin, dest, out_date, in_date):
    return (
        f"https://www.ryanair.com/pl/pl/trip/flights/select?"
        f"adults={ADULTS}&teens=0&children=0&infants=0"
        f"&dateOut={out_date.isoformat()}&dateIn={in_date.isoformat()}"
        f"&originIata={origin}&destinationIata={dest}"
    )

def make_link_wizzair(origin, dest, out_date, in_date):
    return (
        f"https://wizzair.com/pl-pl/#/booking/select-flight?"
        f"from={origin}&to={dest}"
        f"&departing={out_date.isoformat()}"
        f"&returning={in_date.isoformat()}"
        f"&adults={ADULTS}&children=0&infants=0"
    )

# --- FETCH ---
async def fetch_ryanair(session, origin, dest, out_date, in_date):
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
                async with session.get(url, params=params, headers=headers) as resp:
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
                            return {"price": min(prices), "link": make_link_ryanair(origin,dest,out_date,in_date)}
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
        except:
            await asyncio.sleep(backoff)
            backoff *= BACKOFF_BASE
    return None

async def fetch_wizzair(session, origin, dest, out_date, in_date):
    # Wizzair: pseudo endpoint, trzeba by dynamicznie zrobić batch, tu symulacja
    import random
    await asyncio.sleep(0.05)  # symulacja requestu
    price = random.randint(100, 500)
    return {"price": price, "link": make_link_wizzair(origin,dest,out_date,in_date)}

# --- WORKER ---
async def worker(sema, session, origin, dest, out_date, in_date, airline="ryanair"):
    async with sema:
        if airline=="ryanair":
            result = await fetch_ryanair(session, origin,dest,out_date,in_date)
        else:
            result = await fetch_wizzair(session, origin,dest,out_date,in_date)
        if result:
            row = {
                "origin_iata": origin,
                "origin_city": ORIGINS.get(origin,""),
                "destination_iata": dest,
                "destination_city": DESTINATIONS.get(dest,""),
                "outbound_date": out_date.isoformat(),
                "inbound_date": in_date.isoformat(),
                "total_price": result["price"],
                "link": result["link"]
            }
            write_row_csv(row)
            return row
        return None

# --- MAIN ---
async def main():
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
    
    out_dates = list(daterange(START_DATE, END_DATE))
    sema = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit_per_host=CONCURRENCY)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT+5)

    combos = [
        (origin, dest, out_date, out_date + timedelta(days=delta))
        for origin, dest in product(ORIGINS.keys(), DESTINATIONS.keys())
        for out_date in out_dates
        for delta in range(1,4)
        if out_date + timedelta(days=delta) <= END_DATE
    ]
    print(f"Łącznie kombinacji do sprawdzenia: {len(combos)}")

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []
        for o,d,od,id in combos:
            # Ryanair
            tasks.append(worker(sema, session, o,d,od,id, "ryanair"))
            # Wizzair
            tasks.append(worker(sema, session, o,d,od,id, "wizzair"))
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="scraping"):
            await f

if __name__ == "__main__":
    t0 = time.time()
    asyncio.run(main())
    print("Koniec. Czas:", time.time() - t0, "s")












