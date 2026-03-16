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
END_DATE = date(2026, 6, 30)

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

CONCURRENCY = 20
REQUEST_TIMEOUT = 20
MAX_RETRIES = 5
BACKOFF_BASE = 1.5

BATCH_SIZE = 2000

OUTPUT_CSV = "ryanair_nov_full_async.csv"

CSV_HEADERS = [
    "origin_iata","origin_city",
    "destination_iata","destination_city",
    "outbound_date","inbound_date",
    "total_price","link"
]

rows_buffer = []

# --- FUNKCJE ---
def daterange(start, end):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)

def make_link(origin, dest, out_date, in_date):
    return (
        f"https://www.ryanair.com/pl/pl/trip/flights/select?"
        f"adults=1&dateOut={out_date}&dateIn={in_date}"
        f"&originIata={origin}&destinationIata={dest}"
    )

def flush_buffer():
    global rows_buffer
    if not rows_buffer:
        return

    with open(OUTPUT_CSV,"a",newline="",encoding="utf-8") as f:
        writer = csv.DictWriter(f,fieldnames=CSV_HEADERS)
        writer.writerows(rows_buffer)

    rows_buffer = []

def write_row(row):
    rows_buffer.append(row)

    if len(rows_buffer) >= 50:
        flush_buffer()

async def fetch(session, origin, dest, out_date, in_date):
    url = "https://services-api.ryanair.com/farfnd/3/roundTripFares"

    params = {
        "departureAirportIataCode": origin,
        "arrivalAirportIataCode": dest,
        "outboundDepartureDateFrom": out_date.isoformat(),
        "outboundDepartureDateTo": out_date.isoformat(),
        "inboundDepartureDateFrom": in_date.isoformat(),
        "inboundDepartureDateTo": in_date.isoformat(),
        "market": MARKET,
        "language": LANG
    }

    backoff = 1

    for _ in range(MAX_RETRIES):

        try:
            async with async_timeout.timeout(REQUEST_TIMEOUT):
                async with session.get(url,params=params) as resp:

                    if resp.status != 200:
                        await asyncio.sleep(backoff)
                        backoff *= BACKOFF_BASE
                        continue

                    data = await resp.json(content_type=None)

                    fares = data.get("fares") or data.get("roundTripFares") or []

                    prices = []

                    for f in fares:
                        try:
                            o = f["outbound"]["price"]["value"]
                            i = f["inbound"]["price"]["value"]
                            prices.append(o+i)
                        except:
                            pass

                    if prices:
                        return min(prices)

                    return None

        except:
            await asyncio.sleep(backoff)
            backoff *= BACKOFF_BASE

    return None

async def worker(sema, session, origin, dest, out_date, in_date):

    async with sema:

        price = await fetch(session,origin,dest,out_date,in_date)

        if price:

            row = {
                "origin_iata":origin,
                "origin_city":ORIGINS.get(origin,""),
                "destination_iata":dest,
                "destination_city":DESTINATIONS.get(dest,""),
                "outbound_date":out_date.isoformat(),
                "inbound_date":in_date.isoformat(),
                "total_price":price,
                "link":make_link(origin,dest,out_date,in_date)
            }

            write_row(row)

async def main():

    with open(OUTPUT_CSV,"w",newline="",encoding="utf-8") as f:
        writer = csv.DictWriter(f,fieldnames=CSV_HEADERS)
        writer.writeheader()

    out_dates = list(daterange(START_DATE,END_DATE))

    combos = [
        (o,d,od,od+timedelta(days=delta))
        for o,d in product(ORIGINS.keys(),DESTINATIONS.keys())
        for od in out_dates
        for delta in range(1,4)
        if od+timedelta(days=delta)<=END_DATE
    ]

    print("kombinacji:",len(combos))

    sema = asyncio.Semaphore(CONCURRENCY)

    connector = aiohttp.TCPConnector(limit_per_host=CONCURRENCY)

    async with aiohttp.ClientSession(connector=connector) as session:

        for i in range(0,len(combos),BATCH_SIZE):

            batch = combos[i:i+BATCH_SIZE]

            tasks = [
                worker(sema,session,o,d,od,id)
                for o,d,od,id in batch
            ]

            for f in tqdm(asyncio.as_completed(tasks),total=len(tasks)):
                await f

    flush_buffer()

if __name__ == "__main__":

    t0 = time.time()

    asyncio.run(main())

    print("czas:",round(time.time()-t0,2),"s")
