import asyncio
import aiohttp
import async_timeout
import pandas as pd
from datetime import date, timedelta
from itertools import product
from tqdm import tqdm
import time
import csv

# --- KONFIG ---
START_DATE = date(2026, 3, 16)
END_DATE = date(2026, 8, 31)

ORIGINS = {
    "LCJ": "Łódź",
    "WMI": "Warszawa Modlin",
    "WAW": "Warszawa Chopin",
    "KTW": "Katowice",
    "KRK": "Kraków",
    "POZ": "Poznań",

    # dodany londyn
    "STN": "London Stansted",
    "LTN": "London Luton"
}

DESTINATIONS = {
    "STN": "London Stansted",
    "LTN": "London Luton",
    "EDI": "Edinburgh",

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

    "LIS": "Lizbona",
    "OPO": "Porto",
    "FAO": "Faro",

    "BVA": "Paryż Beauvais",
    "MRS": "Marsylia",

    "ATH": "Ateny",
    "SKG": "Saloniki",
    "CFU": "Korfu",
    "RHO": "Rodos",
    "CHQ": "Chania",

    "PFO": "Pafos",

    "DBV": "Dubrownik",
    "SPU": "Split",
    "ZAD": "Zadar",

    "TGD": "Podgorica",
}

LONDON_AIRPORTS = {"STN","LTN"}
LONDON_ALLOWED_DEST = {"FNC","TFS"}

ADULTS = 1
LANG = "pl-pl"
MARKET = "pl-pl"

CONCURRENCY = 8
REQUEST_TIMEOUT = 20
MAX_RETRIES = 6
BACKOFF_BASE = 1.5

OUTPUT_CSV = 'ryanair_nov_full_async.csv'

CSV_HEADERS = [
    "origin_iata","origin_city",
    "destination_iata","destination_city",
    "outbound_date","inbound_date",
    "total_price","link"
]


def daterange(start_date,end_date):
    d=start_date
    while d<=end_date:
        yield d
        d+=timedelta(days=1)


def make_link(origin,dest,out_date,in_date):

    return (
        f"https://www.ryanair.com/pl/pl/trip/flights/select?"
        f"adults={ADULTS}&teens=0&children=0&infants=0"
        f"&dateOut={out_date.isoformat()}&dateIn={in_date.isoformat()}"
        f"&isConnectedFlight=false&discount=0&promoCode=&isReturn=true"
        f"&originIata={origin}&destinationIata={dest}"
    )


def write_row_csv(row):

    with open(OUTPUT_CSV,"a",newline="",encoding="utf-8") as f:

        writer=csv.DictWriter(f,fieldnames=CSV_HEADERS)
        writer.writerow(row)


async def fetch_farfnd(session,origin,dest,out_date,in_date):

    url="https://services-api.ryanair.com/farfnd/3/roundTripFares"

    params={
        "departureAirportIataCode":origin,
        "arrivalAirportIataCode":dest,
        "outboundDepartureDateFrom":out_date.isoformat(),
        "outboundDepartureDateTo":out_date.isoformat(),
        "inboundDepartureDateFrom":in_date.isoformat(),
        "inboundDepartureDateTo":in_date.isoformat(),
        "language":LANG,
        "market":MARKET,
        "limit":10
    }

    headers={"User-Agent":"Mozilla/5.0"}

    backoff=1.0

    for attempt in range(MAX_RETRIES):

        try:

            async with async_timeout.timeout(REQUEST_TIMEOUT):

                async with session.get(url,params=params,headers=headers) as resp:

                    if resp.status==200:

                        data=await resp.json(content_type=None)

                        fares=data.get("fares") or data.get("roundTripFares") or []

                        prices=[]

                        for f in fares:

                            try:

                                outp=f["outbound"]["price"]["value"]
                                inp=f["inbound"]["price"]["value"]

                                prices.append(outp+inp)

                            except:
                                pass

                        if prices:
                            return min(prices)

                        return None

                    elif resp.status in (429,500,502,503):

                        await asyncio.sleep(backoff)
                        backoff*=BACKOFF_BASE

        except:
            await asyncio.sleep(backoff)
            backoff*=BACKOFF_BASE

    return None


async def worker_task(sema,session,origin,dest,out_date,in_date):

    async with sema:

        price=await fetch_farfnd(session,origin,dest,out_date,in_date)

        if price:

            row={
                "origin_iata":origin,
                "origin_city":ORIGINS.get(origin,""),
                "destination_iata":dest,
                "destination_city":DESTINATIONS.get(dest,""),
                "outbound_date":out_date.isoformat(),
                "inbound_date":in_date.isoformat(),
                "total_price":price,
                "link":make_link(origin,dest,out_date,in_date)
            }

            write_row_csv(row)

            return row

        return None


async def main():

    with open(OUTPUT_CSV,"w",newline="",encoding="utf-8") as f:
        writer=csv.DictWriter(f,fieldnames=CSV_HEADERS)
        writer.writeheader()

    out_dates=list(daterange(START_DATE,END_DATE))

    connector=aiohttp.TCPConnector(limit_per_host=CONCURRENCY)

    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT+5)

    sema=asyncio.Semaphore(CONCURRENCY)

    combos = [

    (origin, dest, out_date, out_date + timedelta(days=delta))

    for origin, dest in product(ORIGINS.keys(), DESTINATIONS.keys())

    for out_date in out_dates

    for delta in (
        range(1,9) if origin in LONDON_AIRPORTS
        else range(1,4)
    )

    if out_date + timedelta(days=delta) <= END_DATE

    and not (
        origin in LONDON_AIRPORTS
        and dest not in LONDON_ALLOWED_DEST
    )
]

    print("Kombinacji:",len(combos))

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    ) as session:

        tasks=[worker_task(sema,session,o,d,od,id) for o,d,od,id in combos]

        for f in tqdm(asyncio.as_completed(tasks),total=len(tasks)):

            await f


if __name__=="__main__":

    start=time.time()

    asyncio.run(main())

    print("Czas:",time.time()-start)
