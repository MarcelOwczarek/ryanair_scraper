import asyncio
import aiohttp
import async_timeout
from datetime import date, timedelta
from itertools import product
from tqdm import tqdm
import csv
import time

# --- KONFIG ---
START_DATE = date(2026,3,16)
END_DATE = date(2026,6,30)

RANGE_DAYS = 30

CONCURRENCY = 20
REQUEST_TIMEOUT = 20

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



OUTPUT_CSV = "ryanair_nov_full_async.csv"

def make_link(origin,dest,out_date,in_date):
    return f"https://www.ryanair.com/pl/pl/trip/flights/select?originIata={origin}&destinationIata={dest}&dateOut={out_date}&dateIn={in_date}&adults=1"

async def fetch(session,origin,dest,start,end):

    url="https://services-api.ryanair.com/farfnd/3/roundTripFares"

    params={
        "departureAirportIataCode":origin,
        "arrivalAirportIataCode":dest,
        "outboundDepartureDateFrom":start.isoformat(),
        "outboundDepartureDateTo":end.isoformat(),
        "market":"pl-pl",
        "language":"pl-pl",
        "limit":100
    }

    try:
        async with async_timeout.timeout(REQUEST_TIMEOUT):

            async with session.get(url,params=params) as resp:

                if resp.status!=200:
                    return []

                data=await resp.json(content_type=None)

                fares=data.get("fares") or data.get("roundTripFares") or []

                results=[]

                for f in fares:

                    try:

                        out_date=f["outbound"]["departureDate"][:10]
                        in_date=f["inbound"]["departureDate"][:10]

                        price=f["outbound"]["price"]["value"]+f["inbound"]["price"]["value"]

                        results.append((out_date,in_date,price))

                    except:
                        pass

                return results

    except:
        return []

async def worker(sema,session,origin,dest,start,end,writer):

    async with sema:

        fares=await fetch(session,origin,dest,start,end)

        for out_date,in_date,price in fares:

            writer.writerow({
                "origin_iata":origin,
                "origin_city":ORIGINS.get(origin,""),
                "destination_iata":dest,
                "destination_city":DESTINATIONS.get(dest,""),
                "outbound_date":out_date,
                "inbound_date":in_date,
                "total_price":price,
                "link":make_link(origin,dest,out_date,in_date)
            })

def date_chunks(start,end,days):

    current=start

    while current<=end:

        chunk_end=min(current+timedelta(days=days),end)

        yield current,chunk_end

        current=chunk_end+timedelta(days=1)

async def main():

    with open(OUTPUT_CSV,"w",newline="",encoding="utf-8") as f:

        writer=csv.DictWriter(f,fieldnames=CSV_HEADERS)

        writer.writeheader()

        sema=asyncio.Semaphore(CONCURRENCY)

        connector=aiohttp.TCPConnector(limit_per_host=CONCURRENCY)

        async with aiohttp.ClientSession(connector=connector) as session:

            tasks=[]

            for origin,dest in product(ORIGINS,DESTINATIONS):

                for start,end in date_chunks(START_DATE,END_DATE,RANGE_DAYS):

                    tasks.append(worker(sema,session,origin,dest,start,end,writer))

            for ftask in tqdm(asyncio.as_completed(tasks),total=len(tasks)):

                await ftask

if __name__=="__main__":

    t0=time.time()

    asyncio.run(main())

    print("czas:",round(time.time()-t0,2),"sek")
