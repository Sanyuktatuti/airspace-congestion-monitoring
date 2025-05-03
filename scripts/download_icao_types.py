import requests
import csv

API_KEY = "26960a82-e995-4c19-a4f9-1273905875e5"
BASE = "https://applications.icao.int/dataservices/api"

# 1) get all manufacturers
r = requests.get(f"{BASE}/manufacturer-list",
    params={"api_key": API_KEY, "format": "csv"})
r.raise_for_status()
manufacturers = [row[0] for row in csv.reader(r.text.splitlines())][1:]

# 2) for each manufacturer fetch its types and append to a master CSV
with open("icao_all_types.csv", "w", newline="") as out:
    writer = None
    for mfr in manufacturers:
        resp = requests.get(f"{BASE}/type-list",
            params={"api_key": API_KEY, "format": "csv", "manufacturer": mfr})
        if resp.status_code != 200:
            continue
        rows = list(csv.reader(resp.text.splitlines()))
        if writer is None:
            writer = csv.writer(out)
            writer.writerow(rows[0])           # write header once
        for row in rows[1:]:
            writer.writerow(row)
