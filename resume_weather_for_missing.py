# resume_weather_for_missing.py
import requests, certifi, time, csv
import pandas as pd
from pathlib import Path
from datetime import date, timedelta

OUT_DIR = Path("weather_outputs")
OUT_DIR.mkdir(exist_ok=True)
AP_CSV = Path("airports.csv")
MISSING = Path("missing_airports.txt")
LOG = Path("resume_log.csv")

OPEN_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
DAILY_VARS = "weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,wind_gusts_10m_max,wind_direction_10m_dominant,shortwave_radiation_sum,uv_index_max"

REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_REQUESTS = 4.0   # mais conservador
SLEEP_ON_429 = 60             # aguarda 60s se 429
RETRY_ATTEMPTS = 6
USE_MONTHLY = True
START_DATE = date(2024,1,1)
END_DATE = date(2024,12,31)

def months_between(start, end):
    periods = pd.date_range(start=start, end=end, freq='MS').to_pydatetime().tolist()
    res = []
    for s in periods:
        sdate = date(s.year, s.month, 1)
        if s.month == 12:
            edate = date(s.year, 12, 31)
        else:
            edate = date(s.year, s.month+1, 1) - timedelta(days=1)
        if edate > end:
            edate = end
        res.append((sdate, edate))
    return res

# load airports coords
ap = pd.read_csv(AP_CSV, engine="python")
ap.columns = [c.replace("\ufeff","").strip() for c in ap.columns]
if "iata_code" in ap.columns:
    ap = ap.rename(columns={"iata_code":"iata"})
if "iata" not in ap.columns and "ident" in ap.columns:
    ap = ap.rename(columns={"ident":"iata"})
latcol = [c for c in ap.columns if "latitude" in c.lower()]
loncol = [c for c in ap.columns if "longitude" in c.lower()]
ap = ap.rename(columns={latcol[0]:"lat", loncol[0]:"lon"})
ap["iata"] = ap["iata"].astype(str).str.strip().str.upper()
ap = ap.set_index("iata")

if not MISSING.exists():
    raise FileNotFoundError("missing_airports.txt não encontrado. Rode list_missing_airports.py primeiro.")

with open(MISSING) as f:
    miss = [l.strip().upper() for l in f if l.strip()]

# prepare log
if not LOG.exists():
    with open(LOG, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["iata","status","notes"])

def safe_get(params):
    try:
        r = requests.get(OPEN_METEO_ARCHIVE, params=params, timeout=REQUEST_TIMEOUT, verify=certifi.where())
        r.raise_for_status()
        return r
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 429:
            raise requests.exceptions.RetryError("429")
        raise

for iata in miss:
    print("===== PROCESSANDO", iata, "=====")
    if iata not in ap.index:
        print("Sem coords para", iata, "- registrando e pulando")
        with open(LOG, "a", newline="") as f:
            csv.writer(f).writerow([iata, "no_coords", "pulado"])
        continue

    outfn = OUT_DIR / f"weather_{iata}.csv"
    if outfn.exists():
        print("Cache já existe (talvez foi baixado por outro batch). Pulando.")
        with open(LOG, "a", newline="") as f:
            csv.writer(f).writerow([iata, "cached", "ok"])
        continue

    lat = ap.loc[iata, "lat"]
    lon = ap.loc[iata, "lon"]
    months = months_between(START_DATE, END_DATE) if USE_MONTHLY else [(START_DATE, END_DATE)]
    parts = []
    failed = False
    for (sdate, edate) in months:
        params = {
            "latitude": float(lat),
            "longitude": float(lon),
            "start_date": sdate.isoformat(),
            "end_date": edate.isoformat(),
            "daily": DAILY_VARS,
            "timezone": "UTC"
        }
        backoff = 1
        for attempt in range(1, RETRY_ATTEMPTS+1):
            try:
                r = safe_get(params)
                j = r.json()
                if "daily" not in j or "time" not in j["daily"]:
                    print(f"[{iata}] resposta sem daily no mês {sdate} -> {list(j.keys())}")
                    failed = True
                    break
                dfw = pd.DataFrame({"time": j["daily"]["time"]})
                for var in j["daily"]:
                    if var == "time": continue
                    dfw[var] = j["daily"][var]
                dfw["iata"] = iata
                parts.append(dfw)
                time.sleep(SLEEP_BETWEEN_REQUESTS)
                break
            except requests.exceptions.RetryError:
                print(f"[{iata}] 429 recebido. Tentativa {attempt}/{RETRY_ATTEMPTS}. Dormindo {SLEEP_ON_429 + backoff}s.")
                time.sleep(SLEEP_ON_429 + backoff)
                backoff *= 2
            except Exception as e:
                print(f"[{iata}] Erro (attempt {attempt}):", type(e).__name__, e)
                time.sleep(2 + attempt)
        else:
            print(f"[{iata}] esgotou tentativas para o mês {sdate} -> pulando aeroporto")
            failed = True
            break

    if parts and not failed:
        df_all = pd.concat(parts, ignore_index=True)
        df_all["time"] = pd.to_datetime(df_all["time"])
        df_all.to_csv(outfn, index=False)
        print(f"[{iata}] salvo cache ({len(df_all)} linhas).")
        with open(LOG, "a", newline="") as f:
            csv.writer(f).writerow([iata, "ok", "salvo"])
    else:
        print(f"[{iata}] falhou no download completo.")
        with open(LOG, "a", newline="") as f:
            csv.writer(f).writerow([iata, "failed", "partial_or_429"])

print("Fim do resume. Veja", LOG)
