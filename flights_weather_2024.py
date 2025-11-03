#!/usr/bin/env python3
"""
merge_by_origin_date.py

Lê flight_data_2024.csv (sep=';'), normaliza fl_date -> YYYY-MM-DD,
lê todos os arquivos selected_weather/weather_<IATA>.csv (com 'time' YYYY-MM-DD),
padroniza ambos como strings 'YYYY-MM-DD' e faz merge por (origin,iata) e (flight_date,time).

Saídas:
 - flights_with_origin_weather.csv  (merge left)
 - missing_weather_origins.csv      (IATA dos 20 sem arquivo)
 - missing_matches.csv              (voos que não encontraram weather após merge)
 - flight_counts_by_origin.csv      (n voos por origem)
"""
from pathlib import Path
import pandas as pd
import numpy as np

# CONFIG
FLIGHTS_CSV = Path("./flight_data_2024.csv")
WEATHER_DIR = Path("./selected_weather")
SELECTED = [
    "BOS","ALB","JFK","PHL","ATL","MIA","ORD","MSP","IAH","MSY",
    "DEN","SLC","PHX","LAS","LAX","SFO","SEA","ANC","HNL","SJU"
]
OUT_MERGED = Path("./flights_with_origin_weather.csv")
OUT_MISSING_ORIG = Path("./missing_weather_origins.csv")
OUT_MISSING_MATCHES = Path("./missing_matches.csv")
OUT_COUNTS = Path("./flight_counts_by_origin.csv")

# ---------- FUNCTIONS ----------
def read_flights(path: Path):
    # seu CSV usa ';' como separador, e fl_date está em DD/MM/YYYY no exemplo
    df = pd.read_csv(path, sep=";", engine="python", dtype=str)
    df.columns = [c.strip().replace("\ufeff","") for c in df.columns]
    return df

def normalize_flight_dates_and_origin(df: pd.DataFrame):
    # ensure fl_date present (header may be 'fl_date')
    if "fl_date" not in df.columns and "flight_date" not in df.columns:
        raise KeyError("Nenhuma coluna 'fl_date' ou 'flight_date' encontrada no CSV de voos.")
    # unify to 'flight_date' column
    if "flight_date" not in df.columns:
        df = df.rename(columns={"fl_date": "flight_date"})
    # parse to datetime with dayfirst=True (input example 01/01/2024)
    df["flight_date_parsed"] = pd.to_datetime(df["flight_date"], dayfirst=True, errors="coerce")
    # create standardized string 'YYYY-MM-DD' for exact matching
    df["flight_date_str"] = df["flight_date_parsed"].dt.strftime("%Y-%m-%d")
    # normalize origin IATA (uppercase trimmed)
    if "origin" not in df.columns:
        raise KeyError("Coluna 'origin' não encontrada no CSV de voos.")
    df["origin"] = df["origin"].astype(str).str.strip().str.upper()
    # keep original types: also parse dep_time to preserve previous behavior (optional)
    if "dep_time" in df.columns and "dep_time_raw" not in df.columns:
        df = df.rename(columns={"dep_time":"dep_time_raw"})
    # convert dep_time_raw to numeric where possible (keeps strings too)
    return df

def read_and_stack_weather(selected_iatas, weather_dir: Path):
    rows = []
    found = []
    for iata in selected_iatas:
        p = weather_dir / f"weather_{iata}.csv"
        if not p.exists():
            continue
        try:
            # weather files have column 'time' like '2024-01-01'
            w = pd.read_csv(p, dtype=str, parse_dates=["time"])
        except Exception:
            # fallback if parse_dates fails
            w = pd.read_csv(p, dtype=str)
            if "time" in w.columns:
                w["time"] = pd.to_datetime(w["time"], errors="coerce")
        if "time" in w.columns:
            w["date_str"] = pd.to_datetime(w["time"], errors="coerce").dt.strftime("%Y-%m-%d")
        elif "date" in w.columns:
            w["date_str"] = pd.to_datetime(w["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        else:
            # if no date-like column, skip this weather file (cannot join)
            continue

        # ensure iata column consistent
        if "iata" not in w.columns:
            w["iata"] = iata
        w["iata"] = w["iata"].astype(str).str.strip().str.upper()

        # keep all columns (including weather vars); add source_iata for traceability
        rows.append(w.assign(source_file=p.name))
        found.append(iata)

    if not rows:
        # empty DataFrame with minimal cols
        return pd.DataFrame(columns=["iata","date_str"]), found

    weather_all = pd.concat(rows, ignore_index=True, sort=False)
    # keep iata and date_str columns for joining, and weather columns as-is
    # ensure no duplicates in date_str formatting
    weather_all["date_str"] = weather_all["date_str"].astype(str)
    weather_all["iata"] = weather_all["iata"].astype(str).str.strip().str.upper()
    return weather_all, found

# ---------- MAIN ----------
def main():
    print("Lendo voos:", FLIGHTS_CSV)
    flights = read_flights(FLIGHTS_CSV)
    print("Colunas voos:", flights.columns.tolist())

    flights = normalize_flight_dates_and_origin(flights)
    # filter only origins in SELECTED
    flights_sel = flights[flights["origin"].isin(SELECTED)].copy()
    flights_sel["orig_iata"] = flights_sel["origin"]  # explicit column you asked

    print(f"Total voos (arquivo): {len(flights)} | Após filtro origens (20): {len(flights_sel)}")

    # read and stack weather files for the selected iatas
    print("Lendo weather para os 20 iatas em:", WEATHER_DIR)
    weather_all, found = read_and_stack_weather(SELECTED, WEATHER_DIR)
    missing_origins = sorted(set(SELECTED) - set(found))
    pd.DataFrame({"iata": missing_origins}).to_csv(OUT_MISSING_ORIG, index=False)
    print(f"Weather encontrados para {len(found)} IATA; faltam {len(missing_origins)} (salvo em {OUT_MISSING_ORIG})")

    if weather_all.empty:
        print("Nenhum weather lido. Saindo.")
        flights_sel.to_csv(OUT_MERGED, index=False)
        return

    # prepare keys as strings for both dataframes
    flights_sel["flight_date_str"] = flights_sel["flight_date_str"].astype(str)
    weather_all["date_str"] = weather_all["date_str"].astype(str)

    # Reduce weather_all to columns we want to attach (keep all weather cols)
    # Ensure unique rows per (iata,date_str) — if there are duplicates (unlikely for daily), keep first
    weather_unique = weather_all.drop_duplicates(subset=["iata","date_str"], keep="first").copy()

    # perform left merge on (origin == iata) and (flight_date_str == date_str)
    merged = flights_sel.merge(
        weather_unique,
        left_on=["origin","flight_date_str"],
        right_on=["iata","date_str"],
        how="left",
        suffixes=("","_weather")
    )

    # Count how many flights matched weather
    weather_cols = [c for c in merged.columns if c not in flights_sel.columns]  # newly added cols
    # Determine rows with at least one weather value (non-null in any added weather column)
    if weather_cols:
        has_weather = merged[weather_cols].notna().any(axis=1)
        n_matched = int(has_weather.sum())
    else:
        n_matched = 0

    print(f"Voos com weather associado: {n_matched} / {len(merged)}")

    # Save merged and missing matches (flights without weather)
    merged.to_csv(OUT_MERGED, index=False)
    merged.loc[~merged.index.isin(merged[has_weather].index)].to_csv(OUT_MISSING_MATCHES, index=False)
    # counts by origin
    flights_sel["origin"].value_counts().rename_axis("origin").reset_index(name="n_flights").to_csv(OUT_COUNTS, index=False)

    print("Arquivos gerados:")
    print(" - merged:", OUT_MERGED)
    print(" - missing weather origins:", OUT_MISSING_ORIG)
    print(" - flights without weather matches:", OUT_MISSING_MATCHES)
    print(" - counts:", OUT_COUNTS)

    # print small diagnostics
    print("\nExemplo de linhas sem weather (até 10):")
    no_weather = merged.loc[~has_weather].head(10)
    if no_weather.empty:
        print("Todas as linhas casaram com weather (ou não há colunas de weather).")
    else:
        print(no_weather[["flight_date","origin","flight_date_str","iata","date_str"]].to_string(index=False))

if __name__ == "__main__":
    main()
