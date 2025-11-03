#!/usr/bin/env python3
"""
Listar arquivos weather_*.csv, extrair metadados e sugerir seleção diversa.
Corrigido para evitar erro de f-string (aspas internas).
Dependências: pandas
"""

import os
import glob
import pandas as pd
from collections import defaultdict

# --- configurações ---
WEATHER_DIR = "./weather_outputs"   # ajuste para a pasta onde estão os CSVs
PATTERN = os.path.join(WEATHER_DIR, "weather_*.csv")
SAMPLE_N = 12                    # default de aeroportos a sugerir (mude se desejar)

# possíveis nomes de colunas de tempo / lat / lon (case-insensitive)
TIME_COLS = {"time","timestamp","datetime","date","datetime_utc","obs_time"}
LAT_COLS = {"lat","latitude","station_lat","latitude_deg","lat_deg"}
LON_COLS = {"lon","longitude","station_lon","longitude_deg","lon_deg"}

def find_column(cols, candidates):
    """Retorna nome de coluna real (case-sensitive) presente em cols cujo lower() está em candidates."""
    mapping = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand in mapping:
            return mapping[cand]
    return None

def assign_region(lat, lon):
    """
    Heurística simples para regiões dos EUA baseada em lon/lat.
    Se lat/lon for None -> 'unknown'
    Regiões: Northeast, Southeast, Midwest, Mountain, West
    """
    if lat is None or lon is None:
        return "unknown"
    # lon esperado negativo para EUA continental
    try:
        lat = float(lat)
        lon = float(lon)
    except Exception:
        return "unknown"
    if lon > -90:
        return "Northeast" if lat >= 36 else "Southeast"
    if -105 < lon <= -90:
        return "Midwest"
    if -115 < lon <= -105:
        return "Mountain"
    return "West"

def human_size(n):
    for unit in ['B','KB','MB','GB']:
        if n < 1024.0:
            return f"{n:.1f}{unit}"
        n /= 1024.0
    return f"{n:.1f}TB"

def summarize_weather_files(pattern=PATTERN, max_preview_rows=1000):
    files = sorted(glob.glob(pattern))
    summary = []
    for fp in files:
        fname = os.path.basename(fp)
        # extrai IATA assumindo padrão weather_IATA.csv
        iata = None
        if fname.lower().startswith("weather_") and fname.lower().endswith(".csv"):
            iata = fname[len("weather_"):-4].upper()
        size_bytes = os.path.getsize(fp)
        n_rows = None
        min_time = None
        max_time = None
        lat = None
        lon = None
        error = None

        try:
            # lê só as primeiras linhas para inspecionar colunas rapidamente
            df = pd.read_csv(fp, nrows=max_preview_rows)
            # conta linhas mais precisamente (substitui tentativa anterior)
            try:
                with open(fp, 'r', encoding='utf-8', errors='ignore') as f:
                    n_rows = sum(1 for _ in f) - 1  # menos header
            except Exception:
                n_rows = None

            time_col = find_column(df.columns, TIME_COLS)
            lat_col = find_column(df.columns, LAT_COLS)
            lon_col = find_column(df.columns, LON_COLS)

            if time_col:
                try:
                    times = pd.to_datetime(df[time_col], errors="coerce")
                    if not times.dropna().empty:
                        min_time = times.min()
                        max_time = times.max()
                except Exception:
                    min_time = None
                    max_time = None

            # tenta obter lat/lon a partir das colunas se existirem
            if lat_col and lon_col:
                lat_vals = pd.to_numeric(df[lat_col], errors="coerce")
                lon_vals = pd.to_numeric(df[lon_col], errors="coerce")
                if not lat_vals.dropna().empty and not lon_vals.dropna().empty:
                    lat = float(lat_vals.dropna().iloc[0])
                    lon = float(lon_vals.dropna().iloc[0])

            # se não houver lat/lon, às vezes o IATA está na própria tabela (como no exemplo enviado)
            # e podemos deixar lat/lon = None (não crítico).
        except Exception as e:
            error = str(e)

        region = assign_region(lat, lon)
        summary.append({
            "iata": iata,
            "file": fp,
            "size_bytes": size_bytes,
            "n_rows": n_rows,
            "min_time": str(min_time) if min_time is not None else None,
            "max_time": str(max_time) if max_time is not None else None,
            "lat": lat,
            "lon": lon,
            "region": region,
            "error": error
        })
    return summary

def suggest_diverse_selection(summary, n=SAMPLE_N):
    """
    Escolhe até n aeroportos tentando cobrir o máximo de regiões:
    - round-robin por região;
    - completa pelos maiores arquivos restantes.
    """
    regions = defaultdict(list)
    unknown = []
    for item in summary:
        r = item.get("region") or "unknown"
        if r == "unknown":
            unknown.append(item)
        else:
            regions[r].append(item)

    for r in regions:
        regions[r].sort(key=lambda x: (x.get("n_rows") or 0), reverse=True)
    unknown.sort(key=lambda x: (x.get("n_rows") or 0), reverse=True)

    selected = []
    region_list = sorted(regions.keys())
    idx = 0
    while len(selected) < n and any(len(v) > idx for v in regions.values()):
        for r in region_list:
            if len(selected) >= n:
                break
            if len(regions[r]) > idx:
                selected.append(regions[r][idx])
        idx += 1

    if len(selected) < n:
        remaining = []
        for r in region_list:
            remaining += regions[r][idx:]
        remaining += unknown
        remaining.sort(key=lambda x: (x.get("n_rows") or 0), reverse=True)
        for it in remaining:
            if len(selected) >= n:
                break
            if it not in selected:
                selected.append(it)

    return selected

def print_summary(summary):
    if not summary:
        print("Nenhum arquivo encontrado com o padrão especificado.")
        return
    print(f"Encontrados {len(summary)} arquivos.\n")
    print(f"{'IATA':6} {'REGION':10} {'ROWS':8} {'SIZE':8} {'MIN_TIME':20} {'MAX_TIME':20} {'LAT':7} {'LON':8} {'FILE'}")
    print("-"*120)
    for s in summary:
        lat_str = f"{s['lat']:.3f}" if s['lat'] is not None else "-"
        lon_str = f"{s['lon']:.3f}" if s['lon'] is not None else "-"
        iata_str = (s['iata'] or '-')[:6]
        region_str = (s['region'] or '-')[:10]
        rows_str = str(s['n_rows'] or '-')
        min_t = (s['min_time'] or '-')[:19]
        max_t = (s['max_time'] or '-')[:19]
        print(f"{iata_str:6} {region_str:10} {rows_str:8} {human_size(s['size_bytes']):8} {min_t:20} {max_t:20} {lat_str:7} {lon_str:8} {os.path.basename(s['file'])}")

def print_selection(sel):
    print(f"\nSugestão de seleção ({len(sel)} aeroportos):")
    for s in sel:
        print(f"- {s['iata'] or os.path.basename(s['file'])} | região: {s['region']} | rows: {s['n_rows'] or '-'} | file: {os.path.basename(s['file'])}")

if __name__ == "__main__":
    summary = summarize_weather_files()
    print_summary(summary)

    # sugestão: N entre 5 e 20 costuma ser bom para prototipagem
    N = SAMPLE_N
    print(f"\nGerando sugestão diversa com N = {N}.")
    selection = suggest_diverse_selection(summary, n=N)
    print_selection(selection)

    # salvar resumo em CSV
    out_csv = os.path.join(WEATHER_DIR, "weather_files_summary.csv")
    try:
        pd.DataFrame(summary).to_csv(out_csv, index=False)
        print(f"\nResumo salvo em: {out_csv}")
    except Exception as e:
        print("Erro ao salvar resumo CSV:", e)
