"""
Fetch daily USD/EGP (Yahoo Finance: EGP=X) for 2021-2024, average per month,
and merge with the existing usd_egp_monthly.csv (2025-01 .. 2026-10).

Run on your own machine (needs internet):
    pip install yfinance pandas
    python fetch_usd_egp_2021_2024.py

Put this script next to usd_egp_monthly.csv (or edit EXISTING below).
Output: usd_egp_monthly_2021_2026.csv
"""
import pandas as pd
import yfinance as yf

EXISTING = "usd_egp_monthly.csv"
OUT = "usd_egp_monthly_2021_2026.csv"

# 1) daily rates 2021-2025 (2025 is only used to sanity-check against your file)
daily = yf.download("EGP=X", start="2021-01-01", end="2026-01-01",
                    interval="1d", auto_adjust=False, progress=False)
close = daily["Close"]
if isinstance(close, pd.DataFrame):
    close = close.iloc[:, 0]
close = close.dropna()
close = close[close > 0]

monthly = close.groupby(close.index.to_period("M")).mean().round(4)
monthly.index = monthly.index.astype(str)          # "2021-01"
monthly = monthly.rename("usd_egp").rename_axis("year_month").reset_index()

# 2) existing file
old = pd.read_csv(EXISTING)

# 3) sanity check: same method on 2025 vs your existing values
chk = monthly.merge(old, on="year_month", suffixes=("_calc", "_yours"))
if not chk.empty:
    chk["diff_pct"] = ((chk.usd_egp_calc / chk.usd_egp_yours - 1) * 100).round(2)
    print("Method check on overlapping months (2025):")
    print(chk.to_string(index=False))

# 4) build final file: 2021-2024 from this script + your 2025-2026 untouched
new = monthly[monthly.year_month < "2025-01"]
final = pd.concat([new, old], ignore_index=True).sort_values("year_month")
final.to_csv(OUT, index=False)
print(f"\nWrote {OUT}: {len(final)} rows, {final.year_month.min()} -> {final.year_month.max()}")
missing = sorted(set(pd.period_range("2021-01", "2026-09", freq="M").astype(str)) - set(final.year_month))
print("Missing months:", missing or "none")