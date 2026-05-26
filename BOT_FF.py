"""
IB Forward Volatility Trading Bot
HD2 Afgangsprojekt - Tobias Hagendam Ludvigsen

Krav: IB Gateway/TWS port 7497
Pakker: pip install ib_insync yfinance scipy pandas requests lxml rich python-dotenv
"""

import math, os, csv, io, sqlite3, time, logging
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.getLogger("yfinance").setLevel(logging.CRITICAL)

import requests
import yfinance as yf
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box
from ib_insync import IB, Stock, Option, Contract, ComboLeg, LimitOrder, MarketOrder
from scipy.stats import norm

# ── .env ────────────────────────────────────────────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_ENV_PATH   = os.path.join(_SCRIPT_DIR, ".env")

try:
    from dotenv import load_dotenv
    if os.path.exists(_ENV_PATH):
        load_dotenv(dotenv_path=_ENV_PATH)
        print(f"✅ .env indlæst fra: {_ENV_PATH}")
        DOTENV_OK = True
    else:
        print(f"⚠ .env fil ikke fundet på: {_ENV_PATH}")
        print("  Opret filen og tilføj: IB_ACCOUNT, EMAIL_AFSENDER, EMAIL_MODTAGER, EMAIL_PASSWORD")
        DOTENV_OK = False
except ImportError:
    DOTENV_OK = False
    print("⚠ python-dotenv ikke installeret. Kør: pip install python-dotenv")

try:
    import pandas_market_calendars as mcal
    MARKET_CAL = True
except ImportError:
    MARKET_CAL = False


# ═══════════════════════════════════════════════════════════════
# KONFIGURATION
# ═══════════════════════════════════════════════════════════════

IB_HOST      = "127.0.0.1"
IB_PORT      = 7497
IB_CLIENT_ID = 1
EXCHANGE     = "SMART"
CURRENCY     = "USD"

IB_ACCOUNT     = os.getenv("IB_ACCOUNT")
EMAIL_AFSENDER = os.getenv("EMAIL_AFSENDER")
EMAIL_MODTAGER = os.getenv("EMAIL_MODTAGER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_SMTP     = "smtp.gmail.com"
EMAIL_PORT     = 587

FF_LOWER      = 0.35
DTE_PAIRS     = [(30, 60), (30, 91), (60, 91)]
DTE_TARGETS   = [30, 60, 91]
DTE_MAX_DIFF  = 25
MAX_POSITIONS = 20
MAX_TRADES    = MAX_POSITIONS
MIN_OPEN_INT  = 500
MIN_VOLUME    = 100
MIN_SPOT      = 5.0
MIN_IV        = 0.10
RISK_FREE     = 0.045

EURUSD        = 1.12   # EUR→USD (opdater ved behov)

F_STAR        = {"30-60": 0.600, "30-91": 0.600, "60-91": 0.548}
QUARTER_KELLY = 0.25

LIMIT_TOLERANCE  = 1.30   # 30% over BS — BS undervurderer markedsprisen
FILL_TIMEOUT     = 10     # Sekunder vi venter på fill-bekræftelse

VIX_TRIGGER      = 0.10
SPOT_CHUNK_SIZE  = 200
SPOT_CHUNK_SLEEP = 3
MAX_WORKERS      = 3
OPTION_DELAY     = 1

TICKER_CACHE = "russell3000_tickers.csv"
DB_FILE      = "ff_trading.db"
SCHEDULE     = ["16:00", "18:30", "21:00"]
EMAIL_TIME   = "21:30"
TEST_MODE    = True
CSV_LOG_FILE = "ff_data_log.csv"

console = Console()


# ═══════════════════════════════════════════════════════════════
# DATO-HJÆLPEFUNKTIONER
# ═══════════════════════════════════════════════════════════════

def parse_expiry_date(exp_str: str) -> date:
    exp_str = str(exp_str).strip()
    if len(exp_str) == 8 and "-" not in exp_str:
        return date(int(exp_str[:4]), int(exp_str[4:6]), int(exp_str[6:8]))
    return date.fromisoformat(exp_str)

def ib_exp_to_iso(exp_ib: str) -> str:
    return f"{exp_ib[:4]}-{exp_ib[4:6]}-{exp_ib[6:8]}"


# ═══════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════

def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker      TEXT, dte_par TEXT, strike REAL,
            exp_front   TEXT, exp_back TEXT,
            entry_date  TEXT, entry_price REAL, contracts INTEGER,
            ff_entry    REAL, iv_front REAL, iv_back REAL, fwd_vol REAL,
            status      TEXT DEFAULT 'OPEN',
            exit_date   TEXT, exit_price REAL, pnl REAL, pnl_pct REAL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS screening_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, tickers_scanned INTEGER,
            signals_found INTEGER, orders_placed INTEGER,
            kapital REAL, duration_sec REAL
        )
    """)
    conn.commit()
    conn.close()

def save_position(ticker, dte_par, strike, exp_f, exp_b,
                  entry_price, contracts, ff, iv_f, iv_b, fwd):
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        INSERT INTO positions
        (ticker, dte_par, strike, exp_front, exp_back,
         entry_date, entry_price, contracts, ff_entry,
         iv_front, iv_back, fwd_vol)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, (ticker, dte_par, strike, exp_f, exp_b,
          date.today().isoformat(), entry_price, contracts,
          ff, iv_f, iv_b, fwd))
    conn.commit()
    conn.close()

def get_open_positions():
    conn = sqlite3.connect(DB_FILE)
    rows = conn.execute("""
        SELECT ticker, dte_par, strike, exp_front, exp_back,
               entry_date, entry_price, contracts, ff_entry,
               iv_front, iv_back, id
        FROM positions WHERE status = 'OPEN'
        ORDER BY ff_entry DESC
    """).fetchall()
    conn.close()
    return rows

def close_position_db(pos_id, exit_price, entry_price, contracts):
    pnl     = (exit_price - entry_price) * contracts * 100
    pnl_pct = (exit_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        UPDATE positions SET status='CLOSED',
        exit_date=?, exit_price=?, pnl=?, pnl_pct=? WHERE id=?
    """, (date.today().isoformat(), exit_price, pnl, pnl_pct, pos_id))
    conn.commit()
    conn.close()
    return pnl, pnl_pct

def log_screening(n, signals, orders, kapital, duration):
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        INSERT INTO screening_log
        (timestamp, tickers_scanned, signals_found,
         orders_placed, kapital, duration_sec)
        VALUES (?,?,?,?,?,?)
    """, (datetime.now().isoformat(), n, signals, orders, kapital, duration))
    conn.commit()
    conn.close()

def sync_db_with_ib(ib):
    """Fjerner DB-positioner der ikke eksisterer i IB."""
    ib_tickers = get_ib_open_tickers(ib)
    db_pos     = get_open_positions()
    fjernet    = 0
    for pos in db_pos:
        ticker = pos[0]
        pos_id = pos[11]
        if ticker not in ib_tickers:
            conn = sqlite3.connect(DB_FILE)
            conn.execute(
                "UPDATE positions SET status='SYNC_REMOVED' WHERE id=?",
                (pos_id,)
            )
            conn.commit()
            conn.close()
            console.print(f"[dim]🔄 Sync: {ticker} fjernet fra DB (ikke i IB)[/]")
            fjernet += 1
    if fjernet:
        console.print(f"[yellow]⚠ DB sync: {fjernet} positioner fjernet[/]")
    return fjernet


# ═══════════════════════════════════════════════════════════════
# CSV LOG
# ═══════════════════════════════════════════════════════════════

def log_to_csv(results, vix, vix_chg, kapital, db_pos_count):
    write_hdr = not os.path.exists(CSV_LOG_FILE)
    ts        = datetime.now().strftime("%Y-%m-%d %H:%M")
    with open(CSV_LOG_FILE, "a", newline="", encoding="utf-8") as f:
        fields = ["timestamp","dato","ugedag","ticker","par",
                  "dte_front_faktisk","dte_back_faktisk",
                  "exp_front","exp_back","spot","strike","net_debit",
                  "iv_front","iv_back","fwd_vol","ff",
                  "open_interest_front","signal",
                  "vix","vix_daglig_chg","kapital","aabne_positioner"]
        w = csv.DictWriter(f, fieldnames=fields)
        if write_hdr:
            w.writeheader()
        for r in results:
            w.writerow({
                "timestamp":           ts,
                "dato":                date.today().isoformat(),
                "ugedag":              date.today().strftime("%A"),
                "ticker":              r["ticker"],
                "par":                 r["par"],
                "dte_front_faktisk":   r.get("dte_f",""),
                "dte_back_faktisk":    r.get("dte_b",""),
                "exp_front":           r.get("exp_f",""),
                "exp_back":            r.get("exp_b",""),
                "spot":                round(r["spot"],2),
                "strike":              r["strike"],
                "net_debit":           round(r.get("net_debit",0),2),
                "iv_front":            round(r["iv_f"],4),
                "iv_back":             round(r["iv_b"],4),
                "fwd_vol":             round(r["fwd"],4) if r["fwd"] else "",
                "ff":                  round(r["ff"],4),
                "open_interest_front": r.get("oi_f",""),
                "signal":              "JA" if r["signal"] else "NEJ",
                "vix":                 round(vix,2),
                "vix_daglig_chg":      round(vix_chg,4),
                "kapital":             round(kapital,2),
                "aabne_positioner":    db_pos_count,
            })
    console.print(f"[dim]📊 {len(results)} observationer → {CSV_LOG_FILE}[/]")


# ═══════════════════════════════════════════════════════════════
# EMAIL
# ═══════════════════════════════════════════════════════════════

def send_daily_email(resultater, vix, kapital):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    dag      = date.today().strftime("%d. %B %Y")
    db_pos   = get_open_positions()
    signaler = [r for r in resultater if r.get("signal")]
    today    = date.today()

    pos_rows = ""
    for pos in db_pos:
        (ticker,dte_par,strike,exp_f,exp_b,
         entry_date,entry_price,contracts,ff,iv_f,iv_b,pos_id) = pos
        dte_left = (parse_expiry_date(exp_f) - today).days
        farve    = "#e74c3c" if dte_left <= 5 else "#2ecc71"
        pos_rows += (f"<tr><td><b>{ticker}</b></td><td>{dte_par}</td>"
                     f"<td>${strike:.0f}</td><td>{entry_date}</td>"
                     f"<td>${entry_price:.2f}</td><td>{contracts}</td>"
                     f"<td>{ff:.3f}</td>"
                     f"<td style='color:{farve}'><b>{dte_left}d</b></td></tr>")

    sig_rows = ""
    for r in signaler[:10]:
        sig_rows += (f"<tr><td><b style='color:#27ae60'>{r['ticker']}</b></td>"
                     f"<td>{r['par']}</td><td>${r['spot']:.0f}</td>"
                     f"<td>{r['iv_f']:.1%}</td><td>{r['iv_b']:.1%}</td>"
                     f"<td>{r['fwd']:.1%}</td><td><b>{r['ff']:.4f}</b></td></tr>")

    html = f"""<html><body style="font-family:Arial;max-width:800px;margin:auto;">
    <h1 style="color:#1a5276;border-bottom:2px solid #1a5276;">
        📈 FF Trading Bot — {dag}</h1>
    <table style="width:100%;background:#f0f4f8;padding:15px;margin-bottom:20px;"><tr>
        <td><b>Kapital:</b> ${kapital:,.0f}</td>
        <td><b>VIX:</b> {vix:.1f}</td>
        <td><b>Positioner:</b> {len(db_pos)}/{MAX_POSITIONS}</td>
        <td><b>Signaler:</b> {len(signaler)}</td>
    </tr></table>
    <h2>📋 Åbne Positioner</h2>
    <table border="1" cellpadding="8" style="border-collapse:collapse;width:100%;">
        <tr style="background:#1a5276;color:white;">
            <th>Ticker</th><th>Par</th><th>Strike</th><th>Entry</th>
            <th>Pris</th><th>Kontr.</th><th>FF</th><th>DTE</th>
        </tr>{pos_rows}</table>
    <h2>🟢 Signaler (FF > {FF_LOWER})</h2>
    <table border="1" cellpadding="8" style="border-collapse:collapse;width:100%;">
        <tr style="background:#27ae60;color:white;">
            <th>Ticker</th><th>Par</th><th>Spot</th>
            <th>IV_f</th><th>IV_b</th><th>Fwd</th><th>FF</th>
        </tr>{sig_rows}</table>
    <p style="color:#999;font-size:12px;">
        FF Trading Bot | HD2 Afgangsprojekt | Tobias Hagendam Ludvigsen
    </p></body></html>"""

    try:
        msg            = MIMEMultipart("alternative")
        msg["Subject"] = (f"📈 FF Bot — {dag} | "
                          f"{len(signaler)} signaler | ${kapital:,.0f}")
        msg["From"]    = EMAIL_AFSENDER
        msg["To"]      = EMAIL_MODTAGER
        msg.attach(MIMEText(html, "html", "utf-8"))
        with smtplib.SMTP(EMAIL_SMTP, EMAIL_PORT) as s:
            s.starttls()
            s.login(EMAIL_AFSENDER, EMAIL_PASSWORD)
            s.sendmail(EMAIL_AFSENDER, EMAIL_MODTAGER, msg.as_string())
        console.print(f"[green]📧 Email sendt til {EMAIL_MODTAGER}[/]")
    except Exception as e:
        console.print(f"[red]❌ Email fejl: {e}[/]")

def should_send_email(last_email):
    now   = datetime.now()
    today = date.today()
    h, m  = map(int, EMAIL_TIME.split(":"))
    t     = datetime(today.year, today.month, today.day, h, m)
    return (abs((now - t).total_seconds()) <= 300 and
            (last_email is None or last_email.date() != today))


# ═══════════════════════════════════════════════════════════════
# TICKER UNIVERS
# ═══════════════════════════════════════════════════════════════

FALLBACK = [
    "AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","AMD","INTC",
    "MU","QCOM","AVGO","CRM","ADBE","ORCL","SNOW","PLTR","JPM","GS",
    "MS","BAC","V","MA","JNJ","PFE","ABBV","LLY","XOM","CVX",
    "WMT","COST","NFLX","DIS","SPY","QQQ","IWM","GLD","TLT",
]

def load_universe():
    if os.path.exists(TICKER_CACHE):
        mtime = datetime.fromtimestamp(os.path.getmtime(TICKER_CACHE))
        if mtime.date() == date.today():
            tickers = pd.read_csv(TICKER_CACHE)["ticker"].tolist()
            console.print(f"  ✅ {len(tickers):,} tickers fra cache")
            return tickers

    console.print("  Henter Russell 3000 fra iShares...")
    tickers = set()
    try:
        url = ("https://www.ishares.com/us/products/239714/"
               "ishares-russell-3000-etf/1467271812596.ajax"
               "?tab=holdings&fileType=csv")
        r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            lines = r.text.splitlines()
            start = next((i for i,l in enumerate(lines)
                          if "Ticker" in l or "ticker" in l), 0)
            df  = pd.read_csv(io.StringIO("\n".join(lines[start:])),
                              on_bad_lines="skip")
            col = next((c for c in df.columns if "ticker" in c.lower()), None)
            if col:
                t = df[col].dropna().astype(str).str.strip().str.upper()
                tickers.update(t[t.str.isalpha() & (t.str.len() <= 5)].tolist())
    except Exception as e:
        console.print(f"  [yellow]iShares fejl: {e}[/]")

    try:
        sp = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
        tickers.update(
            sp["Symbol"].str.replace(".", "-", regex=False)
            .str.strip().str.upper().tolist()
        )
    except Exception as e:
        console.print(f"  [yellow]Wikipedia fejl: {e}[/]")

    tickers = sorted([
        t for t in tickers
        if isinstance(t, str) and not t.startswith("$")
        and t.replace("-","").isalpha() and 1 <= len(t) <= 5
        and t not in ("N/A","NA","NAN","CASH")
    ])

    if tickers:
        pd.DataFrame({"ticker": tickers}).to_csv(TICKER_CACHE, index=False)
        console.print(f"  ✅ {len(tickers):,} tickers gemt")
    else:
        tickers = FALLBACK
    return tickers


# ═══════════════════════════════════════════════════════════════
# MARKEDSDATA
# ═══════════════════════════════════════════════════════════════

def get_spot(ticker):
    try:
        h = yf.Ticker(ticker).history(period="1d", interval="5m")
        if h.empty:
            h = yf.Ticker(ticker).history(period="2d")
        return float(h["Close"].iloc[-1]) if not h.empty else None
    except Exception as e:
        console.print(f"[dim]⚠ get_spot({ticker}): {e}[/]")
        return None

def get_vix():
    try:
        h = yf.Ticker("^VIX").history(period="2d")
        if len(h) >= 2:
            return (float(h["Close"].iloc[-1]),
                    (float(h["Close"].iloc[-1]) - float(h["Close"].iloc[-2])) /
                    float(h["Close"].iloc[-2]))
        return float(h["Close"].iloc[-1]), 0.0
    except Exception as e:
        console.print(f"[dim]⚠ get_vix: {e}[/]")
        return 0.0, 0.0

def get_yf_expiries(ticker):
    try:
        return yf.Ticker(ticker).options
    except Exception as e:
        console.print(f"[dim]⚠ get_yf_expiries({ticker}): {e}[/]")
        return []

def find_nearest_expiry(expiries, target_dte):
    today = date.today()
    best, best_diff = None, float("inf")
    for d in expiries:
        try:
            dte  = (date.fromisoformat(d) - today).days
            diff = abs(dte - target_dte)
            if diff < best_diff:
                best_diff, best = diff, (d, dte)
        except Exception:
            continue
    return best if best and best_diff <= DTE_MAX_DIFF else None

def get_atm_call(ticker, expiry_str, spot):
    try:
        chain = yf.Ticker(ticker).option_chain(expiry_str)
        calls = chain.calls.copy()
        if calls.empty:
            return None
        calls = calls[
            (calls["openInterest"].fillna(0) >= MIN_OPEN_INT) |
            (calls["volume"].fillna(0) >= MIN_VOLUME)
        ]
        if calls.empty:
            return None
        calls["dist"] = (calls["strike"] - spot).abs()
        atm = calls.loc[calls["dist"].idxmin()]
        if abs(float(atm["strike"]) - spot) / spot > 0.20:
            return None
        iv = float(atm["impliedVolatility"])
        if iv <= MIN_IV:
            return None
        return {
            "strike": float(atm["strike"]),
            "iv":     iv,
            "oi":     int(atm.get("openInterest", 0) or 0),
        }
    except Exception as e:
        console.print(f"[dim]⚠ get_atm_call({ticker} {expiry_str}): {e}[/]")
        return None


# ═══════════════════════════════════════════════════════════════
# FF BEREGNING
# ═══════════════════════════════════════════════════════════════

def fwd_vol(iv_f, t_f, iv_b, t_b, ticker=""):
    den = t_b - t_f
    if den <= 0:
        return None
    num = (iv_b**2)*t_b - (iv_f**2)*t_f
    if num <= 0:
        return None
    return math.sqrt(num / den)

def calc_ff(iv_f, fv):
    return (iv_f - fv) / fv if fv and fv > 0 else None


# ═══════════════════════════════════════════════════════════════
# BLACK-SCHOLES
# ═══════════════════════════════════════════════════════════════

def bs_call(S, K, T, r, iv):
    if T <= 0 or iv <= 0:
        return max(S - K, 0)
    d1 = (math.log(S/K) + (r + 0.5*iv**2)*T) / (iv*math.sqrt(T))
    d2 = d1 - iv*math.sqrt(T)
    return S*norm.cdf(d1) - K*math.exp(-r*T)*norm.cdf(d2)

def net_debit_bs(spot, strike, dte_f, dte_b, iv_f, iv_b):
    front = bs_call(spot, strike, dte_f/365, RISK_FREE, iv_f)
    back  = bs_call(spot, strike, dte_b/365, RISK_FREE, iv_b)
    return max(back - front, 0.05)


# ═══════════════════════════════════════════════════════════════
# KELLY
# ═══════════════════════════════════════════════════════════════

def kelly_contracts(kapital, net_debit, dte_par, n_open=0):
    """
    Quarter Kelly:
      f_eff      = f* × 0.25
      allokering = kapital × f_eff / MAX_POSITIONS
      kontrakter = allokering / (net_debit × 100)
    """
    f_eff      = F_STAR.get(dte_par, 0.600) * QUARTER_KELLY
    allokering = kapital * f_eff / MAX_POSITIONS
    if net_debit <= 0:
        return 1
    return max(int(allokering / (net_debit * 100)), 1)


# ═══════════════════════════════════════════════════════════════
# BATCH SPOT-PRISER
# ═══════════════════════════════════════════════════════════════

def batch_spots(tickers: list) -> dict:
    spots  = {}
    chunks = [tickers[i:i+SPOT_CHUNK_SIZE]
              for i in range(0, len(tickers), SPOT_CHUNK_SIZE)]

    console.print(
        f"[dim]📥 Spot-batches: {len(tickers):,} tickers → "
        f"{len(chunks)} batches à {SPOT_CHUNK_SIZE}...[/]"
    )

    for idx, chunk in enumerate(chunks):
        try:
            data  = yf.download(chunk, period="1d", interval="5m",
                                 auto_adjust=True, progress=False,
                                 threads=False, multi_level_index=False)
            close = data.get("Close", pd.DataFrame())
            if close.empty:
                time.sleep(SPOT_CHUNK_SLEEP)
                continue

            if isinstance(close, pd.Series):
                val = float(close.dropna().iloc[-1])
                if val >= MIN_SPOT:
                    spots[chunk[0]] = val
            else:
                last_row = close.ffill().iloc[-1].dropna()
                valid    = last_row[last_row >= MIN_SPOT]
                spots.update(valid.to_dict())

            console.print(
                f"[dim]  Batch {idx+1:>2}/{len(chunks)}: "
                f"{len([t for t in chunk if t in spots]):>3} priser [/]",
                end="\r"
            )
        except Exception as e:
            console.print(f"\n[dim]⚠ Spot-batch {idx+1} fejl: {e}[/]")
            time.sleep(10)

        if idx < len(chunks) - 1:
            time.sleep(SPOT_CHUNK_SLEEP)

    console.print(
        f"\n[dim]✅ Spot-priser hentet: "
        f"{len(spots):,}/{len(tickers):,} aktier over ${MIN_SPOT}[/]"
    )
    return spots


# ═══════════════════════════════════════════════════════════════
# SCREEN EN AKTIE
# ═══════════════════════════════════════════════════════════════

def screen_ticker(ticker, spot=None):
    if spot is None:
        spot = get_spot(ticker)
    if not spot or spot < MIN_SPOT:
        return []

    time.sleep(OPTION_DELAY)
    expiries = get_yf_expiries(ticker)
    if not expiries:
        return []

    results = []
    exp_map = {}
    for target in DTE_TARGETS:
        res = find_nearest_expiry(expiries, target)
        if res:
            exp_map[target] = res

    for dte_f_t, dte_b_t in DTE_PAIRS:
        front = exp_map.get(dte_f_t)
        back  = exp_map.get(dte_b_t)
        if not front or not back:
            continue

        exp_f, dte_f = front
        exp_b, dte_b = back

        if exp_f == exp_b:
            continue

        time.sleep(OPTION_DELAY)
        opt_f = get_atm_call(ticker, exp_f, spot)
        time.sleep(OPTION_DELAY)
        opt_b = get_atm_call(ticker, exp_b, spot)

        if not opt_f or not opt_b:
            continue

        iv_f = opt_f["iv"]
        iv_b = opt_b["iv"]
        t_f  = dte_f / 365
        t_b  = dte_b / 365

        fv = fwd_vol(iv_f, t_f, iv_b, t_b, ticker)
        if fv and fv < 0.05:
            continue

        ff        = calc_ff(iv_f, fv) if fv else -99.0
        signal    = ff > FF_LOWER
        net_debit = net_debit_bs(spot, opt_f["strike"], dte_f, dte_b, iv_f, iv_b)

        results.append({
            "ticker":    ticker,
            "par":       f"{dte_f_t}-{dte_b_t}",
            "spot":      spot,
            "strike":    opt_f["strike"],
            "exp_f":     exp_f,
            "exp_b":     exp_b,
            "dte_f":     dte_f,
            "dte_b":     dte_b,
            "iv_f":      iv_f,
            "iv_b":      iv_b,
            "fwd":       fv or 0,
            "ff":        ff,
            "signal":    signal,
            "net_debit": net_debit,
            "oi_f":      opt_f["oi"],
        })
    return results


# ═══════════════════════════════════════════════════════════════
# IB — STRIKES OG ORDRER
# ═══════════════════════════════════════════════════════════════

def get_ib_atm_strike(ib, ticker, spot):
    try:
        stock = Stock(ticker, EXCHANGE, CURRENCY)
        ib.qualifyContracts(stock)
        chains = ib.reqSecDefOptParams(stock.symbol, "", stock.secType, stock.conId)
        if not chains:
            return None, None, None
        chain       = next((c for c in chains if c.exchange == "SMART"), chains[0])
        strikes     = sorted(chain.strikes)
        expirations = sorted(chain.expirations)
        atm_strike  = min(strikes, key=lambda k: abs(k - spot))
        return atm_strike, expirations, strikes
    except Exception as e:
        console.print(f"[dim]⚠ get_ib_atm_strike({ticker}): {e}[/]")
        return None, None, None

def find_common_atm_strike(ib_strikes, spot, exp_f_ib, exp_b_ib, ticker, ib):
    atm_idx    = min(range(len(ib_strikes)), key=lambda i: abs(ib_strikes[i] - spot))
    candidates = [atm_idx] + [atm_idx + d for d in [1,-1,2,-2,3,-3]]
    for idx in candidates:
        if not (0 <= idx < len(ib_strikes)):
            continue
        strike = ib_strikes[idx]
        try:
            c_f = Option(ticker, exp_f_ib, strike, "C", EXCHANGE)
            ib.qualifyContracts(c_f)
            c_b = Option(ticker, exp_b_ib, strike, "C", EXCHANGE)
            ib.qualifyContracts(c_b)
            return strike
        except Exception:
            continue
    console.print(f"[dim]⚠ {ticker}: Ingen fælles strike fundet[/]")
    return None

def find_ib_expiry(expirations, target_dte):
    today = date.today()
    best, best_diff = None, float("inf")
    for exp in expirations:
        try:
            exp_date = parse_expiry_date(exp)
            dte      = (exp_date - today).days
            diff     = abs(dte - target_dte)
            if diff < best_diff:
                best_diff, best = diff, (exp, dte)
        except Exception:
            continue
    return best if best and best_diff <= DTE_MAX_DIFF else None

def get_ib_contract(ib, ticker, expiry_ib, strike):
    try:
        c = Option(ticker, expiry_ib, strike, "C", EXCHANGE)
        ib.qualifyContracts(c)
        return c
    except Exception as e:
        console.print(f"    ⚠️  Kontrakt fejl ({ticker} {expiry_ib} ${strike}): {e}")
        return None

def place_order(ib, ticker, c_f, c_b, net_debit, n):
    """
    Limit calendar spread ordre.
    Venter FILL_TIMEOUT sekunder på fill-bekræftelse.
    """
    try:
        bag           = Contract()
        bag.symbol    = ticker
        bag.secType   = "BAG"
        bag.currency  = CURRENCY
        bag.exchange  = EXCHANGE
        leg1          = ComboLeg()
        leg1.conId    = c_f.conId
        leg1.ratio    = 1
        leg1.action   = "SELL"
        leg1.exchange = EXCHANGE
        leg2          = ComboLeg()
        leg2.conId    = c_b.conId
        leg2.ratio    = 1
        leg2.action   = "BUY"
        leg2.exchange = EXCHANGE
        bag.comboLegs = [leg1, leg2]

        limit_price   = round(net_debit * LIMIT_TOLERANCE, 2)
        order         = LimitOrder("BUY", n, limit_price)
        order.account = IB_ACCOUNT
        order.tif     = "DAY"
        order.transmit= True

        trade = ib.placeOrder(bag, order)

        for _ in range(FILL_TIMEOUT):
            ib.sleep(1)
            status = trade.orderStatus.status
            fill   = trade.orderStatus.avgFillPrice
            if status == "Filled":
                console.print(f"   [dim]✅ Fyldt til ${fill:.3f} (limit: ${limit_price:.2f})[/]")
                return status, fill
            if status in ("Cancelled", "ApiCancelled", "Inactive"):
                return status, 0.0

        status = trade.orderStatus.status
        console.print(f"   [dim]⏳ Ikke fyldt efter {FILL_TIMEOUT}s ({status}) — annullerer[/]")
        ib.cancelOrder(order)
        return "NotFilled", 0.0

    except Exception as e:
        console.print(f"[red]❌ place_order({ticker}): {e}[/]")
        return f"FEJL: {e}", 0


# ═══════════════════════════════════════════════════════════════
# KAPITAL OG POSITIONER
# ═══════════════════════════════════════════════════════════════

def get_kapital(ib, verbose=False):
    try:
        vals    = ib.accountValues(IB_ACCOUNT)
        nl_vals = [v for v in vals if v.tag == "NetLiquidation"]
        if verbose and nl_vals:
            for v in nl_vals:
                console.print(f"[dim]   Kapital: {v.tag} = {v.value} {v.currency}[/]")
        for currency in ["BASE", "USD", ""]:
            for v in vals:
                if v.tag == "NetLiquidation":
                    if currency == "" or v.currency == currency:
                        try:
                            val = float(v.value)
                            if val > 0:
                                if v.currency == "EUR":
                                    val_usd = val * EURUSD
                                    if verbose:
                                        console.print(
                                            f"[dim]   Kapital: €{val:,.0f} EUR "
                                            f"→ ${val_usd:,.0f} USD (×{EURUSD})[/]"
                                        )
                                    return val_usd
                                return val
                        except Exception:
                            pass
        for v in vals:
            if v.tag == "TotalCashValue":
                try:
                    val = float(v.value)
                    if val > 0:
                        return val
                except Exception:
                    pass
    except Exception as e:
        console.print(f"[red]❌ get_kapital fejl: {e}[/]")
    console.print("[red]❌ Kapital ikke tilgængelig[/]")
    return 0.0

def get_ib_open_tickers(ib):
    try:
        return {p.contract.symbol
                for p in ib.positions(IB_ACCOUNT)
                if p.contract.secType in ("OPT","BAG")}
    except Exception as e:
        console.print(f"[dim]⚠ get_ib_open_tickers: {e}[/]")
        return set()

def estimate_back_value(ticker, strike, exp_b_str, iv_b_stored):
    spot = get_spot(ticker)
    if not spot:
        return 0.0
    exp_b_date    = parse_expiry_date(exp_b_str)
    dte_remaining = (exp_b_date - date.today()).days
    if dte_remaining <= 0:
        return max(spot - strike, 0.0)
    return bs_call(spot, strike, dte_remaining/365, RISK_FREE, iv_b_stored)

def check_expired():
    positions = get_open_positions()
    closed    = []
    today     = date.today()
    for pos in positions:
        (ticker,dte_par,strike,exp_f,exp_b,
         entry_date,entry_price,contracts,ff,iv_f,iv_b,pos_id) = pos
        exp_f_date = parse_expiry_date(exp_f)
        if today >= exp_f_date:
            exit_price = estimate_back_value(ticker, strike, exp_b, iv_b)
            pnl, pct   = close_position_db(pos_id, exit_price, entry_price, contracts)
            closed.append({"ticker": ticker, "par": dte_par,
                           "pnl": pnl, "pnl_pct": pct, "exit_price": exit_price})
    return closed


# ═══════════════════════════════════════════════════════════════
# SCHEDULING
# ═══════════════════════════════════════════════════════════════

def market_is_open_today():
    if TEST_MODE:
        return True
    today = date.today()
    if today.weekday() >= 5:
        return False
    if not MARKET_CAL:
        return True
    try:
        nyse = mcal.get_calendar("NYSE")
        s    = nyse.schedule(start_date=today.isoformat(), end_date=today.isoformat())
        return not s.empty
    except Exception:
        return today.weekday() < 5

def should_stop():
    return datetime.now().hour >= 23

def next_run_time():
    now   = datetime.now()
    today = date.today()
    for t in SCHEDULE:
        h, m = map(int, t.split(":"))
        run  = datetime(today.year, today.month, today.day, h, m)
        if run > now:
            return run.strftime("%H:%M")
    return "16:00 (i morgen)"

def should_run_now(last_run):
    now   = datetime.now()
    today = date.today()
    for t in SCHEDULE:
        h, m = map(int, t.split(":"))
        run  = datetime(today.year, today.month, today.day, h, m)
        if abs((now - run).total_seconds()) <= 300:
            already = (last_run is not None and
                       (now - last_run).total_seconds() < 600)
            return not already
    if TEST_MODE:
        return last_run is None
    return False


# ═══════════════════════════════════════════════════════════════
# SCREENING
# ═══════════════════════════════════════════════════════════════

def run_screening(ib, universe, kapital, vix=0, vix_chg=0):
    start = time.time()

    if kapital <= 0:
        console.print("[red]❌ Ingen kapital — screening afbrudt[/]")
        return []

    console.rule("[cyan]🔍 Starter screening[/]")
    console.print(
        f"[dim]{datetime.now().strftime('%H:%M:%S')}[/] "
        f"Screener [cyan]{len(universe):,}[/] aktier..."
    )

    spot_cache = batch_spots(universe)
    kandidater = [t for t in universe if t in spot_cache]
    console.print(
        f"[dim]{len(kandidater):,} kandidater med gyldig spot "
        f"(filtreret fra {len(universe):,})[/]"
    )

    all_results, errors, done = [], [], 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(screen_ticker, t, spot_cache.get(t)): t
                   for t in kandidater}
        for future in as_completed(futures):
            ticker = futures[future]
            done  += 1
            try:
                res = future.result()
                if res:
                    all_results.extend(res)
                    best = max(res, key=lambda x: x["ff"])
                    if best["signal"]:
                        console.print(
                            f"  [green]🟢 [{done:04d}] {ticker:<6} FF={best['ff']:+.4f} ({best['par']})[/]"
                        )
                    elif done % 200 == 0:
                        console.print(
                            f"  [dim][{done:04d}/{len(kandidater)}] "
                            f"{ticker:<6} FF={best['ff']:+.4f}[/]"
                        )
                else:
                    errors.append(ticker)
            except Exception as e:
                console.print(f"[dim]⚠ Screening fejl ({ticker}): {e}[/]")
                errors.append(ticker)

    valid   = sorted([r for r in all_results if r["ff"] != -99],
                     key=lambda x: x["ff"], reverse=True)
    signals = [r for r in valid if r["signal"]]

    tbl = Table(
        box=box.SIMPLE_HEAD,
        title=f"TOP 15  |  {len(valid)} aktier  |  [green]{len(signals)} signaler[/]",
        title_style="bold white",
    )
    for col in ["Ticker","Par","Spot","IV_f","IV_b","Fwd","FF","OI","Net debit (BS)","Status"]:
        tbl.add_column(col)
    for r in valid[:15]:
        ff_col = "green" if r["signal"] else ("yellow" if r["ff"] > 0.20 else "white")
        flag   = "🟢 SIGNAL" if r["signal"] else ("⚠ tæt på" if r["ff"] > 0.20 else "")
        tbl.add_row(
            r["ticker"], r["par"], f"${r['spot']:.0f}",
            f"{r['iv_f']:.1%}", f"{r['iv_b']:.1%}", f"{r['fwd']:.1%}",
            f"[{ff_col}]{r['ff']:+.4f}[/{ff_col}]",
            str(r.get("oi_f",0)), f"${r['net_debit']:.2f}", flag
        )
    console.print(tbl)

    db_pos = get_open_positions()
    if db_pos:
        pt = Table(box=box.SIMPLE_HEAD, title="Åbne Positioner", title_style="bold white")
        for col in ["Ticker","Par","Strike","Entry","Pris","Kontr","FF","DTE"]:
            pt.add_column(col)
        today = date.today()
        for pos in db_pos:
            (ticker,dte_par,strike,exp_f,exp_b,
             entry_date,entry_price,contracts,ff,iv_f,iv_b,pos_id) = pos
            dte_left = (parse_expiry_date(exp_f) - today).days
            dc = "red" if dte_left <= 3 else "white"
            pt.add_row(ticker, dte_par, f"${strike:.0f}", entry_date,
                       f"${entry_price:.2f}", str(contracts),
                       f"{ff:.3f}", f"[{dc}]{dte_left}d[/{dc}]")
        console.print(pt)

    ib_open   = get_ib_open_tickers(ib)
    db_open   = {pos[0] for pos in db_pos}
    open_set  = ib_open | db_open
    pos_count = len(db_pos)
    placed    = 0

    for r in signals[:MAX_POSITIONS]:
        if r["ticker"] in open_set:
            existing_ff     = next((p[8] for p in db_pos if p[0] == r["ticker"]), None)
            existing_ff_str = f"{existing_ff:.3f}" if existing_ff is not None else "N/A"
            diff = r["ff"] - (existing_ff or 0)
            if diff > 0.05:
                console.print(f"[yellow]⚡ {r['ticker']} åben (FF={existing_ff_str}) "
                               f"→ nyt FF={r['ff']:.3f} ({diff:+.3f})[/]")
            else:
                console.print(f"[dim]⏭ {r['ticker']} åben (FF={existing_ff_str})[/]")
            continue

        if pos_count >= MAX_POSITIONS:
            console.print(f"[yellow]⚠ Max {MAX_POSITIONS} positioner[/]")
            break

        ib_strike_approx, ib_exps, ib_strikes = get_ib_atm_strike(ib, r["ticker"], r["spot"])
        if not ib_strike_approx:
            console.print(f"   [yellow]⚠ {r['ticker']}: Ingen IB chain[/]")
            continue

        ib_f = find_ib_expiry(ib_exps, r["dte_f"])
        ib_b = find_ib_expiry(ib_exps, r["dte_b"])
        if not ib_f or not ib_b:
            console.print(f"   [yellow]⚠ {r['ticker']}: Ingen IB expiry match[/]")
            continue

        exp_ib_f, dte_ib_f = ib_f
        exp_ib_b, dte_ib_b = ib_b

        ib_strike = find_common_atm_strike(ib_strikes, r["spot"], exp_ib_f, exp_ib_b,
                                            r["ticker"], ib)
        if not ib_strike:
            console.print(f"   [yellow]⚠ {r['ticker']}: Ingen fælles strike[/]")
            continue

        net_d = net_debit_bs(r["spot"], ib_strike, r["dte_f"], r["dte_b"],
                              r["iv_f"], r["iv_b"])
        n_con = kelly_contracts(kapital, net_d, r["par"], n_open=pos_count)
        f_eff = F_STAR.get(r["par"], 0.600) * QUARTER_KELLY

        console.print(
            f"\n[bold green]🟢 {r['ticker']} — {r['par']} DTE[/]\n"
            f"   Spot: ${r['spot']:.2f} | Strike IB: ${ib_strike:.0f} | OI: {r.get('oi_f',0):,}\n"
            f"   IV_f: {r['iv_f']:.2%} | IV_b: {r['iv_b']:.2%} | "
            f"Fwd: {r['fwd']:.2%} | FF: {r['ff']:.4f}\n"
            f"   Net debit (BS): ${net_d:.2f} | Kelly f_eff={f_eff:.3f} → {n_con}x "
            f"(${n_con*net_d*100:,.0f}) | Limit: ${round(net_d*LIMIT_TOLERANCE,2):.2f}"
        )

        c_f = get_ib_contract(ib, r["ticker"], exp_ib_f, ib_strike)
        c_b = get_ib_contract(ib, r["ticker"], exp_ib_b, ib_strike)

        if c_f and c_b:
            status, fill_px = place_order(ib, r["ticker"], c_f, c_b, net_d, n_con)
            if status == "Filled" and fill_px > 0:
                save_position(r["ticker"], r["par"], ib_strike,
                              ib_exp_to_iso(exp_ib_f), ib_exp_to_iso(exp_ib_b),
                              fill_px, n_con, r["ff"],
                              r["iv_f"], r["iv_b"], r["fwd"])
                placed    += 1
                pos_count += 1
                open_set.add(r["ticker"])
                console.print(f"   [green]✅ FYLDT: {n_con}x {r['ticker']} @ ${fill_px:.3f}[/]")
            elif status == "NotFilled":
                console.print(f"   [yellow]⏳ {r['ticker']}: Ikke fyldt — limit for lav[/]")
            else:
                console.print(f"   [red]❌ Ordre afvist ({status}): {r['ticker']}[/]")
        else:
            console.print(f"   [yellow]⚠ {r['ticker']}: Kontrakter utilgængelige[/]")

    log_to_csv(valid, vix, vix_chg, kapital, db_pos_count=len(db_pos))
    duration = time.time() - start
    log_screening(len(kandidater), len(signals), placed, kapital, duration)

    console.rule(
        f"[cyan]Færdig: {len(valid)} aktier | {len(signals)} signaler | "
        f"{placed} ordrer | {duration:.0f}s[/]"
    )
    return valid


# ═══════════════════════════════════════════════════════════════
# HOVEDPROGRAM
# ═══════════════════════════════════════════════════════════════

def run():
    init_db()

    if not DOTENV_OK:
        console.print(f"[yellow]⚠ Opret .env fil i: {_SCRIPT_DIR}[/]")
    if not EMAIL_PASSWORD:
        console.print("[yellow]⚠ EMAIL_PASSWORD ikke sat i .env — emails vil fejle[/]")

    console.print(Panel(
        f"[bold cyan]FF Forward Volatility Trading Bot[/]\n"
        f"Konto: [yellow]{IB_ACCOUNT}[/]  Signal: FF > {FF_LOWER}  "
        f"Max: {MAX_POSITIONS} positioner  Screening: {' / '.join(SCHEDULE)}\n"
        f"[dim]LimitOrder: ×{LIMIT_TOLERANCE} | Kelly: Quarter ({QUARTER_KELLY}) | "
        f"Gmail SMTP[/]",
        title="HD2 Afgangsprojekt — Tobias Hagendam Ludvigsen",
    ))

    ib = IB()
    try:
        ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID)
        console.print(f"[green]✅ IB forbundet | Konto: {ib.wrapper.accounts[0]}[/]")
    except Exception as e:
        console.print(f"[red]❌ IB forbindelsesfejl: {e}[/]")
        return

    console.print("\n[bold]Ticker-univers:[/]")
    universe = load_universe()
    console.print(f"  {len(universe):,} aktier klar\n")

    # Synkroniser DB med IB ved opstart
    sync_db_with_ib(ib)

    last_run    = None
    last_email  = None
    alle_res    = []
    last_vix    = None
    kapital     = 0.0
    kapital_tid = None

    try:
        while True:
            if should_stop():
                console.print("\n[yellow]🔴 Kl. 22:00 — stopper.[/]")
                break

            if not market_is_open_today():
                console.print(f"[dim]Markedet lukket ({date.today().strftime('%A')}) — stopper.[/]")
                break

            nu = datetime.now()
            if kapital_tid is None or (nu - kapital_tid).seconds > 300:
                kapital     = get_kapital(ib, verbose=(kapital_tid is None))
                kapital_tid = nu

            try:
                vix, vix_chg = get_vix()
            except Exception:
                vix, vix_chg = (last_vix or 0.0), 0.0

            if kapital <= 0:
                console.print("[red]❌ Kapital utilgængelig — venter 60s[/]")
                time.sleep(60)
                continue

            closed = check_expired()
            for c in closed:
                col = "green" if c["pnl"] >= 0 else "red"
                console.print(
                    f"[{col}]📋 LUKKET: {c['ticker']} "
                    f"Exit: ${c.get('exit_price', 0):.3f} | "
                    f"P&L: ${c['pnl']:+,.0f} ({c['pnl_pct']:+.1f}%)[/{col}]"
                )

            vix_trigger = (last_vix is not None and abs(vix_chg) >= VIX_TRIGGER)

            if should_run_now(last_run) or vix_trigger:
                if vix_trigger:
                    console.print(f"[yellow]⚡ VIX trigger: {vix:.1f} ({vix_chg:+.1%})[/]")
                alle_res = run_screening(ib, universe, kapital, vix, vix_chg)
                last_run = datetime.now()
                last_vix = vix
            else:
                n_open  = len(get_open_positions())
                vix_str = f"{vix:.1f}" if vix > 0 else "N/A"
                console.print(
                    f"[dim]{datetime.now().strftime('%H:%M:%S')}  "
                    f"Afventer: {next_run_time()}  "
                    f"| Kapital: ${kapital:,.0f}  "
                    f"| VIX: {vix_str}  "
                    f"| Pos: {n_open}/{MAX_POSITIONS}[/]",
                    end="\r"
                )
                last_vix = vix

            if should_send_email(last_email) and alle_res:
                send_daily_email(alle_res, vix, kapital)
                last_email = datetime.now()

            time.sleep(30)

    except KeyboardInterrupt:
        console.print("\n[yellow]⏹ Stoppet manuelt.[/]")
    finally:
        ib.disconnect()
        console.print("[dim]IB forbindelse lukket.[/]")


if __name__ == "__main__":
    run()