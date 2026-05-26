# Forward Volatility Calendar Spread — Live Trading Bot

> HD2 Finansiering Afgangsprojekt — Tobias Hagendam Ludvigsen, CBS 2026

En systematisk options-handelsstrategi baseret på Forward Factor (FF) som handelssignal. Backtestet på WRDS OptionMetrics data 2010-2025 og implementeret live via Interactive Brokers API.

---

## Indhold

- [Strategi](#strategi)
- [Arkitektur](#arkitektur)
- [Installation](#installation)
- [Konfiguration](#konfiguration)
- [Kørsel](#kørsel)
- [Backtestresultater](#backtestresultater)
- [Filstruktur](#filstruktur)

---

## Strategi

Strategien udnytter backwardation i options-termstrukturen ved at handle **long call calendar spreads** når Forward Factor overstiger tærsklen 0.35.

**Forward Volatility:**
```
FwdVol = sqrt((IV_back² × T_back - IV_front² × T_front) / (T_back - T_front))
```

**Forward Factor:**
```
FF = (IV_front - FwdVol) / FwdVol
```

**Signal:** FF > 0.35 → åbn long calendar spread

**DTE-kombinationer:** 30-60, 30-90 og 60-90 dage

---

## Arkitektur

```
yfinance          →  Spot-priser, IV, FF-beregning (500+ aktier)
IB API            →  Validering af kontrakter + ordreafgivelse
Quarter Kelly     →  Positionsstørrelse baseret på backtestens f*
Gmail SMTP        →  Daglig email-rapport kl. 21:30
```

Screener køres automatisk kl. 16:00, 18:30 og 21:00 dansk tid.

---

## Installation

**Krav:**
- Python 3.10+
- Interactive Brokers Gateway/TWS (port 7497)
- Gmail konto med App Password

**Installér pakker:**
```bash
pip install ib_insync yfinance scipy pandas requests rich python-dotenv
```

---

## Konfiguration

Kopiér `.env.example` til `.env` og udfyld dine egne værdier:

```bash
cp .env.example .env
```

Åbn `.env` og udfyld:

```
IB_ACCOUNT=DIN_KONTO_HER
EMAIL_AFSENDER=din@gmail.com
EMAIL_MODTAGER=din@gmail.com
EMAIL_PASSWORD=xxxx xxxx xxxx xxxx
```

**Gmail App Password:**
1. Gå til myaccount.google.com → Sikkerhed
2. Slå 2-trinsbekræftelse til
3. Søg efter "App-adgangskoder" → Generér
4. Kopiér de 16 tegn ind i `.env`

**IB Gateway:**
- Sørg for IB Gateway eller TWS kører på port 7497
- Paper trading anbefales til test

---

## Kørsel

**Start botten:**
```bash
python BOT_FF.py
```

**Eller via bat-fil (Windows):**
```
start_ff_bot.bat
```

---

## Backtestresultater

Backtestet på WRDS OptionMetrics data 2010-2025, Russell 3000 universet.

| Metric | 30-60 DTE | 30-90 DTE | 60-90 DTE |
|--------|-----------|-----------|-----------|
| CAGR (Quarter Kelly) | ~20% | 32.3% | 20.4% |
| Win rate | ~57% | 63.5% | ~55% |
| Profit factor | 4.43 | — | — |
| Sharpe (trade-niveau) | 1.16 | — | — |

**Benchmark:**
- SPY: 13.8% CAGR
- Berkshire Hathaway: 13.8% CAGR

**Decil-analyse (Spearman rank korrelation):**

| DTE-par | Spearman | p-værdi | Monotonitet |
|---------|----------|---------|-------------|
| 30-60 | 0.939 | <0.001 | 89% |
| 30-90 | 1.000 | ≈0 | 100% |
| 60-90 | 1.000 | ≈0 | 100% |

---

## Filstruktur

```
FF_Live_screener/
├── BOT_FF.py           # Hovedbot — screener + ordreafgivelse
├── start_ff_bot.bat    # Windows starter
├── .env.example        # Kredential-skabelon
└── .gitignore          # Ekskluderer .env og data
```

---

## Akademisk kontekst

Projektet er udarbejdet som HD2 Finansiering afgangsprojekt på CBS 2026.

Baseret på Campasano (2018): *"The Forward Volatility Agreement"*

**Karakter: 10**

---

*Tobias Hagendam Ludvigsen — CBS HD2 Finansiering 2026*
