OBJECTIVE

\---------

You are a research + engineering agent. Your goal: \*\*decode the trading strategy\*\* encoded in the attached trade-call documents and deliver a fully-reproducible, production-ready Python implementation that can be run on a Mac (VS Code / Thonny) without causing IP bans. Do not return a roadmap — return working code + tests + clear evidence. Use the attached documents as primary data.

PRIMARY INPUTS (INGEST THESE EXACT SOURCES)

\------------------------------------------

1) https://cdn.discordapp.com/attachments/1438163404496834573/1438164210419765390/Zyncas\_Spot\_12\_nov\_optimized.pdf?...  

2) https://github.com/rosariodawson/bhu/blob/0e25c7ee58693dc18ca733df0e2047ca58e2909d/Zyncas%20Spot%2012%20nov.pdf  

3) https://github.com/oreibokkarao-bit/god/blob/b7715a2ba3d4022876af41ab83e7681218dc690a/Zyncas%20Spot%2012%20nov.pdf  

4) https://raw.githubusercontent.com/rosariodawson/bhu/refs/heads/main/Cracking%20Zyncas\_%20Turning%2011-22-32%25%20Alt-Spike%20Patterns%20into%20Pre-Emptive%20Spot%20Trades.md  

5) https://pdf.ai/share/t/cmhw1q2vp000rjl04buili04m

If other files are discovered in the same repos/links, ingest them too but \*\*label provenance\*\* for each datum.

ASSUMPTIONS (if anything ambiguous, pick reasonable default and state it)

\-------------------------------------------------------------------------

\- Use IST timezone (Asia/Kolkata) when aligning timestamps from trade-call logs and exchange data.

\- When a trade-call date/time lacks timezone, assume it’s in IST unless later contradicted by file metadata.

\- If market-data for a listed coin/symbol is unavailable from an exchange on that historical date, log this as missing and try alternative sources (CoinGecko, CoinPaprika) for price verification.

REQUIREMENTS — ANALYSIS & DECODING

\----------------------------------

1\. \*\*Parse every trade call\*\* from the provided docs into structured rows: \`{call\_id, date\_time\_ISO, exchange(if noted), symbol, side, entry, tp1, tp2, tp3, sl, result/outcome, notes}\`. Provide a CSV/SQLite export and the parsing code used (robust to OCR artifacts).

2\. \*\*Reconstruct ground-truth market data\*\* for each trade call: fetch historical tick/1m/5m OHLCV and (if available) orderbook snapshots, trades, and funding rates for the exact timeframe covering -24h..+48h around the call. Prefer exchange native data (Binance/Bybit/Bitget) first; fall back to CoinGecko/CryptoCompare for missing tick data. Save raw snapshots (parquet or sqlite) with provenance (source URL, timestamp).

3\. \*\*Define candidate trigger hypotheses\*\* (start with at least 12 well-structured hypotheses) — e.g.,:  

   - Fresh spot inflow surge (exchange deposit + spot buy taker volume)  

   - CVD kick on 5m/15m (spot-taker dominance)  

   - OI build in perp preceded by spot flow divergence  

   - VPVR gap + volume spike at support band  

   - Bollinger expansion on 5m following longsmith candle + taker-buy > 0.6  

   - New listing / liquidity relocation signal  

   For each hypothesis, implement detection logic and metrics (precise formulas), and instrument the code to return numeric scores.

4\. \*\*Label each trade-call\*\* with which trigger hypothesis(es) fired, including exact metric values and timestamps when triggers were first satisfied.

5\. \*\*Backtest / Event Study:\*\* For each trigger hypothesis and combination rules, compute:

   - Hit rate (TP1/TP2/TP3) and false positive rate.

   - Realized returns distribution at +30m, +2h, +24h and also max adverse excursion (MAE) between entry and target/SL.

   - Cumulative abnormal returns (CAR) event-study at +30m/+2h/+24h with bootstrapped confidence intervals.

   - Expectancy per trade, win-rate, average R, Sharpe-like metric.

   - Statistical significance (bootstrap or permutation tests) of trigger → positive outcome (report p-values).

6\. \*\*Failure-mode analysis:\*\* For failed calls (calls that lost or hit SL), run automated diagnostics to produce a taxonomy for each failure:

   - data-mismatch (e.g., slippage vs backtest assumption)

   - late trigger (trigger fired after major move)

   - macro regime mismatch (BTC downtrend)

   - illiquidity (thin orderbook, wide spreads > threshold)

   - fake pump (whale exit / wash trading)

   Produce a structured matrix summary and representative examples (2–5 failing trades with annotated charts and timeline of signals).

7\. \*\*Macro filters:\*\* Test whether adding macro gates (BTC trend, BTC dominance, top-of-book funding-breadth) would have improved performance. Quantify improvement (delta expectancy, delta winrate). Compare: no-filter vs BTC-trend-only vs BTCdom-only vs both.

8\. \*\*Ablation & Feature Importance:\*\* For the final best-performing rule set, run ablation to show which features drive performance. Provide feature importance (SHAP or permutation importance) and an interpretable priority list.

9\. \*\*Overfitting controls:\*\* Use time-based cross-validation (walk-forward), hold out the most recent 20% of calls for final out-of-sample validation. Report possible look-ahead issues and how they were mitigated.

DELIVERABLES — REQUIRED OUTPUTS

\--------------------------------

Deliver \*\*all\*\* of the following as part of the final package:

A. \*\*Ingest Manifest:\*\* list of files used, their exact URLs, SHA256 checksums, and a small README describing how they were processed.

B. \*\*Data snapshots\*\* (raw + processed) used for analysis in a reproducible folder (parquet or sqlite). Provide sample SQL queries to reproduce key tables.

C. \*\*Exploratory Analysis Notebook(s)\*\* (Jupyter) containing charts, event-study results, failure-mode examples, and all intermediate computations. Notebooks should be runnable; include a \`requirements.txt\`.

D. \*\*Final Strategy Spec doc\*\* (markdown) that states:

   - precise signal definitions (math, timeframes, thresholds)

   - how signals are combined into a trigger (boolean / score threshold / ensemble)

   - risk rules, position sizing, stop logic, partial exit ladder (TP1/2/3)

   - expected holding time distribution and recommended leverage

   - why microcaps require special treatment (if found)

E. \*\*Production-ready Python repo\*\* (zippable) that runs on macOS with:

   - \`python >=3.10, <=3.12\` compatible code

   - modules: \`data\_ingest.py\`, \`signals.py\`, \`backtest.py\`, \`live\_screener.py\`, \`risk\_manager.py\`, \`cli.py\`

   - robust logging, sqlite audit trail, and optional CSV output

   - sample config files (\`config.example.yml\`) with toggles for exchanges, timeframes, watchlist, min\_marketcap, ip\_safety toggles

   - full README with Mac/VS Code run instructions, virtualenv steps, and how to reproduce the analysis

   - unit tests for core functions (pytest) and a small integration test against historical local snapshots

F. \*\*Live Screener Mode\*\* (\`live\_screener.py\`) that:

   - Can run in “dry-run” mode using public REST webs (respecting rate limits) or in websocket mode (faster).  

   - Built-in IP-ban safety: token-bucket limiter per-exchange, jittered request intervals, exponential backoff with randomized jitter, backoff on 429/418/5xx, optional multiple API keys rotation, persistent sqlite caching to avoid redundant calls.  

   - Safe defaults: recommended polling limits for each exchange (documented).  

   - Can be configured to use proxy rotation (documented as optional) — \*do not\* require proxies to run.

G. \*\*Backtest & Event Study Results\*\* — CSVs and charts for:

   - per-call P&L details,

   - hits/misses,

   - ROC curves, confusion matrix,

   - CAR plots (+30m/+2h/+24h) with 95% CIs.

H. \*\*Failure Mode Report\*\* — a structured CSV with failure category per failed call + short textual reasoning.

I. \*\*One-click package\*\*: zip with the repo + data snapshot + notebooks + artifacts and a simple \`run\_all.sh\` to re-run the analysis locally.

J. \*\*Deliver final production-ready Python file(s)\*\* — not pseudo-code. If space-constrained, at minimum \`live\_screener.py\` and \`risk\_manager.py\` must run end-to-end in dry-run mode and produce the same signal decisions the backtest found.

ENGINEERING / INFRA & IP-BAN SAFETY RULES

\----------------------------------------

\- Use async websockets where available (uvloop recommended). Use \`aiohttp\`/\`websockets\` for connections.

\- Implement \*\*per-exchange token-bucket\*\*: default bucket sizes and refill rates must be conservative (document recommended values) and adjustable via config.

\- Use \*\*exponential backoff\*\* with full-jitter on non-200 responses and on socket failures. Log and persist retry attempts.

\- Use \*\*local caching\*\* (sqlite) for any historical lookup so that repeated runs don’t re-query exchanges.

\- Avoid aggressive polling of markets with tiny marketcap; for microcaps, prefer websockets (if available) or slow REST polling + on-disk caching.

\- If using multiple API keys to spread load: treat this as optional and clearly document legal/compliance implications — don’t require rotating through proxy pools by default.

\- Do not perform scraping that violates exchange ToS. Prefer official public REST or websocket endpoints.

EVALUATION METRICS (what to report)

\-----------------------------------

\- Hit rates for TP1/TP2/TP3, mean R, expectancy, median time-to-target.

\- Out-of-sample expectancy (walk-forward).

\- Improvement delta when adding BTC-trend / BTCdom filters.

\- P-value or confidence intervals for CAR at +30m/+2h/+24h.

\- IP-safety score (estimated requests per minute with default config) and an "IP-risk" advisory.

QUANT & STATISTICAL DETAILS (must be followed)

\-----------------------------------------------

\- Use bootstrap (n >= 10,000) where appropriate for confidence intervals on small samples (microcap calls).

\- Use permutation tests to assess whether observed hit rates exceed random chance given the same universe and entry rules.

\- Report multiple-testing correction (Benjamini-Hochberg or Bonferroni) for p-values across many hypotheses.

\- Provide code that numerically reproduces every table/figure.

MODEL / ML (optional only if it adds value)

\------------------------------------------

\- If using ML classifiers, they must be explainable (LightGBM / XGBoost allowed). Provide feature importance, calibration plots, and be cautious of data leakage. Default preference: rule-based + scoring + simple ML post-filter as an optional ensemble.

\- Save deterministic random seeds and provide model artifacts.

DELIVERABLE FORMAT & COMMUNICATION

\---------------------------------

\- Provide everything in a single zip. Also provide a short executive summary (1 page) with the main conclusion: the best decoded trigger(s), performance metrics, and whether the strategy is robust to microcap idiosyncrasies.

\- Provide an audit log of every external request the pipeline made (endpoints + timestamps).

\- In the final message, \*\*do not\*\* ask me for clarifications; choose reasonable defaults, note them clearly at the top of your report, and proceed.

FINAL MANDATE (non-negotiable)

\-----------------------------

\- \*\*You must output a working, production-ready Python implementation\*\* (repo + zip). It must include a \`live\_screener.py\` that runs in dry-run mode on my Mac and reproduces the analysis' signal decisions. If any part cannot be produced (e.g., due to data withheld by exchanges), explicitly state what is missing and provide a reproducible fallback procedure.

\- Provide clear reproduction steps so I can run everything on my Mac (VS Code) without remote compute.

TIMELINE & RESOURCES

\--------------------

\- No time estimate required here — just proceed and produce the deliverables described above.

\- Use the attached files as ground truth for decoding and cross-check all inferences against them.

If you understand, proceed to:

1) ingest the files and produce an ingest manifest,  

2) parse trade calls and return the first 10 parsed rows as proof,  

3) then run the analysis pipeline described and assemble deliverables.