@echo off
cd /d "%~dp0"
echo Starting Polymarket dashboard on http://localhost:5050
echo   /candles   Chainlink 1m candles
echo   /livetest  Strategy test + candle probabilities
echo.
py -3 -m pip install -r requirements.txt -q
py -3 app.py
pause
