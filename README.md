
<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/88725aad-b2d0-43ec-8988-e058eddfd8ef" />


Go market engine for Dhan live data.

It reads the Dhan feed and sends market events through Go channels. From that it builds:

- live LTP and quote updates
- 1 minute OHLC candles
- EMA20 indicator
- 5 level order book
- candle storage in Postgres

## Desktop terminal

Quest 007 adds a native macOS desktop app using [Fyne](https://fyne.io/), [Apptrix WebView](https://addons.fyne.io/addons/org.apptrix.components.webview/), [Apache ECharts](https://echarts.apache.org/). 

The window has a watchlist, ECharts candle chart with EMA20, and order book table. Chart files are bundled in the app, so no frontend server is needed. A tiny local bridge gives chart state to the WebView every 250 ms.

## Run it

Need Go, macOS build tools, a Dhan `.env`, and valid `auth.json` setup.

```bash
go build ./cmd/terminal
go run ./cmd/terminal
```

The first build can show a macOS `libobjc` duplicate library warning. Its only a linker warning, build still works.

## Main folders

```text
cmd/terminal/          desktop app entry point
internals/marketfeed   Dhan websocket feed
internals/ohlc         candle aggregation
internals/ema          EMA calculations
internals/orderbook    depth book state
internals/desktop      Fyne UI and chart bridge
assets/chart/          bundled HTML, JS, ECharts
```

Keep it simple for now. One selected instrument, one minute chart, one EMA20. Trading buttons come later maybe.

## Note

The Apptrix WebView addon is GPL-3.0-or-later or commercial licensed.
