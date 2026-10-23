package desktop

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"apptrix.org/components/widget/webview"
	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/widget"

	"yswa-var/dhan_go_bus/internals/bus"
	"yswa-var/dhan_go_bus/internals/ema"
	"yswa-var/dhan_go_bus/internals/marketfeed"
	"yswa-var/dhan_go_bus/internals/ohlc"
	"yswa-var/dhan_go_bus/internals/orderbook"
	"yswa-var/dhan_go_bus/internals/pipeline"
)

// Run builds and runs the Fyne terminal. chartAssets is the embedded assets/chart directory.
func Run(chartAssets embed.FS, clientID, accessToken string, instruments []Instrument, history ema.CandleLoader) error {
	if len(instruments) == 0 {
		return errors.New("terminal needs at least one instrument")
	}
	selected := instruments[0]
	for _, instrument := range instruments {
		if instrument.Symbol == "GOLDM" {
			selected = instrument
			break
		}
	}
	state := NewState(instruments, selected)
	application := fyneApp()
	window := application.NewWindow("PROJX Gold Terminal")
	window.Resize(fyne.NewSize(1200, 800))

	root, err := fs.Sub(chartAssets, "assets/chart")
	if err != nil {
		return fmt.Errorf("open embedded chart assets: %w", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("start local chart bridge: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/chart/", http.StripPrefix("/chart/", http.FileServer(http.FS(root))))
	mux.HandleFunc("/state", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if err := state.MarshalWrite(w); err != nil {
			http.Error(w, "state unavailable", http.StatusInternalServerError)
		}
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			state.SetError(err)
		}
	}()

	chart, err := webview.New(window)
	if err != nil {
		_ = server.Close()
		_ = listener.Close()
		return fmt.Errorf("create system WebView: %w", err)
	}
	chartURL := &url.URL{Scheme: "http", Host: listener.Addr().String(), Path: "/chart/index.html"}
	chart.Load(chartURL)

	list := widget.NewList(func() int { return len(instruments) }, func() fyne.CanvasObject {
		return container.NewVBox(widget.NewLabel("Instrument"), widget.NewSeparator())
	}, func(i widget.ListItemID, object fyne.CanvasObject) {
		row := object.(*fyne.Container)
		label := row.Objects[0].(*widget.Label)
		label.SetText(instruments[i].Symbol)
	})
	list.Select(indexOf(instruments, selected))
	selectedLabel := widget.NewLabel(selected.Symbol + " · 1m · EMA20")
	selectedLabel.TextStyle = fyne.TextStyle{Bold: true}
	ltpLabel := widget.NewLabel("LTP —")
	chartHeader := container.NewHBox(selectedLabel, layout.NewSpacer(), ltpLabel)
	chartPane := container.NewBorder(chartHeader, nil, nil, nil, chart)

	bookTable := widget.NewTable(func() (int, int) { return orderbook.LevelCount + 1, 4 }, func() fyne.CanvasObject { return widget.NewLabel("") }, func(id widget.TableCellID, object fyne.CanvasObject) {
		label := object.(*widget.Label)
		if id.Row == 0 {
			headers := []string{"Bid Qty", "Bid Price", "Ask Price", "Ask Qty"}
			label.TextStyle = fyne.TextStyle{Bold: true}
			label.SetText(headers[id.Col])
			return
		}
		book, ok := state.Book(state.Selected())
		if !ok {
			label.SetText("—")
			return
		}
		level := id.Row - 1
		var text string
		switch id.Col {
		case 0:
			text = strconv.FormatInt(book.Bids[level].Quantity, 10)
		case 1:
			text = formatPrice(book.Bids[level].Price)
		case 2:
			text = formatPrice(book.Asks[level].Price)
		case 3:
			text = strconv.FormatInt(book.Asks[level].Quantity, 10)
		}
		label.SetText(text)
	})
	bookTable.SetColumnWidth(0, 170)
	bookTable.SetColumnWidth(1, 250)
	bookTable.SetColumnWidth(2, 250)
	bookTable.SetColumnWidth(3, 170)
	bookPane := container.NewBorder(widget.NewLabelWithStyle("5-Level Order Book", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}), nil, nil, nil, bookTable)
	watchPane := container.NewBorder(widget.NewLabelWithStyle("Watchlist", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}), nil, nil, nil, list)
	mainSplit := container.NewHSplit(watchPane, chartPane)
	mainSplit.SetOffset(0.23)
	content := container.NewVSplit(mainSplit, container.NewPadded(bookPane))
	content.SetOffset(0.68)
	window.SetContent(content)

	ctx, cancel := context.WithCancel(context.Background())
	var historyMu sync.Mutex
	historyLoaded := make(map[string]bool)
	loadHistory := func(instrument Instrument) {
		if history == nil {
			return
		}
		instrumentKey := key(instrument)
		historyMu.Lock()
		if historyLoaded[instrumentKey] {
			historyMu.Unlock()
			return
		}
		historyLoaded[instrumentKey] = true
		historyMu.Unlock()
		go func() {
			securityID, err := strconv.ParseInt(instrument.SecurityID, 10, 64)
			var candles []ohlc.Candle
			if err == nil {
				candles, err = history.LoadRecentCandles(ctx, securityID, instrument.ExchangeSegment, time.Minute, 500)
			}
			if err != nil {
				historyMu.Lock()
				delete(historyLoaded, instrumentKey)
				historyMu.Unlock()
				state.SetError(fmt.Errorf("load %s chart history: %w", instrument.Symbol, err))
				return
			}
			state.LoadCandles(instrument, candles)
		}()
	}
	list.OnSelected = func(id widget.ListItemID) {
		if id >= 0 && id < len(instruments) {
			instrument := instruments[id]
			state.Select(instrument)
			refreshHeader(state, selectedLabel, ltpLabel)
			bookTable.Refresh()
			loadHistory(instrument)
		}
	}
	loadHistory(selected)
	eventBus, done, engineErr := startEngine(ctx, clientID, accessToken, instruments, state)
	if engineErr != nil {
		cancel()
		chart.Close()
		_ = server.Shutdown(context.Background())
		return engineErr
	}
	var refreshDone chan struct{}
	refreshDone = make(chan struct{})
	go func() {
		defer close(refreshDone)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fyne.Do(func() { refreshHeader(state, selectedLabel, ltpLabel); bookTable.Refresh() })
			}
		}
	}()
	window.SetOnClosed(func() {
		cancel()
		eventBus.Close()
		chart.Close()
		shutdownCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
		defer stop()
		_ = server.Shutdown(shutdownCtx)
	})
	window.ShowAndRun()
	cancel()
	eventBus.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		log.Print("market engine shutdown timed out")
	}
	<-refreshDone
	chart.Close()
	_ = server.Close()
	return nil
}

// state.MarshalWrite keeps the bridge response small and avoids exposing internal synchronization.
func (s *State) MarshalWrite(w interface{ Write([]byte) (int, error) }) error {
	data, err := s.MarshalSnapshot()
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func startEngine(ctx context.Context, clientID, token string, instruments []Instrument, state *State) (*bus.BroadcastBusImpl[marketfeed.MarketEvent], <-chan struct{}, error) {
	client, err := marketfeed.NewClient(marketfeed.ClientConfig{ClientID: clientID, AccessToken: token,
		OnConnect: func() { state.SetError(nil) }, OnError: func(err error) { state.SetError(err) }})
	if err != nil {
		return nil, nil, err
	}
	eventBus := bus.NewBroadcastBus[marketfeed.MarketEvent]()
	_, pipelineEvents := eventBus.Subscribe(512)
	_, uiEvents := eventBus.Subscribe(512)
	_, depthEvents := eventBus.Subscribe(512)
	aggregator := ohlc.NewMinuteAggregator()
	candleBus := bus.NewBroadcastBus[ohlc.Candle]()
	_, candles := candleBus.Subscribe(256)
	candlePipeline, err := pipeline.NewCandlePipelineWithPublisher(pipelineEvents, aggregator, candleBus.Publish)
	if err != nil {
		eventBus.Close()
		candleBus.Close()
		return nil, nil, err
	}
	emaEngine := ema.NewEMA20Engine()
	bookEngine := orderbook.NewOrderBookEngine()
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		if err := candlePipeline.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			state.SetError(err)
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case candle, ok := <-candles:
				if !ok {
					return
				}
				update, hasEMA, err := emaEngine.OnCandle(candle)
				if err != nil {
					state.SetError(err)
					continue
				}
				state.OnCandle(candle, update, hasEMA)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-uiEvents:
				if !ok {
					return
				}
				state.OnMarketEvent(event)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-depthEvents:
				if !ok {
					return
				}
				depth, yes := event.(marketfeed.Depth)
				if !yes {
					continue
				}
				snapshot, err := bookEngine.OnDepth(depth)
				if err != nil {
					continue
				}
				state.OnBook(snapshot)
			}
		}
	}()
	go func() {
		defer close(done)
		defer candleBus.Close()
		defer wg.Wait()
		subscriptions := make([]marketfeed.Subscription, 0, len(instruments))
		for _, instrument := range instruments {
			subscriptions = append(subscriptions, marketfeed.Subscription{ExchangeSegment: instrument.ExchangeSegment, SecurityID: instrument.SecurityID, Symbol: instrument.Symbol})
		}
		if err := client.RunWithBus(ctx, subscriptions, eventBus); err != nil {
			state.SetError(err)
		}
		eventBus.Close()
	}()
	return eventBus, done, nil
}

func refreshHeader(state *State, symbol, ltp *widget.Label) {
	snapshot := state.Snapshot()
	symbol.SetText(snapshot.Symbol + " · 1m · EMA20")
	if snapshot.LTP > 0 {
		ltp.SetText(fmt.Sprintf("LTP %s", formatFloat(snapshot.LTP)))
	} else {
		ltp.SetText("LTP —")
	}
}

func indexOf(instruments []Instrument, selected Instrument) int {
	for i, instrument := range instruments {
		if key(instrument) == key(selected) {
			return i
		}
	}
	return 0
}
func formatPrice(price int64) string {
	if price == 0 {
		return "—"
	}
	return formatFloat(float64(price) / float64(ohlc.PriceScale))
}
func formatFloat(value float64) string { return fmt.Sprintf("%.2f", value) }

// fyneApp is separated to keep the build's required desktop entry point small.
func fyneApp() fyne.App { return app.NewWithID("org.projx.gold-terminal") }
