(() => {
  const chart = echarts.init(document.getElementById('chart'));
  const status = document.getElementById('status');
  chart.setOption({
    animation: false,
    backgroundColor: '#10151d',
    grid: { left: 58, right: 24, top: 38, bottom: 36 },
    xAxis: { type: 'category', data: [], boundaryGap: true, axisLine: { lineStyle: { color: '#465160' } }, axisLabel: { color: '#9aa7b7' } },
    yAxis: { scale: true, splitLine: { lineStyle: { color: '#252d38' } }, axisLabel: { color: '#9aa7b7' } },
    tooltip: { trigger: 'axis' },
    legend: { show: false },
    series: [
      { name: 'Candles', type: 'candlestick', data: [], itemStyle: { color: '#24b47e', color0: '#e25555', borderColor: '#24b47e', borderColor0: '#e25555' } },
      { name: 'EMA20', type: 'line', data: [], showSymbol: false, lineStyle: { color: '#f2bf55', width: 1.5 } }
    ]
  });
  let selected = '';
  async function refresh() {
    try {
      const response = await fetch('/state', { cache: 'no-store' });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const state = await response.json();
      const rows = state.candles || [];
      const x = rows.map(row => row.time);
      chart.setOption({
        xAxis: { data: x },
        series: [
          { data: rows.map(row => [row.open, row.close, row.low, row.high]) },
          { data: rows.map(row => row.ema || null) }
        ]
      });
      if (selected !== state.symbol) {
        selected = state.symbol;
        chart.setOption({ title: { text: `${state.symbol || 'No instrument'} · 1m · EMA20`, left: 14, top: 8, textStyle: { color: '#dbe3ed', fontSize: 13, fontWeight: 'normal' } } });
      }
      status.textContent = state.error || (state.ltp > 0 ? `LTP ${Number(state.ltp).toLocaleString('en-IN', { maximumFractionDigits: 2 })}` : 'Waiting for market data…');
      status.style.color = state.error ? '#ff8c8c' : '#aab6c5';
    } catch (error) {
      status.textContent = `Chart update failed: ${error.message}`;
      status.style.color = '#ff8c8c';
    }
  }
  setInterval(refresh, 250);
  refresh();
  window.addEventListener('resize', () => chart.resize());
})();
