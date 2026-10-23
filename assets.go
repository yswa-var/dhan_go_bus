package dhan_go_bus

import "embed"

// ChartAssets contains the offline Apache ECharts page and browser bundle.
//
//go:embed assets/chart/*
var ChartAssets embed.FS
