package dhan_ops

import (
	"bytes"
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSelectNearestFuture(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		contracts []Contract
		wantID    string
		wantErr   string
	}{
		{
			name: "nearest active and correct exchange",
			contracts: []Contract{
				future("expired", "2026-10-22", "MCX"),
				future("nse", "2026-10-24", "NSE"),
				future("far", "2026-12-04", "MCX"),
				future("near", "2026-11-05", "MCX"),
			},
			wantID: "near",
		},
		{
			name: "no active contract",
			contracts: []Contract{
				future("expired", "2026-10-22", "MCX"),
			},
			wantErr: "no active GOLDM MCX contract",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := selectNearestFuture(test.contracts, "GOLDM", "MCX", "FUTCOM", now)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("error = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("selectNearestFuture() error = %v", err)
			}
			if got.SecurityID != test.wantID {
				t.Fatalf("selected security ID = %q, want %q", got.SecurityID, test.wantID)
			}
		})
	}
}

func TestReadContractsPreservesDhanExchangeSegments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "contracts.csv")
	var data bytes.Buffer
	writer := csv.NewWriter(&data)
	_ = writer.Write([]string{"SEM_EXM_EXCH_ID", "SEM_SEGMENT", "SEM_SMST_SECURITY_ID", "SEM_INSTRUMENT_NAME", "SEM_CUSTOM_SYMBOL", "SEM_EXPIRY_DATE", "SEM_STRIKE_PRICE", "SEM_OPTION_TYPE", "SM_SYMBOL_NAME"})
	_ = writer.Write([]string{"BSE", "C", "ignored-currency", "FUTCUR", "USDINR NOV FUT", "2099-11-25", "-0.01000", "XX", "USDINR"})
	_ = writer.Write([]string{"MCX", "M", "goldm", "FUTCOM", "GOLDM NOV FUT", "2099-11-05", "0", "XX", "GOLDM"})
	writer.Flush()
	if err := os.WriteFile(path, data.Bytes(), 0600); err != nil {
		t.Fatalf("write CSV file: %v", err)
	}

	contracts, err := readContracts(path)
	if err != nil {
		t.Fatalf("readContracts() error = %v", err)
	}
	if len(contracts) != 1 {
		t.Fatalf("read %d supported contracts, want 1", len(contracts))
	}
	if contracts[0].ExchangeSegment != goldSegment {
		t.Fatalf("GOLDM segment = %q, want %q", contracts[0].ExchangeSegment, goldSegment)
	}
}

func TestResolvedContractCacheReuseWindow(t *testing.T) {
	now := time.Date(2026, 10, 23, 14, 0, 0, 0, time.UTC)
	contracts := []Contract{{
		SecurityID:      "goldm-future",
		Underlying:      "GOLDM",
		Symbol:          "GOLDM NOV FUT",
		Instrument:      "FUTCOM",
		ExchangeSegment: goldSegment,
		Expiry:          "2026-11-05",
	}}

	t.Run("reuses cache younger than twelve hours", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "contracts.csv")
		generated := now.Add(-11 * time.Hour)
		cachePath, err := saveResolvedContracts(path, generated, contracts)
		if err != nil {
			t.Fatalf("saveResolvedContracts() error = %v", err)
		}
		if !strings.Contains(filepath.Base(cachePath), generated.Format(resolvedTimestampLayout)) {
			t.Fatalf("cache filename = %q, missing generation timestamp", filepath.Base(cachePath))
		}

		got, ok := loadRecentResolvedContracts(path, now)
		if !ok {
			t.Fatal("loadRecentResolvedContracts() did not reuse recent cache")
		}
		if len(got) != 1 || got[0].SecurityID != contracts[0].SecurityID || got[0].ExchangeSegment != goldSegment {
			t.Fatalf("loaded contracts = %#v, want %#v", got, contracts)
		}
	})

	t.Run("ignores cache twelve hours old or older", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "contracts.csv")
		if _, err := saveResolvedContracts(path, now.Add(-12*time.Hour), contracts); err != nil {
			t.Fatalf("saveResolvedContracts() error = %v", err)
		}
		if _, ok := loadRecentResolvedContracts(path, now); ok {
			t.Fatal("loadRecentResolvedContracts() reused expired cache")
		}
	})
}

func TestSelectATMOptions(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	contracts := optionContracts("2026-10-30", "GOLDM", 90, 110)
	contracts = append(contracts, optionContracts("2026-11-27", "GOLDM", 90, 110)...)
	contracts = append(contracts, optionContracts("2026-10-20", "GOLDM", 90, 110)...)

	selected, err := selectATMOptions(contracts, "GOLDM", 100.5, now)
	if err != nil {
		t.Fatalf("selectATMOptions() error = %v", err)
	}
	if len(selected) != 22 {
		t.Fatalf("selected %d contracts, want 22", len(selected))
	}
	if selected[0].Strike != "95" || selected[0].OptionType != "CE" {
		t.Fatalf("first selected contract = %#v, want strike 95 CE", selected[0])
	}
	if selected[21].Strike != "105" || selected[21].OptionType != "PE" {
		t.Fatalf("last selected contract = %#v, want strike 105 PE", selected[21])
	}
	for i := 0; i < len(selected); i += 2 {
		if selected[i].OptionType != "CE" || selected[i+1].OptionType != "PE" {
			t.Fatalf("option pair at %d = %s/%s, want CE/PE", i, selected[i].OptionType, selected[i+1].OptionType)
		}
	}
}

func TestSelectATMOptionsErrors(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		modify  func([]Contract) []Contract
		wantErr string
	}{
		{
			name: "missing pair",
			modify: func(contracts []Contract) []Contract {
				return removeContract(contracts, "2026-10-30", "100", "PE")
			},
			wantErr: "missing GOLDM PE contract",
		},
		{
			name: "insufficient strikes",
			modify: func(_ []Contract) []Contract {
				return optionContracts("2026-10-30", "GOLDM", 95, 104)
			},
			wantErr: "need at least 11",
		},
		{
			name: "invalid option type",
			modify: func(contracts []Contract) []Contract {
				for i := range contracts {
					if contracts[i].Expiry == "2026-10-30" && contracts[i].Strike == "100" && contracts[i].OptionType == "CE" {
						contracts[i].OptionType = "CALL"
						break
					}
				}
				return contracts
			},
			wantErr: "invalid GOLDM option type",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			contracts := optionContracts("2026-10-30", "GOLDM", 90, 110)
			got, err := selectATMOptions(test.modify(contracts), "GOLDM", 100, now)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("selectATMOptions() error = %v, want %q", err, test.wantErr)
			}
			if got != nil {
				t.Fatalf("selected contracts = %d, want nil on error", len(got))
			}
		})
	}
}

func TestValidateSelectedContractsRejectsDuplicateSecurityIDs(t *testing.T) {
	contracts := []Contract{
		{SecurityID: "same", Underlying: "GOLDM", Symbol: "GOLDM NOV FUT"},
		{SecurityID: "same", Underlying: "GOLD", Symbol: "GOLD DEC FUT"},
	}
	if err := validateSelectedContracts(contracts); err == nil || !strings.Contains(err.Error(), `duplicate security ID "same"`) {
		t.Fatalf("error = %v, want duplicate security ID error", err)
	}
}

func TestResolveContractsReturnsFixedUniverseInOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "contracts.csv")
	var data bytes.Buffer
	writer := csv.NewWriter(&data)
	_ = writer.Write([]string{"EXCH_ID", "SEGMENT", "SECURITY_ID", "INSTRUMENT", "DISPLAY_NAME", "EXPIRY_DATE", "STRIKE_PRICE", "OPTION_TYPE", "UNDERLYING_SYMBOL"})
	writeFutureRow(t, writer, "MCX", "goldm-future", "GOLDM", "2026-11-05")
	writeFutureRow(t, writer, "MCX", "gold-future", "GOLD", "2026-12-04")
	writeFutureRow(t, writer, "MCX", "goldpetal-future", "GOLDPETAL", "2026-10-30")
	writeFutureRow(t, writer, "MCX", "goldguinea-future", "GOLDGUINEA", "2026-10-30")
	for _, underlying := range []string{"GOLDM", "GOLD"} {
		for _, contract := range optionContracts("2026-10-29", underlying, 90, 110) {
			_ = writer.Write([]string{"MCX", "M", underlying + "-" + contract.SecurityID, "OPTFUT", contract.Symbol, contract.Expiry, contract.Strike, contract.OptionType, contract.Underlying})
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatalf("write CSV data: %v", err)
	}
	if err := os.WriteFile(path, data.Bytes(), 0600); err != nil {
		t.Fatalf("write CSV file: %v", err)
	}

	var quoteCalls []string
	quote := func(segment, securityID string) (float64, error) {
		quoteCalls = append(quoteCalls, segment+":"+securityID)
		return 100, nil
	}
	contracts, err := resolveContracts(path, time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC), quote)
	if err != nil {
		t.Fatalf("resolveContracts() error = %v", err)
	}
	if len(contracts) != 48 {
		t.Fatalf("resolved %d contracts, want 48", len(contracts))
	}
	if got, want := contracts[0].SecurityID, "goldm-future"; got != want {
		t.Fatalf("first contract = %q, want %q", got, want)
	}
	if got, want := contracts[1].OptionType, "CE"; got != want {
		t.Fatalf("first GOLDM option type = %q, want %q", got, want)
	}
	if got, want := contracts[23].SecurityID, "gold-future"; got != want {
		t.Fatalf("GOLD future position = %q, want %q", got, want)
	}
	if got, want := contracts[46].Underlying, "GOLDPETAL"; got != want {
		t.Fatalf("GOLDPETAL position = %q, want %q", got, want)
	}
	if got, want := contracts[47].Underlying, "GOLDGUINEA"; got != want {
		t.Fatalf("last contract = %q, want %q", got, want)
	}
	if got, want := strings.Join(quoteCalls, ","), "MCX_COMM:goldm-future,MCX_COMM:gold-future"; got != want {
		t.Fatalf("quote calls = %q, want %q", got, want)
	}
}

func TestOptionOrderingIsDeterministic(t *testing.T) {
	contracts := optionContracts("2026-10-30", "GOLDM", 90, 100)
	// The helper emits CE then PE for each ascending strike. Reversing the input
	// must not affect the selected order.
	for i, j := 0, len(contracts)-1; i < j; i, j = i+1, j-1 {
		contracts[i], contracts[j] = contracts[j], contracts[i]
	}
	selected, err := selectATMOptions(contracts, "GOLDM", 95, time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("selectATMOptions() error = %v", err)
	}
	for i := 0; i < len(selected); i += 2 {
		if selected[i].OptionType != "CE" || selected[i+1].OptionType != "PE" {
			t.Fatalf("pair at %d = %s/%s, want CE/PE", i, selected[i].OptionType, selected[i+1].OptionType)
		}
		if i > 0 {
			previous, _ := strconv.ParseFloat(selected[i-2].Strike, 64)
			current, _ := strconv.ParseFloat(selected[i].Strike, 64)
			if current < previous {
				t.Fatalf("strikes are not ascending at %d", i)
			}
		}
	}
}

func future(id, expiry, exchange string) Contract {
	return Contract{SecurityID: id, Underlying: "GOLDM", Instrument: "FUTCOM", Expiry: expiry, exchange: exchange}
}

func optionContracts(expiry, underlying string, firstStrike, lastStrike int) []Contract {
	var contracts []Contract
	for strike := firstStrike; strike <= lastStrike; strike++ {
		for _, optionType := range []string{"PE", "CE"} {
			contracts = append(contracts, Contract{
				SecurityID: expiry + "-" + optionType + "-" + strconv.Itoa(strike),
				Underlying: underlying,
				Instrument: "OPTFUT",
				Expiry:     expiry,
				Strike:     formatStrike(strike),
				OptionType: optionType,
			})
		}
	}
	return contracts
}

func removeContract(contracts []Contract, expiry, strike, optionType string) []Contract {
	filtered := contracts[:0]
	for _, contract := range contracts {
		if contract.Expiry == expiry && contract.Strike == strike && contract.OptionType == optionType {
			continue
		}
		filtered = append(filtered, contract)
	}
	return filtered
}

func formatStrike(strike int) string {
	return strconv.Itoa(strike)
}

func writeFutureRow(t *testing.T, writer *csv.Writer, exchange, securityID, underlying, expiry string) {
	t.Helper()
	instrument := "FUTCOM"
	segment := "M"
	if err := writer.Write([]string{exchange, segment, securityID, instrument, underlying + " FUT", expiry, "0", "XX", underlying}); err != nil {
		t.Fatalf("write future row: %v", err)
	}
}
