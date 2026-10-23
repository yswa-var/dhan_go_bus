package dhan_ops

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	contractURL  = "https://images.dhan.co/api-data/api-scrip-master.csv"
	contractFile = "contracts.csv"
	goldExchange = "MCX"
	goldSegment  = "MCX_COMM"

	resolvedContractsPrefix = "resolved-contracts-"
	resolvedTimestampLayout = "20060102T150405Z"
	resolvedContractsMaxAge = 12 * time.Hour
)

var resolvedUnderlyings = []string{"GOLDM", "GOLD", "GOLDPETAL", "GOLDGUINEA"}

type Contract struct {
	SecurityID string
	Underlying string
	Symbol     string
	Instrument string
	// ExchangeSegment is the Dhan API segment for this contract.
	ExchangeSegment string
	Expiry          string
	Strike          string
	OptionType      string

	exchange string
	segment  string
}

// GetContract downloads Dhan's contract master into the repository root.
func GetContract() error {
	root, err := findRoot()
	if err != nil {
		return err
	}

	response, err := http.Get(contractURL)
	if err != nil {
		return fmt.Errorf("download contract master: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("download contract master: %s", response.Status)
	}

	file, err := os.CreateTemp(root, contractFile+".tmp-")
	if err != nil {
		return fmt.Errorf("create contract file: %w", err)
	}
	temporary := file.Name()
	defer os.Remove(temporary)

	if _, err := io.Copy(file, response.Body); err != nil {
		_ = file.Close()
		return fmt.Errorf("save contract master: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close contract file: %w", err)
	}
	if err := os.Rename(temporary, filepath.Join(root, contractFile)); err != nil {
		return fmt.Errorf("replace contract file: %w", err)
	}
	return nil
}

// ResolveContracts loads a recent startup selection when available. Otherwise
// it resolves the contracts, saves the final list, and returns it.
func ResolveContracts(csvPath string) ([]Contract, error) {
	now := time.Now().UTC()
	if contracts, ok := loadRecentResolvedContracts(csvPath, now); ok {
		return contracts, nil
	}

	contracts, err := resolveContracts(csvPath, now, GetQuotePrice)
	if err != nil {
		return nil, err
	}
	if _, err := saveResolvedContracts(csvPath, now, contracts); err != nil {
		return nil, fmt.Errorf("save resolved contracts: %w", err)
	}
	return contracts, nil
}

func loadRecentResolvedContracts(csvPath string, now time.Time) ([]Contract, bool) {
	directory := filepath.Dir(csvPath)
	paths, err := filepath.Glob(filepath.Join(directory, resolvedContractsPrefix+"*.csv"))
	if err != nil {
		return nil, false
	}

	type candidate struct {
		path      string
		generated time.Time
	}
	var candidates []candidate
	for _, path := range paths {
		generated, ok := resolvedContractsTimestamp(filepath.Base(path))
		if !ok {
			continue
		}
		age := now.Sub(generated)
		if age >= 0 && age < resolvedContractsMaxAge {
			candidates = append(candidates, candidate{path: path, generated: generated})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].generated.After(candidates[j].generated)
	})

	for _, candidate := range candidates {
		contracts, err := readResolvedContracts(candidate.path)
		if err == nil && validateSelectedContracts(contracts) == nil {
			return contracts, true
		}
	}
	return nil, false
}

func resolvedContractsTimestamp(name string) (time.Time, bool) {
	if !strings.HasPrefix(name, resolvedContractsPrefix) || !strings.HasSuffix(name, ".csv") {
		return time.Time{}, false
	}
	stamp := strings.TrimSuffix(strings.TrimPrefix(name, resolvedContractsPrefix), ".csv")
	generated, err := time.Parse(resolvedTimestampLayout, stamp)
	if err != nil {
		return time.Time{}, false
	}
	return generated, true
}

func readResolvedContracts(path string) ([]Contract, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read resolved contract header: %w", err)
	}
	column := make(map[string]int, len(headers))
	for i, header := range headers {
		column[strings.ToUpper(strings.TrimSpace(header))] = i
	}
	get := func(row []string, name string) string {
		if index, ok := column[name]; ok && index < len(row) {
			return strings.TrimSpace(row[index])
		}
		return ""
	}

	var contracts []Contract
	for rowNumber := 2; ; rowNumber++ {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read resolved contract row %d: %w", rowNumber, err)
		}
		contract := Contract{
			SecurityID:      get(row, "SECURITY_ID"),
			Underlying:      strings.ToUpper(get(row, "UNDERLYING")),
			Symbol:          get(row, "SYMBOL"),
			Instrument:      strings.ToUpper(get(row, "INSTRUMENT")),
			ExchangeSegment: strings.ToUpper(get(row, "EXCHANGE_SEGMENT")),
			Expiry:          get(row, "EXPIRY"),
			Strike:          get(row, "STRIKE"),
			OptionType:      strings.ToUpper(get(row, "OPTION_TYPE")),
		}
		if contract.SecurityID == "" || contract.Underlying == "" || contract.Instrument == "" {
			return nil, fmt.Errorf("resolved contract row %d is incomplete", rowNumber)
		}
		contracts = append(contracts, contract)
	}
	if len(contracts) == 0 {
		return nil, errors.New("resolved contract cache is empty")
	}
	return contracts, nil
}

func saveResolvedContracts(csvPath string, generated time.Time, contracts []Contract) (string, error) {
	directory := filepath.Dir(csvPath)
	name := resolvedContractsPrefix + generated.UTC().Format(resolvedTimestampLayout) + ".csv"
	target := filepath.Join(directory, name)

	file, err := os.CreateTemp(directory, resolvedContractsPrefix+"tmp-")
	if err != nil {
		return "", err
	}
	temporary := file.Name()
	defer os.Remove(temporary)

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{"SECURITY_ID", "UNDERLYING", "SYMBOL", "INSTRUMENT", "EXCHANGE_SEGMENT", "EXPIRY", "STRIKE", "OPTION_TYPE"}); err != nil {
		_ = file.Close()
		return "", err
	}
	for _, contract := range contracts {
		if err := writer.Write([]string{
			contract.SecurityID,
			contract.Underlying,
			contract.Symbol,
			contract.Instrument,
			contract.ExchangeSegment,
			contract.Expiry,
			contract.Strike,
			contract.OptionType,
		}); err != nil {
			_ = file.Close()
			return "", err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		_ = file.Close()
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(temporary, target); err != nil {
		return "", err
	}
	return target, nil
}

func resolveContracts(csvPath string, now time.Time, quotePrice func(string, string) (float64, error)) ([]Contract, error) {
	contracts, err := readContracts(csvPath)
	if err != nil {
		return nil, err
	}

	goldMFuture, err := selectNearestFuture(contracts, "GOLDM", goldExchange, "FUTCOM", now)
	if err != nil {
		return nil, err
	}
	goldFuture, err := selectNearestFuture(contracts, "GOLD", goldExchange, "FUTCOM", now)
	if err != nil {
		return nil, err
	}
	goldPetalFuture, err := selectNearestFuture(contracts, "GOLDPETAL", goldExchange, "FUTCOM", now)
	if err != nil {
		return nil, err
	}
	goldGuineaFuture, err := selectNearestFuture(contracts, "GOLDGUINEA", goldExchange, "FUTCOM", now)
	if err != nil {
		return nil, err
	}
	goldMPrice, err := quotePrice(goldSegment, goldMFuture.SecurityID)
	if err != nil {
		return nil, fmt.Errorf("get GOLDM futures price: %w", err)
	}
	goldMOptions, err := selectATMOptions(contracts, "GOLDM", goldMPrice, now)
	if err != nil {
		return nil, err
	}

	goldPrice, err := quotePrice(goldSegment, goldFuture.SecurityID)
	if err != nil {
		return nil, fmt.Errorf("get GOLD futures price: %w", err)
	}
	goldOptions, err := selectATMOptions(contracts, "GOLD", goldPrice, now)
	if err != nil {
		return nil, err
	}

	selected := make([]Contract, 0)
	selected = append(selected, goldMFuture)
	selected = append(selected, goldMOptions...)
	selected = append(selected, goldFuture)
	selected = append(selected, goldOptions...)
	selected = append(selected, goldPetalFuture, goldGuineaFuture)

	if err := validateSelectedContracts(selected); err != nil {
		return nil, err
	}
	return selected, nil
}

func readContracts(path string) ([]Contract, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open contract master %q: %w", path, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read contract master header: %w", err)
	}
	column := make(map[string]int, len(headers))
	for i, header := range headers {
		column[strings.ToUpper(strings.TrimSpace(header))] = i
	}
	get := func(row []string, names ...string) string {
		for _, name := range names {
			if index, ok := column[name]; ok && index < len(row) {
				return strings.TrimSpace(row[index])
			}
		}
		return ""
	}

	var contracts []Contract
	for rowNumber := 2; ; rowNumber++ {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read contract row %d: %w", rowNumber, err)
		}

		underlying := strings.ToUpper(get(row, "UNDERLYING_SYMBOL", "SM_SYMBOL_NAME"))
		exchange := strings.ToUpper(get(row, "EXCH_ID", "SEM_EXM_EXCH_ID"))
		segment := strings.ToUpper(get(row, "SEGMENT", "SEM_SEGMENT"))
		instrument := strings.ToUpper(get(row, "INSTRUMENT", "SEM_INSTRUMENT_NAME"))
		if !isSupportedRow(underlying, exchange, segment, instrument) {
			continue
		}

		securityID := get(row, "SECURITY_ID", "SEM_SMST_SECURITY_ID")
		if securityID == "" {
			return nil, fmt.Errorf("contract row %d has no security ID", rowNumber)
		}
		expiry := get(row, "EXPIRY_DATE", "SM_EXPIRY_DATE", "SEM_EXPIRY_DATE")
		if _, err := parseDate(expiry); err != nil {
			return nil, fmt.Errorf("parse expiry on contract row %d: %w", rowNumber, err)
		}

		contracts = append(contracts, Contract{
			SecurityID:      securityID,
			Underlying:      underlying,
			Symbol:          get(row, "DISPLAY_NAME", "SEM_CUSTOM_SYMBOL", "SEM_TRADING_SYMBOL"),
			Instrument:      instrument,
			ExchangeSegment: dhanExchangeSegment(exchange),
			Expiry:          expiry,
			Strike:          get(row, "STRIKE_PRICE", "SEM_STRIKE_PRICE"),
			OptionType:      strings.ToUpper(get(row, "OPTION_TYPE", "SEM_OPTION_TYPE")),
			exchange:        exchange,
			segment:         segment,
		})
	}
	return contracts, nil
}

func isSupportedRow(underlying, exchange, segment, instrument string) bool {
	switch underlying {
	case "GOLD", "GOLDM", "GOLDPETAL", "GOLDGUINEA":
		return exchange == goldExchange && segment == "M" && (instrument == "FUTCOM" || instrument == "OPTFUT")
	default:
		return false
	}
}

func dhanExchangeSegment(exchange string) string {
	if exchange == goldExchange {
		return goldSegment
	}
	return ""
}

func selectNearestFuture(contracts []Contract, underlying, exchange, instrument string, now time.Time) (Contract, error) {
	var candidates []Contract
	today := dateOnly(now)
	for _, contract := range contracts {
		if strings.ToUpper(contract.Underlying) != underlying ||
			strings.ToUpper(contract.Instrument) != instrument {
			continue
		}
		if contract.exchange != "" && contract.exchange != exchange {
			continue
		}
		expiry, err := parseDate(contract.Expiry)
		if err != nil {
			return Contract{}, fmt.Errorf("parse %s future expiry %q: %w", underlying, contract.Expiry, err)
		}
		if expiry.Before(today) {
			continue
		}
		candidates = append(candidates, contract)
	}
	return nearestFuture(candidates, underlying, exchange)
}

func nearestFuture(candidates []Contract, underlying, exchange string) (Contract, error) {
	if len(candidates) == 0 {
		return Contract{}, fmt.Errorf("no active %s %s contract found", underlying, exchange)
	}
	sort.Slice(candidates, func(i, j int) bool {
		left, _ := parseDate(candidates[i].Expiry)
		right, _ := parseDate(candidates[j].Expiry)
		if !left.Equal(right) {
			return left.Before(right)
		}
		if candidates[i].SecurityID != candidates[j].SecurityID {
			return candidates[i].SecurityID < candidates[j].SecurityID
		}
		return candidates[i].Symbol < candidates[j].Symbol
	})
	return candidates[0], nil
}

func selectATMOptions(contracts []Contract, underlying string, price float64, now time.Time) ([]Contract, error) {
	if math.IsNaN(price) || math.IsInf(price, 0) || price <= 0 {
		return nil, fmt.Errorf("invalid %s futures price %.4f", underlying, price)
	}

	today := dateOnly(now)
	var nearestExpiry time.Time
	for _, contract := range contracts {
		if contract.Underlying != underlying || contract.Instrument != "OPTFUT" ||
			(contract.exchange != "" && contract.exchange != goldExchange) {
			continue
		}
		expiry, err := parseDate(contract.Expiry)
		if err != nil {
			return nil, fmt.Errorf("parse %s option expiry %q: %w", underlying, contract.Expiry, err)
		}
		if !expiry.Before(today) && (nearestExpiry.IsZero() || expiry.Before(nearestExpiry)) {
			nearestExpiry = expiry
		}
	}
	if nearestExpiry.IsZero() {
		return nil, fmt.Errorf("no active %s option expiry found", underlying)
	}

	byStrike := make(map[float64]map[string]Contract)
	for _, contract := range contracts {
		if contract.Underlying != underlying || contract.Instrument != "OPTFUT" ||
			(contract.exchange != "" && contract.exchange != goldExchange) {
			continue
		}
		expiry, _ := parseDate(contract.Expiry)
		if !expiry.Equal(nearestExpiry) {
			continue
		}
		optionType := strings.ToUpper(strings.TrimSpace(contract.OptionType))
		if optionType != "CE" && optionType != "PE" {
			return nil, fmt.Errorf("invalid %s option type %q for expiry %s; want CE or PE", underlying, contract.OptionType, formatDate(nearestExpiry))
		}
		strike, err := strconv.ParseFloat(strings.TrimSpace(contract.Strike), 64)
		if err != nil || math.IsNaN(strike) || math.IsInf(strike, 0) {
			return nil, fmt.Errorf("invalid %s option strike %q for expiry %s", underlying, contract.Strike, formatDate(nearestExpiry))
		}
		optionsAtStrike := byStrike[strike]
		if optionsAtStrike == nil {
			optionsAtStrike = make(map[string]Contract, 2)
			byStrike[strike] = optionsAtStrike
		}
		if _, exists := optionsAtStrike[optionType]; exists {
			return nil, fmt.Errorf("duplicate %s %s contract for expiry %s strike %.4f", underlying, optionType, formatDate(nearestExpiry), strike)
		}
		contract.OptionType = optionType
		byStrike[strike][optionType] = contract
	}

	strikes := make([]float64, 0, len(byStrike))
	for strike := range byStrike {
		strikes = append(strikes, strike)
	}
	sort.Float64s(strikes)
	if len(strikes) < 11 {
		return nil, fmt.Errorf("%s expiry %s has only %d listed strikes; need at least 11", underlying, formatDate(nearestExpiry), len(strikes))
	}
	atmIndex := nearestStrikeIndex(strikes, price)
	if atmIndex < 5 || atmIndex+5 >= len(strikes) {
		return nil, fmt.Errorf("%s expiry %s does not have five listed strikes on both sides of ATM %.4f", underlying, formatDate(nearestExpiry), strikes[atmIndex])
	}

	selected := make([]Contract, 0, 22)
	for _, strike := range strikes[atmIndex-5 : atmIndex+6] {
		for _, optionType := range []string{"CE", "PE"} {
			contract, ok := byStrike[strike][optionType]
			if !ok {
				return nil, fmt.Errorf("missing %s %s contract for expiry %s strike %.4f", underlying, optionType, formatDate(nearestExpiry), strike)
			}
			selected = append(selected, contract)
		}
	}
	return selected, nil
}

func nearestStrikeIndex(strikes []float64, price float64) int {
	nearest := 0
	for i := 1; i < len(strikes); i++ {
		currentDistance := math.Abs(strikes[i] - price)
		nearestDistance := math.Abs(strikes[nearest] - price)
		if currentDistance < nearestDistance ||
			(currentDistance == nearestDistance && strikes[i] < strikes[nearest]) {
			nearest = i
		}
	}
	return nearest
}

func validateSelectedContracts(contracts []Contract) error {
	seen := make(map[string]Contract, len(contracts))
	for _, contract := range contracts {
		if contract.SecurityID == "" {
			return fmt.Errorf("selected %s contract has empty security ID", contract.Underlying)
		}
		if previous, exists := seen[contract.SecurityID]; exists {
			return fmt.Errorf("duplicate security ID %q in %s and %s", contract.SecurityID, previous.Symbol, contract.Symbol)
		}
		seen[contract.SecurityID] = contract
	}
	return nil
}

// PrintContracts prints the fixed startup selection as a compact summary.
func PrintContracts(contracts []Contract) {
	futures := make(map[string]Contract)
	options := make(map[string][]Contract)
	for _, contract := range contracts {
		if contract.Instrument == "FUTCOM" {
			futures[contract.Underlying] = contract
		} else if contract.Instrument == "OPTFUT" {
			options[contract.Underlying] = append(options[contract.Underlying], contract)
		}
	}

	futureCount, optionCount := 0, 0
	for _, underlying := range resolvedUnderlyings {
		fmt.Println(underlying)
		if future, ok := futures[underlying]; ok {
			fmt.Printf("  Future: %s\n", future.Symbol)
			futureCount++
		}
		if selected := options[underlying]; len(selected) > 0 {
			strikeSet := make(map[float64]struct{})
			ceCount, peCount := 0, 0
			for _, contract := range selected {
				strike, err := strconv.ParseFloat(contract.Strike, 64)
				if err == nil {
					strikeSet[strike] = struct{}{}
				}
				switch contract.OptionType {
				case "CE":
					ceCount++
				case "PE":
					peCount++
				}
			}
			strikes := make([]float64, 0, len(strikeSet))
			for strike := range strikeSet {
				strikes = append(strikes, strike)
			}
			sort.Float64s(strikes)
			if len(strikes) > 0 {
				fmt.Printf("  ATM: %s\n", formatNumber(strikes[len(strikes)/2]))
			}
			fmt.Printf("  CE: %d | PE: %d\n", ceCount, peCount)
			optionCount += len(selected)
		}
		fmt.Println()
	}
	fmt.Println("TOTAL")
	fmt.Printf("  Futures: %d\n  Options: %d\n  Instruments: %d\n", futureCount, optionCount, futureCount+optionCount)
}

func formatNumber(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func formatDate(value time.Time) string {
	return value.Format("2006-01-02")
}

func dateOnly(value time.Time) time.Time {
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, value.Location())
}

func parseDate(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	for _, layout := range []string{
		"2006-01-02 15:04:05",
		"2006-01-02",
		time.RFC3339,
	} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return dateOnly(parsed), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported expiry format %q", value)
}

func findRoot() (string, error) {
	directory, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory, nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return "", fmt.Errorf("could not find repository root")
		}
		directory = parent
	}
}
