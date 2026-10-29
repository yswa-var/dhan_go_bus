package config

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base32"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	accessTokenURL = "https://auth.dhan.co/app/generateAccessToken"
	authFileName   = "auth.json"
	tokenLifetime  = 24 * time.Hour
)

type Config struct {
	clientID    string
	totpKey     string
	dpin        string
	databaseURL string
	http        *http.Client
	authPath    string
}

type auth struct {
	AccessToken string    `json:"accessToken"`
	GeneratedAt time.Time `json:"generatedAt"`
}

// NewConfig loads Dhan credentials from the root .env file.
func NewConfig() (*Config, error) {
	root, err := findRoot()
	if err != nil {
		return nil, err
	}
	env, err := loadEnv(filepath.Join(root, ".env"))
	if err != nil {
		return nil, err
	}

	config := &Config{
		clientID:    env["CLIENT_ID"],
		totpKey:     env["TOTP_KEY"],
		dpin:        env["DPIN"],
		databaseURL: env["DATABASE_URL"],
		http:        http.DefaultClient,
		authPath:    filepath.Join(root, authFileName),
	}
	if config.clientID == "" || config.totpKey == "" || config.dpin == "" {
		return nil, errors.New(".env must contain CLIENT_ID, TOTP_KEY, and DPIN")
	}
	return config, nil
}

// GetAuthToken returns the cached token or generates a fresh one when it is
// missing or older than 24 hours.
func (c *Config) GetAuthToken() (string, error) {
	return c.getAuthToken()
}

// ClientID returns the Dhan client identifier used by authenticated APIs.
func (c *Config) ClientID() string {
	return c.clientID
}

// DatabaseURL returns the PostgreSQL connection string used by Quest 004.
func (c *Config) DatabaseURL() string {
	return c.databaseURL
}

func (c *Config) getAuthToken() (string, error) {
	if cached, err := readAuth(c.authPath); err == nil && time.Since(cached.GeneratedAt) < tokenLifetime {
		return cached.AccessToken, nil
	}

	code, err := totp(c.totpKey, time.Now())
	if err != nil {
		return "", err
	}

	requestURL := accessTokenURL + "?" + url.Values{
		"dhanClientId": {c.clientID},
		"pin":          {c.dpin},
		"totp":         {code},
	}.Encode()
	request, err := http.NewRequest(http.MethodPost, requestURL, nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("Accept", "application/json")

	client := c.http
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("dhan authentication failed (%s): %s", response.Status, strings.TrimSpace(string(body)))
	}

	var result struct {
		AccessToken string `json:"accessToken"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("decode Dhan authentication response: %w", err)
	}
	if result.AccessToken == "" {
		return "", errors.New("Dhan authentication response did not contain accessToken")
	}

	created := auth{AccessToken: result.AccessToken, GeneratedAt: time.Now().UTC()}
	data, err := json.MarshalIndent(created, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(c.authPath, append(data, '\n'), 0600); err != nil {
		return "", fmt.Errorf("write %s: %w", c.authPath, err)
	}
	return created.AccessToken, nil
}

func readAuth(path string) (auth, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return auth{}, err
	}
	var cached auth
	if err := json.Unmarshal(data, &cached); err != nil {
		return auth{}, err
	}
	if cached.AccessToken == "" || cached.GeneratedAt.IsZero() {
		return auth{}, errors.New("invalid auth.json")
	}
	return cached, nil
}

func totp(secret string, now time.Time) (string, error) {
	key, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(strings.ToUpper(strings.TrimSpace(secret)))
	if err != nil {
		return "", fmt.Errorf("decode TOTP key: %w", err)
	}

	counter := uint64(now.Unix() / 30)
	message := make([]byte, 8)
	binary.BigEndian.PutUint64(message, counter)
	hash := hmac.New(sha1.New, key)
	_, _ = hash.Write(message)
	digest := hash.Sum(nil)
	offset := digest[len(digest)-1] & 0x0f
	value := (uint32(digest[offset])&0x7f)<<24 |
		uint32(digest[offset+1])<<16 |
		uint32(digest[offset+2])<<8 |
		uint32(digest[offset+3])
	return fmt.Sprintf("%06s", strconv.FormatUint(uint64(value%1_000_000), 10)), nil
}

func loadEnv(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	values := make(map[string]string)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		values[strings.TrimSpace(key)] = strings.Trim(strings.TrimSpace(value), "\"'")
	}
	return values, nil
}

func findRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, ".env")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("could not find .env in the current directory or its parents")
		}
		dir = parent
	}
}
