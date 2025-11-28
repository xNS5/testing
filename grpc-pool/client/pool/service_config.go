package pool

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

type ServiceConfig struct {
	MethodConfig        []MethodConfig   `json:"methodConfig,omitempty"`
	LoadBalancingConfig []map[string]any `json:"loadBalancingConfig,omitempty"`
	LoadBalancingPolicy string           `json:"loadBalancingPolicy,omitempty"` // deprecated but supported
	RetryThrottling     *RetryThrottling `json:"retryThrottling,omitempty"`
}

type MethodConfig struct {
	Name                    []Name         `json:"name"`
	WaitForReady            *bool          `json:"waitForReady,omitempty"`
	Timeout                 string         `json:"timeout,omitempty"`
	MaxRequestMessageBytes  *int           `json:"maxRequestMessageBytes,omitempty"`
	MaxResponseMessageBytes *int           `json:"maxResponseMessageBytes,omitempty"`
	RetryPolicy             *RetryPolicy   `json:"retryPolicy,omitempty"`
	HedgingPolicy           *HedgingPolicy `json:"hedgingPolicy,omitempty"`
}

type Name struct {
	Service string `json:"service"`
	Method  string `json:"method"`
}

type RetryPolicy struct {
	MaxAttempts          int      `json:"maxAttempts"`
	InitialBackoff       string   `json:"initialBackoff"`
	MaxBackoff           string   `json:"maxBackoff"`
	BackoffMultiplier    float64  `json:"backoffMultiplier"`
	RetryableStatusCodes []string `json:"retryableStatusCodes"`
}

type HedgingPolicy struct {
	MaxAttempts         int      `json:"maxAttempts"`
	HedgingDelay        string   `json:"hedgingDelay"`
	NonFatalStatusCodes []string `json:"nonFatalStatusCodes"`
}

type RetryThrottling struct {
	MaxTokens  float64 `json:"maxTokens"`
	TokenRatio float64 `json:"tokenRatio"`
}

var validStatus = regexp.MustCompile(`^[A-Z_]+$`)

func (c *ServiceConfig) Validate() error {
	for _, mc := range c.MethodConfig {
		if err := mc.Validate(); err != nil {
			return err
		}
	}

	if c.RetryThrottling != nil {
		if c.RetryThrottling.MaxTokens <= 0 {
			return errors.New("retryThrottling.maxTokens must be > 0")
		}
		if c.RetryThrottling.TokenRatio <= 0 {
			return errors.New("retryThrottling.tokenRatio must be > 0")
		}
	}

	return nil
}

func (m *MethodConfig) Validate() error {
	if len(m.Name) == 0 {
		return errors.New("methodConfig.name must have at least one entry")
	}

	for _, n := range m.Name {
		if err := n.Validate(); err != nil {
			return err
		}
	}

	// Retry vs Hedging: cannot both be set
	if m.RetryPolicy != nil && m.HedgingPolicy != nil {
		return errors.New("retryPolicy and hedgingPolicy cannot both be set")
	}

	if m.RetryPolicy != nil {
		if err := m.RetryPolicy.Validate(); err != nil {
			return err
		}
	}

	if m.HedgingPolicy != nil {
		if err := m.HedgingPolicy.Validate(); err != nil {
			return err
		}
	}

	// Validate timeout
	if m.Timeout != "" {
		if _, err := time.ParseDuration(m.Timeout); err != nil {
			return fmt.Errorf("invalid timeout duration: %w", err)
		}
	}

	return nil
}

func (n *Name) Validate() error {
	if n.Service == "" && n.Method == "" {
		return nil
	}

	if n.Service == "" {
		return errors.New("name.service must not be empty")
	}

	return nil
}

func (r *RetryPolicy) Validate() error {
	if r.MaxAttempts < 2 {
		return errors.New("retryPolicy.maxAttempts must be >= 2")
	}

	if _, err := time.ParseDuration(r.InitialBackoff); err != nil {
		return fmt.Errorf("invalid retryPolicy.initialBackoff: %w", err)
	}

	if _, err := time.ParseDuration(r.MaxBackoff); err != nil {
		return fmt.Errorf("invalid retryPolicy.maxBackoff: %w", err)
	}

	if r.BackoffMultiplier < 1.0 {
		return errors.New("retryPolicy.backoffMultiplier must be >= 1.0")
	}

	if len(r.RetryableStatusCodes) == 0 {
		return errors.New("retryPolicy.retryableStatusCodes must not be empty")
	}

	for _, s := range r.RetryableStatusCodes {
		if !validStatus.MatchString(s) {
			return fmt.Errorf("invalid retryableStatusCode: %s", s)
		}
	}

	return nil
}

func (h *HedgingPolicy) Validate() error {
	if h.MaxAttempts < 2 {
		return errors.New("hedgingPolicy.maxAttempts must be >= 2")
	}

	if _, err := time.ParseDuration(h.HedgingDelay); err != nil {
		return fmt.Errorf("invalid hedgingPolicy.hedgingDelay: %w", err)
	}

	for _, s := range h.NonFatalStatusCodes {
		if !validStatus.MatchString(s) {
			return fmt.Errorf("invalid nonFatalStatusCode: %s", s)
		}
	}

	return nil
}

func (c *ServiceConfig) ToJSON() (string, error) {
	if err := c.Validate(); err != nil {
		return "", err
	}
	b, err := json.MarshalIndent(c, "", "  ")
	return string(b), err
}

func FromJSON(data string) (*ServiceConfig, error) {
	var cfg ServiceConfig
	if err := json.Unmarshal([]byte(data), &cfg); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}
