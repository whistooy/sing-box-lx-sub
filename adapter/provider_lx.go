package adapter

// lx: sub — provider interface and manager definitions

import (
	"context"
	"encoding/json"
	"time"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
)

// Provider exposes the outbounds of a config provider (remote/local) to the
// outbound groups and the Clash API provider endpoints.
type Provider interface {
	Type() string
	Tag() string
	Outbounds() []Outbound
	Outbound(tag string) (Outbound, bool)
	Update() error
	UpdatedAt() time.Time
}

// ProviderSubscriptionInfo is implemented by providers that expose traffic/quota.
type ProviderSubscriptionInfo interface {
	SubscriptionInfo() SubscriptionInfo
}

type SubscriptionInfo struct {
	Upload   int64 `json:"upload,omitempty"`
	Download int64 `json:"download,omitempty"`
	Total    int64 `json:"total,omitempty"`
	Expire   int64 `json:"expire,omitempty"`
}

func (s SubscriptionInfo) MarshalJSON() ([]byte, error) {
	if s.Total == 0 && s.Upload == 0 && s.Download == 0 && s.Expire == 0 {
		return []byte("{}"), nil
	}
	return json.Marshal(map[string]int64{
		"Upload":   s.Upload,
		"Download": s.Download,
		"Total":    s.Total,
		"Expire":   s.Expire,
		"upload":   s.Upload,
		"download": s.Download,
		"total":    s.Total,
		"expire":   s.Expire,
	})
}

// ProviderRegistry resolves a provider "type" to concrete options and constructs
// the provider. It also satisfies option.ProviderOptionsRegistry so the config
// decoder can resolve provider option types.
type ProviderRegistry interface {
	option.ProviderOptionsRegistry
	CreateProvider(ctx context.Context, router Router, logFactory log.Factory, tag string, providerType string, options any) (Provider, error)
}

// ProviderUpdateCallback is invoked when a provider finishes refreshing its nodes.
type ProviderUpdateCallback func(providerTag string)

// ProviderManager is the per-instance registry of created providers.
type ProviderManager interface {
	Providers() []Provider
	Get(tag string) (Provider, bool)
	Create(ctx context.Context, router Router, logFactory log.Factory, tag string, providerType string, options any) error
	RegisterCallback(cb ProviderUpdateCallback) func()
	NotifyUpdated(providerTag string)
}

// ProviderFallbackTag is a direct outbound used as a safe fallback when a provider
// yields no nodes, preventing groups from failing initialization on empty tags.
const ProviderFallbackTag = "Helper-Direct"
