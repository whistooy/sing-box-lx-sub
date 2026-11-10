package provider

// lx: sub — provider manager implementation

import (
	"context"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
)

var _ adapter.ProviderManager = (*Manager)(nil)

// Manager holds the created providers for one instance and is registered in the
// service context so groups and the Clash API can resolve providers by tag.
type Manager struct {
	ctx            context.Context
	logger         logger.ContextLogger
	registry       adapter.ProviderRegistry
	access         sync.Mutex
	providers      []adapter.Provider
	providerByTag  map[string]adapter.Provider
	callbackAccess sync.Mutex
	nextCallbackID uint64
	callbacks      map[uint64]adapter.ProviderUpdateCallback
}

func NewManager(ctx context.Context, logger logger.ContextLogger, registry adapter.ProviderRegistry) *Manager {
	return &Manager{
		ctx:           ctx,
		logger:        logger,
		registry:      registry,
		providerByTag: make(map[string]adapter.Provider),
		callbacks:     make(map[uint64]adapter.ProviderUpdateCallback),
	}
}

func (m *Manager) RegisterCallback(cb adapter.ProviderUpdateCallback) func() {
	m.callbackAccess.Lock()
	m.nextCallbackID++
	id := m.nextCallbackID
	m.callbacks[id] = cb
	m.callbackAccess.Unlock()
	return func() {
		m.callbackAccess.Lock()
		delete(m.callbacks, id)
		m.callbackAccess.Unlock()
	}
}

func (m *Manager) NotifyUpdated(providerTag string) {
	m.callbackAccess.Lock()
	cbs := make([]adapter.ProviderUpdateCallback, 0, len(m.callbacks))
	for _, cb := range m.callbacks {
		cbs = append(cbs, cb)
	}
	m.callbackAccess.Unlock()
	for _, cb := range cbs {
		cb(providerTag)
	}
}

func (m *Manager) Providers() []adapter.Provider {
	m.access.Lock()
	defer m.access.Unlock()
	return append([]adapter.Provider(nil), m.providers...)
}

func (m *Manager) Get(tag string) (adapter.Provider, bool) {
	m.access.Lock()
	defer m.access.Unlock()
	p, loaded := m.providerByTag[tag]
	return p, loaded
}

func (m *Manager) Create(ctx context.Context, router adapter.Router, logFactory log.Factory, tag string, providerType string, options any) error {
	m.access.Lock()
	if _, loaded := m.providerByTag[tag]; loaded {
		m.access.Unlock()
		return E.New("provider already exists: ", tag)
	}
	m.access.Unlock()

	p, err := m.registry.CreateProvider(ctx, router, logFactory, tag, providerType, options)
	if err != nil {
		return E.Cause(err, "create provider[", tag, "]")
	}
	m.access.Lock()
	defer m.access.Unlock()
	if _, loaded := m.providerByTag[tag]; loaded {
		return E.New("provider already exists: ", tag)
	}
	m.providers = append(m.providers, p)
	m.providerByTag[tag] = p
	return nil
}
