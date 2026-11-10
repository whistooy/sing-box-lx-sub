package outbound

import (
	"context"
	"os"
	"strings"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
)

var _ adapter.OutboundManager = (*Manager)(nil)

type Manager struct {
	registry                adapter.OutboundRegistry
	endpoint                adapter.EndpointManager
	defaultTag              string
	access                  sync.RWMutex
	outbounds               []adapter.Outbound
	outboundByTag           map[string]adapter.Outbound
	defaultOutbound         adapter.Outbound
	defaultOutboundFallback func() (adapter.Outbound, error)
	stage                   adapter.StartStage
	// lx:begin chain
	// SPEC 073: опции созданных outbound'ов по тегу (фабрика звеньев цепочки
	// пересоздаёт узел из них) и внутренние outbound'ы (хопы цепочки) — видны
	// Outbound(tag), не входят в Outbounds(), не стартуются/закрываются здесь.
	optionsByTag  map[string]managedOptions
	internalByTag map[string]adapter.Outbound
	// lx:end chain
	dynamicScopes    map[string]*adapter.Scope // lx: sub — managed lifecycle scopes for dynamic nodes
	dynamicTags      map[string]struct{}       // lx: sub — tracked dynamic provider tags
	dynamicClosed    bool                      // lx: sub — closed flag for dynamic scopes
	providerCleanups []func() error            // lx: sub — cleanup callbacks for dynamic providers on shutdown
}

func NewManager(registry adapter.OutboundRegistry, endpoint adapter.EndpointManager, defaultTag string) *Manager {
	return &Manager{
		registry:      registry,
		endpoint:      endpoint,
		defaultTag:    defaultTag,
		outboundByTag: make(map[string]adapter.Outbound),
		// lx:begin chain
		optionsByTag:  make(map[string]managedOptions),
		internalByTag: make(map[string]adapter.Outbound),
		// lx:end chain
		dynamicScopes: make(map[string]*adapter.Scope),
		dynamicTags:   make(map[string]struct{}),
	}
}

func (m *Manager) Initialize(defaultOutboundFallback func() (adapter.Outbound, error)) {
	m.defaultOutboundFallback = defaultOutboundFallback
}

func (m *Manager) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	m.access.Lock()
	m.stage = stage // lx: sub — record current start stage for dynamic lifecycle
	if stage == adapter.StartStateInitialize {
		if m.defaultTag != "" && m.defaultOutbound == nil {
			defaultEndpoint, loaded := m.endpoint.Get(m.defaultTag)
			if !loaded {
				m.access.Unlock()
				return E.New("default outbound not found: ", m.defaultTag)
			}
			m.defaultOutbound = defaultEndpoint
		}
		if m.defaultOutbound == nil {
			directOutbound, err := m.defaultOutboundFallback()
			if err != nil {
				m.access.Unlock()
				return E.Cause(err, "create direct outbound for fallback")
			}
			m.outbounds = append(m.outbounds, directOutbound)
			m.outboundByTag[directOutbound.Tag()] = directOutbound
			m.defaultOutbound = directOutbound
		}
	}
	outbounds := m.outbounds
	m.access.Unlock()
	if stage == adapter.StartStateStart {
		scope.Add(m.closeDynamicScopes)
		var endpoints []adapter.Outbound
		if m.endpoint != nil {
			endpoints = common.Map(m.endpoint.Endpoints(), func(it adapter.Endpoint) adapter.Outbound { return it })
		}
		return m.startOutbounds(scope, append(outbounds, endpoints...))
	}
	for _, outbound := range outbounds {
		lifecycle, isLifecycle := outbound.(adapter.Lifecycle)
		if !isLifecycle {
			continue
		}
		targetScope := m.targetScope(scope, outbound.Tag())
		name := "outbound/" + outbound.Type() + "[" + outbound.Tag() + "]"
		err := targetScope.Start(name, lifecycle, stage)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) startOutbounds(scope *adapter.Scope, outbounds []adapter.Outbound) error {
	started := make(map[string]bool)
	for {
		canContinue := false
	startOne:
		for _, outboundToStart := range outbounds {
			outboundTag := outboundToStart.Tag()
			if started[outboundTag] {
				continue
			}
			dependencies := outboundToStart.Dependencies()
			for _, dependency := range dependencies {
				if !started[dependency] {
					continue startOne
				}
			}
			started[outboundTag] = true
			canContinue = true
			if endpoint, isEndpoint := outboundToStart.(adapter.Endpoint); isEndpoint && m.endpoint != nil {
				err := m.endpoint.StartEndpoint(endpoint)
				if err != nil {
					return err
				}
				continue
			}
			lifecycle, isLifecycle := outboundToStart.(adapter.Lifecycle)
			if !isLifecycle {
				continue
			}
			name := "outbound/" + outboundToStart.Type() + "[" + outboundTag + "]"
			targetScope := m.targetScope(scope, outboundTag)
			err := targetScope.Start(name, lifecycle, adapter.StartStateStart)
			if err != nil {
				return err
			}
		}
		if len(started) == len(outbounds) {
			break
		}
		if canContinue {
			continue
		}
		currentOutbound := common.Find(outbounds, func(it adapter.Outbound) bool {
			return !started[it.Tag()]
		})
		var lintOutbound func(oTree []string, oCurrent adapter.Outbound) error
		lintOutbound = func(oTree []string, oCurrent adapter.Outbound) error {
			problemOutboundTag := common.Find(oCurrent.Dependencies(), func(it string) bool {
				return !started[it]
			})
			if common.Contains(oTree, problemOutboundTag) {
				return E.New("circular outbound dependency: ", strings.Join(oTree, " -> "), " -> ", problemOutboundTag)
			}
			problemOutbound := common.Find(outbounds, func(it adapter.Outbound) bool {
				return it.Tag() == problemOutboundTag
			})
			if problemOutbound == nil {
				return E.New("dependency[", problemOutboundTag, "] not found for outbound[", oCurrent.Tag(), "]")
			}
			return lintOutbound(append(oTree, problemOutboundTag), problemOutbound)
		}
		return lintOutbound([]string{currentOutbound.Tag()}, currentOutbound)
	}
	return nil
}

func (m *Manager) Outbounds() []adapter.Outbound {
	m.access.RLock()
	defer m.access.RUnlock()
	return m.outbounds
}

func (m *Manager) Outbound(tag string) (adapter.Outbound, bool) {
	m.access.RLock()
	outbound, found := m.outboundByTag[tag]
	m.access.RUnlock()
	if found {
		return outbound, true
	}
	// lx:begin chain
	m.access.RLock()
	outbound, found = m.internalByTag[tag]
	m.access.RUnlock()
	if found {
		return outbound, true
	}
	// lx:end chain
	return m.endpoint.Get(tag)
}

func (m *Manager) Default() adapter.Outbound {
	m.access.RLock()
	defer m.access.RUnlock()
	return m.defaultOutbound
}

func (m *Manager) Create(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, inboundType string, options any) error {
	if tag == "" {
		return os.ErrInvalid
	}
	outbound, err := m.registry.CreateOutbound(ctx, router, logger, tag, inboundType, options)
	if err != nil {
		return err
	}
	m.access.Lock()
	defer m.access.Unlock()
	if err = m.checkDynamicClosed(outbound); err != nil {
		return err
	}
	_, loaded := m.outboundByTag[tag]
	if loaded {
		return E.New("duplicate outbound tag: ", tag)
	}
	m.outbounds = append(m.outbounds, outbound)
	m.outboundByTag[tag] = outbound
	m.optionsByTag[tag] = managedOptions{outboundType: inboundType, options: options} // lx: chain
	isExplicitDefault := m.defaultTag != "" && tag == m.defaultTag
	isImplicitDefault := m.defaultTag == "" && m.defaultOutbound == nil && !m.isDynamicTagLocked(tag)
	if isExplicitDefault || isImplicitDefault {
		m.defaultOutbound = outbound
	}
	if tag == adapter.ProviderFallbackTag {
		m.dynamicTags[tag] = struct{}{}
	}
	// lx: sub — start lifecycle for dynamic outbound
	if scope, err := m.startDynamicLifecycle(ctx, logger, outbound, inboundType, tag, m.stage); err != nil {
		m.cleanupFailedCreate(tag, outbound)
		return err
	} else if scope != nil {
		m.dynamicScopes[tag] = scope
	}
	return nil
}
