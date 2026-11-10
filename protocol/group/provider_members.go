package group

// lx: sub — dynamic provider member tracker for outbound groups

import (
	"context"
	"slices"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/service"
)

// providerTracker tracks dynamic outbounds contributed by config providers
// (all providers when useAllProviders is set, or specific named provider tags).
type providerTracker struct {
	ctx             context.Context
	staticTags      []string
	providerTags    []string
	useAllProviders bool
	active          bool

	access          sync.RWMutex
	cachedTags      []string
	cachedOutbounds map[string]adapter.Outbound
	onUpdate        func()
	unregister      func()
}

func newProviderTracker(ctx context.Context, staticTags []string, providerTags []string, useAllProviders bool) *providerTracker {
	t := &providerTracker{
		ctx:             ctx,
		staticTags:      slices.Clone(staticTags),
		providerTags:    providerTags,
		useAllProviders: useAllProviders,
		active:          useAllProviders || len(providerTags) > 0,
		cachedOutbounds: make(map[string]adapter.Outbound),
	}
	t.refresh()
	return t
}

func (t *providerTracker) refresh() {
	if !t.active {
		return
	}
	manager := service.FromContext[adapter.ProviderManager](t.ctx)
	if manager == nil {
		return
	}
	var (
		tags      []string
		outbounds = make(map[string]adapter.Outbound)
	)
	appendProvider := func(p adapter.Provider) {
		for _, detour := range p.Outbounds() {
			tag := detour.Tag()
			if _, exists := outbounds[tag]; !exists {
				tags = append(tags, tag)
				outbounds[tag] = detour
			}
		}
	}
	if t.useAllProviders {
		for _, p := range manager.Providers() {
			appendProvider(p)
		}
	} else {
		for _, tag := range t.providerTags {
			if p, loaded := manager.Get(tag); loaded {
				appendProvider(p)
			}
		}
	}
	if len(tags) == 0 && len(t.staticTags) == 0 && t.hasResolvableProvider() {
		tags = []string{adapter.ProviderFallbackTag}
		if om := service.FromContext[adapter.OutboundManager](t.ctx); om != nil {
			if fallback, loaded := om.Outbound(adapter.ProviderFallbackTag); loaded {
				outbounds[adapter.ProviderFallbackTag] = fallback
			}
		}
	}
	t.access.Lock()
	t.cachedTags = tags
	t.cachedOutbounds = outbounds
	t.access.Unlock()
}

// hasResolvableProvider reports whether at least one of the referenced providers
// exists. A provider that exists but currently yields no nodes still gets the
// Helper-Direct fallback; a reference to a provider that does not exist does not
// (it is a config error and must not silently route through direct).
func (t *providerTracker) hasResolvableProvider() bool {
	if !t.active {
		return false
	}
	manager := service.FromContext[adapter.ProviderManager](t.ctx)
	if manager == nil {
		return false
	}
	if t.useAllProviders {
		return len(manager.Providers()) > 0
	}
	for _, tag := range t.providerTags {
		if _, loaded := manager.Get(tag); loaded {
			return true
		}
	}
	return false
}

func (t *providerTracker) InitialTags() []string {
	if !t.active {
		return nil
	}
	t.access.RLock()
	defer t.access.RUnlock()
	return slices.Clone(t.cachedTags)
}

func (t *providerTracker) Start(scope *adapter.Scope, onUpdate func()) {
	if t == nil || !t.active {
		return
	}
	t.access.Lock()
	t.onUpdate = onUpdate
	t.access.Unlock()
	manager := service.FromContext[adapter.ProviderManager](t.ctx)
	if manager != nil {
		t.unregister = manager.RegisterCallback(func(providerTag string) {
			if !t.useAllProviders && !slices.Contains(t.providerTags, providerTag) {
				return
			}
			t.refresh()
			t.access.RLock()
			cb := t.onUpdate
			t.access.RUnlock()
			if cb != nil {
				cb()
			}
		})
		if scope != nil && t.unregister != nil {
			scope.Add(func() error {
				t.unregister()
				return nil
			})
		}
	}
}

// All returns staticTags plus currently resolved provider tags.
func (t *providerTracker) All() []string {
	if t == nil {
		return nil
	}
	if !t.active {
		return slices.Clone(t.staticTags)
	}
	t.access.RLock()
	defer t.access.RUnlock()
	if len(t.staticTags) == 0 {
		return slices.Clone(t.cachedTags)
	}
	tags := make([]string, 0, len(t.staticTags)+len(t.cachedTags))
	tags = append(tags, t.staticTags...)
	for _, tag := range t.cachedTags {
		if !slices.Contains(tags, tag) {
			tags = append(tags, tag)
		}
	}
	return tags
}

// Outbound returns the resolved provider outbound for a tag if tracked.
func (t *providerTracker) Outbound(tag string) (adapter.Outbound, bool) {
	if t == nil || !t.active {
		return nil, false
	}
	t.access.RLock()
	detour, loaded := t.cachedOutbounds[tag]
	t.access.RUnlock()
	if !loaded {
		if om := service.FromContext[adapter.OutboundManager](t.ctx); om != nil {
			return om.Outbound(tag)
		}
	}
	return detour, loaded
}

// AllOutbounds returns all outbounds (static outbounds from OutboundManager + provider outbounds).
func (t *providerTracker) AllOutbounds(outboundManager adapter.OutboundManager) []adapter.Outbound {
	if t == nil {
		return nil
	}
	outbounds := make([]adapter.Outbound, 0, len(t.staticTags))
	if outboundManager != nil {
		for _, tag := range t.staticTags {
			if detour, loaded := outboundManager.Outbound(tag); loaded {
				outbounds = append(outbounds, detour)
			}
		}
	}
	if !t.active {
		return outbounds
	}
	t.access.RLock()
	defer t.access.RUnlock()
	for _, tag := range t.cachedTags {
		if detour, loaded := t.cachedOutbounds[tag]; loaded {
			outbounds = append(outbounds, detour)
		} else if outboundManager != nil {
			if detour, loaded := outboundManager.Outbound(tag); loaded {
				outbounds = append(outbounds, detour)
			}
		}
	}
	return outbounds
}
