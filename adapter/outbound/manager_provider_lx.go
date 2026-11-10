package outbound

import (
	"context"
	"io"
	"os"
	"slices"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	E "github.com/sagernet/sing/common/exceptions"
)

// lx: sub — file-isolated dynamic provider outbound management

var dynamicScopeLogger = log.NewNOPFactory().NewLogger("outbound/dynamic")

// lx: sub — dynamic provider node identification without tag-string heuristics
func (m *Manager) isDynamicTagLocked(tag string) bool {
	if tag == adapter.ProviderFallbackTag {
		return true
	}
	if m.dynamicTags == nil {
		return false
	}
	_, ok := m.dynamicTags[tag]
	return ok
}

func (m *Manager) isDynamicTag(tag string) bool {
	m.access.RLock()
	defer m.access.RUnlock()
	return m.isDynamicTagLocked(tag)
}

func (m *Manager) RegisterDynamicTag(tag string) {
	m.access.Lock()
	defer m.access.Unlock()
	if m.dynamicTags == nil {
		m.dynamicTags = make(map[string]struct{})
	}
	m.dynamicTags[tag] = struct{}{}
}

func (m *Manager) targetScope(defaultScope *adapter.Scope, tag string) *adapter.Scope {
	if m.isDynamicTag(tag) {
		return m.getOrAllocateDynamicScope(tag, defaultScope, nil)
	}
	return defaultScope
}

func (m *Manager) checkDynamicClosed(outbound adapter.Outbound) error {
	if !m.dynamicClosed {
		return nil
	}
	if closer, ok := outbound.(io.Closer); ok {
		_ = closer.Close()
	}
	return os.ErrClosed
}

func (m *Manager) cleanupFailedCreate(tag string, outbound adapter.Outbound) {
	m.outbounds = slices.DeleteFunc(slices.Clone(m.outbounds), func(o adapter.Outbound) bool { return o == outbound })
	delete(m.outboundByTag, tag)
	delete(m.optionsByTag, tag)
	delete(m.dynamicTags, tag)
	if closer, ok := outbound.(io.Closer); ok {
		_ = closer.Close()
	}
}

func (m *Manager) getOrAllocateDynamicScope(tag string, parentScope *adapter.Scope, logger log.ContextLogger) *adapter.Scope {
	m.access.Lock()
	defer m.access.Unlock()
	if scope, ok := m.dynamicScopes[tag]; ok {
		return scope
	}
	var parentCtx context.Context
	if parentScope != nil {
		parentCtx = parentScope.Context()
	} else {
		parentCtx = context.Background()
	}
	scopeLogger := logger
	if scopeLogger == nil {
		scopeLogger = dynamicScopeLogger
	}
	scope := adapter.NewScope(parentCtx, scopeLogger)
	m.dynamicScopes[tag] = scope
	return scope
}

func (m *Manager) startDynamicLifecycle(ctx context.Context, logger log.ContextLogger, outbound adapter.Outbound, inboundType string, tag string, stage adapter.StartStage) (*adapter.Scope, error) {
	if stage != adapter.StartStateStarted {
		return nil, nil
	}
	lifecycle, isLifecycle := outbound.(adapter.Lifecycle)
	if !isLifecycle {
		return nil, nil
	}
	scope := adapter.NewScope(ctx, logger)
	for _, s := range adapter.ListStartStages {
		if err := scope.Start("outbound/"+inboundType+"["+tag+"]", lifecycle, s); err != nil {
			_ = scope.Close()
			return nil, err
		}
	}
	return scope, nil
}

// Replace builds and starts the new dynamic outbound in its own scope before atomically
// publishing it. If creation or start fails, the existing outbound is untouched.
func (m *Manager) Replace(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, inboundType string, options any) error {
	if tag == "" {
		return os.ErrInvalid
	}
	m.access.RLock()
	if m.dynamicClosed {
		m.access.RUnlock()
		return os.ErrClosed
	}
	stage := m.stage
	m.access.RUnlock()

	outbound, err := m.registry.CreateOutbound(ctx, router, logger, tag, inboundType, options)
	if err != nil {
		return err
	}
	newScope, err := m.startDynamicLifecycle(ctx, logger, outbound, inboundType, tag, stage)
	if err != nil {
		if closer, ok := outbound.(io.Closer); ok {
			_ = closer.Close()
		}
		return err
	}
	m.access.Lock()
	if m.dynamicClosed {
		m.access.Unlock()
		if newScope != nil {
			_ = newScope.Close()
		}
		if closer, ok := outbound.(io.Closer); ok {
			_ = closer.Close()
		}
		return os.ErrClosed
	}
	old, hadOld := m.outboundByTag[tag]
	oldScope := m.dynamicScopes[tag]
	m.outboundByTag[tag] = outbound
	m.optionsByTag[tag] = managedOptions{outboundType: inboundType, options: options}
	if m.dynamicTags == nil {
		m.dynamicTags = make(map[string]struct{})
	}
	m.dynamicTags[tag] = struct{}{}
	if newScope != nil {
		m.dynamicScopes[tag] = newScope
	} else {
		delete(m.dynamicScopes, tag)
	}
	if hadOld {
		next := make([]adapter.Outbound, len(m.outbounds))
		copy(next, m.outbounds)
		for i, item := range next {
			if item == old {
				next[i] = outbound
				break
			}
		}
		m.outbounds = next
		if m.defaultOutbound == old {
			m.defaultOutbound = outbound
		}
	} else {
		m.outbounds = append(slices.Clone(m.outbounds), outbound)
		if m.defaultTag != "" && tag == m.defaultTag {
			m.defaultOutbound = outbound
		}
	}
	m.access.Unlock()

	// Clean up old scope and closer outside the lock
	if oldScope != nil {
		_ = oldScope.Close()
	}
	if closer, ok := old.(io.Closer); ok {
		_ = closer.Close()
	}
	return nil
}

// Remove cleans up a dynamic outbound and closes its lifecycle scope and resources.
func (m *Manager) Remove(tag string) error {
	m.access.Lock()
	old, loaded := m.outboundByTag[tag]
	if !loaded {
		m.access.Unlock()
		return nil
	}
	delete(m.outboundByTag, tag)
	delete(m.optionsByTag, tag)
	delete(m.dynamicTags, tag)
	oldScope := m.dynamicScopes[tag]
	delete(m.dynamicScopes, tag)
	next := make([]adapter.Outbound, 0, len(m.outbounds))
	for _, item := range m.outbounds {
		if item != old {
			next = append(next, item)
		}
	}
	m.outbounds = next
	if m.defaultOutbound == old {
		m.defaultOutbound = nil
		for _, o := range m.outbounds {
			if !m.isDynamicTagLocked(o.Tag()) {
				m.defaultOutbound = o
				break
			}
		}
	}
	m.access.Unlock()

	if oldScope != nil {
		_ = oldScope.Close()
	}
	if closer, ok := old.(io.Closer); ok {
		_ = closer.Close()
	}
	return nil
}

func (m *Manager) RegisterProviderCleanup(cleanup func() error) {
	m.access.Lock()
	defer m.access.Unlock()
	if m.dynamicClosed {
		_ = cleanup()
		return
	}
	m.providerCleanups = append(m.providerCleanups, cleanup)
}

func (m *Manager) closeDynamicScopes() error {
	m.access.Lock()
	m.dynamicClosed = true
	scopes := m.dynamicScopes
	m.dynamicScopes = make(map[string]*adapter.Scope)
	cleanups := m.providerCleanups
	m.providerCleanups = nil
	m.access.Unlock()
	var errs []error
	for _, cleanup := range cleanups {
		if err := cleanup(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, s := range scopes {
		if err := s.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return E.Errors(errs...)
}
