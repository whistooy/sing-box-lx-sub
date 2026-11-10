package group

import (
	"slices"

	"github.com/sagernet/sing-box/adapter"
)

// lx: sub — file-isolated dynamic selector provider tracking and resolution

func (s *Selector) startProviderTracker(scope *adapter.Scope) {
	if s.providerTracker == nil {
		return
	}
	s.providerTracker.Start(scope, func() {
		current := s.selected.Load()
		all := s.All()
		if len(all) == 0 {
			return
		}
		if current == nil || !slices.Contains(all, current.Tag()) {
			s.SelectOutbound(all[0])
			return
		}
		if newOutbound, loaded := s.providerTracker.Outbound(current.Tag()); loaded && newOutbound != current {
			s.SelectOutbound(current.Tag())
		}
	})
}

func (s *Selector) resolveOutbound(tag string) (adapter.Outbound, bool) {
	if !slices.Contains(s.All(), tag) {
		return nil, false
	}
	if s.providerTracker != nil {
		if detour, loaded := s.providerTracker.Outbound(tag); loaded {
			return detour, true
		}
	}
	if detour, loaded := s.outbounds[tag]; loaded {
		return detour, true
	}
	return s.outbound.Outbound(tag)
}
