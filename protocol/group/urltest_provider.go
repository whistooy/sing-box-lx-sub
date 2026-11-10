package group

// lx: sub — dynamic member updates for URLTestGroup

import (
	"slices"

	"github.com/sagernet/sing-box/adapter"
)

func (s *URLTest) updateGroup() {
	if s.group == nil || s.providerTracker == nil {
		return
	}
	newOutbounds := s.providerTracker.AllOutbounds(s.outbound)
	s.group.updateOutbounds(newOutbounds)
}

func (g *URLTestGroup) updateOutbounds(newOutbounds []adapter.Outbound) {
	g.access.Lock()
	defer g.access.Unlock()

	current := g.getOutbounds()
	// Compare instance pointers: if all outbound pointers are identical and in the same
	// order, no nodes were added, removed, or replaced. If any node was replaced with a
	// new instance (even under an identical tag), slices.Equal returns false so we update.
	if slices.Equal(current, newOutbounds) {
		return
	}
	snapshot := slices.Clone(newOutbounds)
	g.outbounds = snapshot
	g.outboundsSnapshot.Store(&snapshot)

	var instanceChanged bool
	selectedTCP, selectedUDP := g.selectedLx()
	if selectedTCP != nil {
		if idx := slices.IndexFunc(newOutbounds, func(o adapter.Outbound) bool { return o.Tag() == selectedTCP.Tag() }); idx >= 0 {
			if newOutbounds[idx] != selectedTCP {
				instanceChanged = true
			}
			g.setSelectedTCPLx(newOutbounds[idx])
		} else {
			g.setSelectedTCPLx(nil)
			instanceChanged = true
		}
	}
	if selectedUDP != nil {
		if idx := slices.IndexFunc(newOutbounds, func(o adapter.Outbound) bool { return o.Tag() == selectedUDP.Tag() }); idx >= 0 {
			if newOutbounds[idx] != selectedUDP {
				instanceChanged = true
			}
			g.setSelectedUDPLx(newOutbounds[idx])
		} else {
			g.setSelectedUDPLx(nil)
			instanceChanged = true
		}
	}

	if g.balancer != nil {
		g.rebuildPool()
	}

	if instanceChanged && g.started {
		g.interruptGroup.Interrupt(g.interruptExternalConnections)
	}

	if g.started {
		g.performUpdateCheck()
	}
}
