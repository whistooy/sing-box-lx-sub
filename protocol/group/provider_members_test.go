package group

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

type testStubOutbound struct {
	adapter.Outbound
	tag string
}

func (o *testStubOutbound) Tag() string            { return o.tag }
func (o *testStubOutbound) Network() []string      { return []string{N.NetworkTCP, N.NetworkUDP} }
func (o *testStubOutbound) Dependencies() []string { return nil }

type testStubProvider struct {
	tag       string
	outbounds []adapter.Outbound
}

func (p *testStubProvider) Type() string                  { return "remote" }
func (p *testStubProvider) Tag() string                   { return p.tag }
func (p *testStubProvider) Outbounds() []adapter.Outbound { return p.outbounds }
func (p *testStubProvider) Outbound(tag string) (adapter.Outbound, bool) {
	for _, o := range p.outbounds {
		if o.Tag() == tag {
			return o, true
		}
	}
	return nil, false
}
func (p *testStubProvider) Update() error        { return nil }
func (p *testStubProvider) UpdatedAt() time.Time { return time.Now() }

type testStubProviderManager struct {
	access    sync.Mutex
	providers map[string]*testStubProvider
	callbacks []adapter.ProviderUpdateCallback
}

func (m *testStubProviderManager) Providers() []adapter.Provider {
	m.access.Lock()
	defer m.access.Unlock()
	res := make([]adapter.Provider, 0, len(m.providers))
	for _, p := range m.providers {
		res = append(res, p)
	}
	return res
}

func (m *testStubProviderManager) Get(tag string) (adapter.Provider, bool) {
	m.access.Lock()
	defer m.access.Unlock()
	p, ok := m.providers[tag]
	if !ok {
		return nil, false
	}
	return p, true
}

func (m *testStubProviderManager) Create(ctx context.Context, router adapter.Router, logFactory log.Factory, tag string, providerType string, options any) error {
	return nil
}

func (m *testStubProviderManager) RegisterCallback(cb adapter.ProviderUpdateCallback) func() {
	m.access.Lock()
	defer m.access.Unlock()
	m.callbacks = append(m.callbacks, cb)
	return func() {}
}

func (m *testStubProviderManager) NotifyUpdated(tag string) {
	m.access.Lock()
	cbs := slices.Clone(m.callbacks)
	m.access.Unlock()
	for _, cb := range cbs {
		cb(tag)
	}
}

func TestProviderTrackerInactive(t *testing.T) {
	ctx := context.Background()
	tracker := newProviderTracker(ctx, []string{"direct"}, nil, false)
	if tracker.InitialTags() != nil {
		t.Fatalf("expected nil initial tags, got %v", tracker.InitialTags())
	}
	all := tracker.All()
	if !slices.Equal(all, []string{"direct"}) {
		t.Fatalf("expected [direct], got %v", all)
	}
}

func TestProviderTrackerEmptyFallback(t *testing.T) {
	mgr := &testStubProviderManager{
		providers: map[string]*testStubProvider{
			"empty-p": {tag: "empty-p", outbounds: nil},
		},
	}
	ctx := service.ContextWith[adapter.ProviderManager](context.Background(), mgr)
	tracker := newProviderTracker(ctx, nil, []string{"empty-p"}, false)

	all := tracker.All()
	expected := []string{adapter.ProviderFallbackTag}
	if !slices.Equal(all, expected) {
		t.Fatalf("expected %v, got %v", expected, all)
	}

	// Non-existent provider should not inject Helper-Direct
	trackerMissing := newProviderTracker(ctx, nil, []string{"nonexistent"}, false)
	if len(trackerMissing.All()) != 0 {
		t.Fatalf("expected empty tags for missing provider, got %v", trackerMissing.All())
	}

	// use_all_providers with 0 providers in manager should not inject Helper-Direct
	mgrEmpty := &testStubProviderManager{providers: map[string]*testStubProvider{}}
	ctxEmpty := service.ContextWith[adapter.ProviderManager](context.Background(), mgrEmpty)
	trackerAllEmpty := newProviderTracker(ctxEmpty, nil, nil, true)
	if len(trackerAllEmpty.All()) != 0 {
		t.Fatalf("expected empty tags for use_all_providers with 0 providers, got %v", trackerAllEmpty.All())
	}
}

func TestProviderTrackerDynamicUpdates(t *testing.T) {
	p := &testStubProvider{
		tag: "sub1",
		outbounds: []adapter.Outbound{
			&testStubOutbound{tag: "node-a"},
			&testStubOutbound{tag: "node-b"},
		},
	}
	mgr := &testStubProviderManager{
		providers: map[string]*testStubProvider{"sub1": p},
	}
	ctx := service.ContextWith[adapter.ProviderManager](context.Background(), mgr)
	tracker := newProviderTracker(ctx, []string{"static1"}, []string{"sub1"}, false)

	initial := tracker.All()
	if !slices.Equal(initial, []string{"static1", "node-a", "node-b"}) {
		t.Fatalf("unexpected initial tags: %v", initial)
	}

	var updated bool
	tracker.Start(nil, func() {
		updated = true
	})

	// Simulate provider update: node-a dropped, node-c added
	mgr.access.Lock()
	p.outbounds = []adapter.Outbound{
		&testStubOutbound{tag: "node-b"},
		&testStubOutbound{tag: "node-c"},
	}
	mgr.access.Unlock()

	mgr.NotifyUpdated("sub1")

	if !updated {
		t.Fatal("expected update callback to be invoked")
	}

	current := tracker.All()
	if !slices.Equal(current, []string{"static1", "node-b", "node-c"}) {
		t.Fatalf("unexpected updated tags: %v", current)
	}
}

func TestProviderTrackerDeduplication(t *testing.T) {
	p := &testStubProvider{
		tag: "sub1",
		outbounds: []adapter.Outbound{
			&testStubOutbound{tag: "node-a"},
			&testStubOutbound{tag: "node-b"},
		},
	}
	mgr := &testStubProviderManager{
		providers: map[string]*testStubProvider{"sub1": p},
	}
	ctx := service.ContextWith[adapter.ProviderManager](context.Background(), mgr)
	// staticTags already contains node-a
	tracker := newProviderTracker(ctx, []string{"node-a"}, []string{"sub1"}, false)

	all := tracker.All()
	if !slices.Equal(all, []string{"node-a", "node-b"}) {
		t.Fatalf("expected deduplicated [node-a, node-b], got %v", all)
	}
}

func TestProviderTrackerNodeReplacement(t *testing.T) {
	nodeA1 := &testStubOutbound{tag: "node-a"}
	p := &testStubProvider{
		tag:       "sub1",
		outbounds: []adapter.Outbound{nodeA1},
	}
	mgr := &testStubProviderManager{
		providers: map[string]*testStubProvider{"sub1": p},
	}
	ctx := service.ContextWith[adapter.ProviderManager](context.Background(), mgr)
	tracker := newProviderTracker(ctx, nil, []string{"sub1"}, false)

	scope := adapter.NewScope(ctx, log.NewNOPFactory().NewLogger(""))
	defer scope.Close()

	var updated bool
	tracker.Start(scope, func() {
		updated = true
	})

	out, ok := tracker.Outbound("node-a")
	if !ok || out != nodeA1 {
		t.Fatalf("expected nodeA1, got %v", out)
	}

	// Now replace node-a with a new instance
	nodeA2 := &testStubOutbound{tag: "node-a"}
	mgr.access.Lock()
	p.outbounds = []adapter.Outbound{nodeA2}
	mgr.access.Unlock()

	mgr.NotifyUpdated("sub1")
	if !updated {
		t.Fatal("expected update callback")
	}

	out2, ok2 := tracker.Outbound("node-a")
	if !ok2 || out2 != nodeA2 {
		t.Fatalf("expected nodeA2 after replacement, got %v", out2)
	}
}

func TestURLTestConcurrentUpdateAndSelect(t *testing.T) {
	nodeA1 := &testStubOutbound{tag: "node-a"}
	nodeA2 := &testStubOutbound{tag: "node-a"}
	nodeB := &testStubOutbound{tag: "node-b"}

	initial := []adapter.Outbound{nodeA1, nodeB}
	group := &URLTestGroup{
		outbounds: initial,
	}
	group.outboundsSnapshot.Store(&initial)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	// Concurrently read outbounds and run Select
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				obs := group.getOutbounds()
				if len(obs) != 2 {
					t.Errorf("expected 2 outbounds, got %d", len(obs))
					return
				}
			}
		}()
	}

	// Concurrently update outbounds with identical tags but different instances
	wg.Add(1)
	go func() {
		defer wg.Done()
		flip := false
		for ctx.Err() == nil {
			var newOuts []adapter.Outbound
			if flip {
				newOuts = []adapter.Outbound{nodeA1, nodeB}
			} else {
				newOuts = []adapter.Outbound{nodeA2, nodeB}
			}
			flip = !flip
			group.updateOutbounds(newOuts)
		}
	}()

	wg.Wait()
}

func TestURLTestSelectionStateRace(t *testing.T) {
	nodeA1 := &testStubOutbound{tag: "node-a"}
	nodeA2 := &testStubOutbound{tag: "node-a"}
	initial := []adapter.Outbound{nodeA1}
	group := &URLTestGroup{
		ctx:       context.Background(),
		outbounds: initial,
		history:   urltest.NewHistoryStorage(),
	}
	group.outboundsSnapshot.Store(&initial)
	group.setSelectedTCPLx(nodeA1)

	start := make(chan struct{})
	var wg sync.WaitGroup
	// Reader 1: pickForDialNode
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 5000; i++ {
			_ = group.pickForDialNode(N.NetworkTCP)
		}
	}()
	// Reader 2: performSelectionUpdate concurrent with updateOutbounds (R1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 5000; i++ {
			group.performSelectionUpdate(false)
		}
	}()
	close(start)
	for i := 0; i < 5000; i++ {
		if i%2 == 0 {
			group.updateOutbounds([]adapter.Outbound{nodeA2})
		} else {
			group.updateOutbounds([]adapter.Outbound{nodeA1})
		}
	}
	wg.Wait()
}
