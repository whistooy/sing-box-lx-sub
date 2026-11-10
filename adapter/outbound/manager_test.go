package outbound

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/stretchr/testify/require"
)

type dummyOutbound struct {
	adapter.Outbound
	tag     string
	closed  bool
	started bool
}

func (d *dummyOutbound) Type() string           { return C.TypeDirect }
func (d *dummyOutbound) Tag() string            { return d.tag }
func (d *dummyOutbound) Network() []string      { return []string{"tcp", "udp"} }
func (d *dummyOutbound) Dependencies() []string { return nil }
func (d *dummyOutbound) Close() error           { d.closed = true; return nil }
func (d *dummyOutbound) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	d.started = true
	return nil
}

type dummyRegistry struct {
	adapter.OutboundRegistry
	creator func(tag string, obType string) (adapter.Outbound, error)
}

func (r *dummyRegistry) CreateOutbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, outboundType string, options any) (adapter.Outbound, error) {
	if r.creator != nil {
		return r.creator(tag, outboundType)
	}
	return &dummyOutbound{tag: tag}, nil
}

func TestManagerDefaultOutboundFallbackExemption(t *testing.T) {
	reg := &dummyRegistry{}
	mgr := NewManager(reg, nil, "")
	ctx := context.Background()
	logger := log.NewNOPFactory().NewLogger("")

	// 1. Create Helper-Direct before any other outbounds
	err := mgr.Create(ctx, nil, logger, adapter.ProviderFallbackTag, C.TypeDirect, nil)
	require.NoError(t, err)

	// Verify Helper-Direct did NOT hijack Default()
	require.Nil(t, mgr.Default())

	// 2. Create the first normal outbound
	err = mgr.Create(ctx, nil, logger, "normal-outbound", C.TypeDirect, nil)
	require.NoError(t, err)

	// Verify the normal outbound is now the default
	require.NotNil(t, mgr.Default())
	require.Equal(t, "normal-outbound", mgr.Default().Tag())
}

func TestManagerReplace(t *testing.T) {
	reg := &dummyRegistry{}
	mgr := NewManager(reg, nil, "")
	ctx := context.Background()
	logger := log.NewNOPFactory().NewLogger("")

	firstNode := &dummyOutbound{tag: "node1"}
	reg.creator = func(tag string, obType string) (adapter.Outbound, error) {
		return firstNode, nil
	}

	err := mgr.Create(ctx, nil, logger, "node1", C.TypeDirect, nil)
	require.NoError(t, err)
	require.Len(t, mgr.Outbounds(), 1)
	require.Equal(t, firstNode, mgr.Outbounds()[0])

	// Replace with second node
	secondNode := &dummyOutbound{tag: "node1"}
	reg.creator = func(tag string, obType string) (adapter.Outbound, error) {
		return secondNode, nil
	}

	err = mgr.Replace(ctx, nil, logger, "node1", C.TypeDirect, nil)
	require.NoError(t, err)
	require.Len(t, mgr.Outbounds(), 1)
	require.Equal(t, secondNode, mgr.Outbounds()[0])
	require.True(t, firstNode.closed)

	// Remove second node
	err = mgr.Remove("node1")
	require.NoError(t, err)
	require.Empty(t, mgr.Outbounds())
	require.True(t, secondNode.closed)
}

func TestManagerProviderNodeDoesNotHijackDefault(t *testing.T) {
	reg := &dummyRegistry{}
	mgr := NewManager(reg, nil, "")
	ctx := context.Background()
	logger := log.NewNOPFactory().NewLogger("")

	// Create a provider node via Replace before any normal outbounds
	err := mgr.Replace(ctx, nil, logger, "provider/node-1", C.TypeDirect, nil)
	require.NoError(t, err)

	// Provider node should NOT be default
	require.Nil(t, mgr.Default())

	// Create normal outbound
	err = mgr.Create(ctx, nil, logger, "direct", C.TypeDirect, nil)
	require.NoError(t, err)

	// Normal outbound becomes default
	require.NotNil(t, mgr.Default())
	require.Equal(t, "direct", mgr.Default().Tag())
}

func TestManagerSlashTagCanBeDefault(t *testing.T) {
	reg := &dummyRegistry{}
	mgr := NewManager(reg, nil, "")
	ctx := context.Background()
	logger := log.NewNOPFactory().NewLogger("")

	// Static outbound with a slash in tag MUST become default when defaultTag is empty
	err := mgr.Create(ctx, nil, logger, "node/1", C.TypeDirect, nil)
	require.NoError(t, err)
	require.NotNil(t, mgr.Default())
	require.Equal(t, "node/1", mgr.Default().Tag())
}

func TestManagerStartupDynamicNodeScopeCleanup(t *testing.T) {
	reg := &dummyRegistry{}
	mgr := NewManager(reg, nil, "")
	ctx := context.Background()
	logger := log.NewNOPFactory().NewLogger("")

	node1Cleaned := false
	node1 := &dummyOutbound{tag: "sub/node-1"}
	reg.creator = func(tag string, obType string) (adapter.Outbound, error) {
		return node1, nil
	}

	// 1. Create dynamic provider node at startup (stage 0) via Replace
	err := mgr.Replace(ctx, nil, logger, "sub/node-1", C.TypeDirect, nil)
	require.NoError(t, err)

	// 2. Start manager
	parentScope := adapter.NewScope(ctx, logger)
	defer parentScope.Close()

	err = mgr.Start(adapter.StartStateStart, parentScope)
	require.NoError(t, err)
	err = mgr.Start(adapter.StartStateStarted, parentScope)
	require.NoError(t, err)

	// Add a cleanup to node1's dynamic scope to verify it gets closed
	scope := mgr.dynamicScopes["sub/node-1"]
	require.NotNil(t, scope)
	scope.Add(func() error {
		node1Cleaned = true
		return nil
	})

	// 3. Replace node1 at runtime
	node2 := &dummyOutbound{tag: "sub/node-1"}
	reg.creator = func(tag string, obType string) (adapter.Outbound, error) {
		return node2, nil
	}

	err = mgr.Replace(ctx, nil, logger, "sub/node-1", C.TypeDirect, nil)
	require.NoError(t, err)

	// Verify node1's scope was closed and executed its cleanup
	require.True(t, node1Cleaned)
	require.True(t, node1.closed)
}

func TestManagerReplaceAfterClose(t *testing.T) {
	reg := &dummyRegistry{}
	mgr := NewManager(reg, nil, "")
	ctx := context.Background()
	logger := log.NewNOPFactory().NewLogger("")

	parentScope := adapter.NewScope(ctx, logger)
	err := mgr.Start(adapter.StartStateStart, parentScope)
	require.NoError(t, err)
	err = mgr.Start(adapter.StartStateStarted, parentScope)
	require.NoError(t, err)

	// Close dynamic scopes (simulates shutdown)
	err = mgr.closeDynamicScopes()
	require.NoError(t, err)

	created := false
	node := &dummyOutbound{tag: "sub/node-1"}
	reg.creator = func(tag string, obType string) (adapter.Outbound, error) {
		created = true
		return node, nil
	}

	// Replace should be rejected immediately before creation
	err = mgr.Replace(ctx, nil, logger, "sub/node-1", C.TypeDirect, nil)
	require.ErrorIs(t, err, os.ErrClosed)
	require.False(t, created)

	// Create should also be rejected and close created outbound
	nodeCreate := &dummyOutbound{tag: "sub/node-2"}
	reg.creator = func(tag string, obType string) (adapter.Outbound, error) {
		return nodeCreate, nil
	}
	err = mgr.Create(ctx, nil, logger, "sub/node-2", C.TypeDirect, nil)
	require.ErrorIs(t, err, os.ErrClosed)
	require.True(t, nodeCreate.closed)
}

func TestManagerTargetScopeNilParent(t *testing.T) {
	reg := &dummyRegistry{}
	mgr := NewManager(reg, nil, "")
	mgr.RegisterDynamicTag("sub/node-1")

	// targetScope with nil defaultScope should not panic
	scope := mgr.targetScope(nil, "sub/node-1")
	require.NotNil(t, scope)
	require.NotNil(t, scope.Context())
	_ = scope.Close()
}

func TestManagerStageRace(t *testing.T) {
	for iteration := 0; iteration < 50; iteration++ {
		reg := &dummyRegistry{}
		mgr := NewManager(reg, nil, "")
		ctx := context.Background()
		logger := log.NewNOPFactory().NewLogger("")
		reg.creator = func(tag string, obType string) (adapter.Outbound, error) {
			return &dummyOutbound{tag: tag}, nil
		}
		if err := mgr.Create(ctx, nil, logger, "sub/node", C.TypeDirect, nil); err != nil {
			t.Fatal(err)
		}
		scope := adapter.NewScope(ctx, logger)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = mgr.Start(adapter.StartStateStart, scope)
		}()
		go func() {
			defer wg.Done()
			_ = mgr.Replace(ctx, nil, logger, "sub/node", C.TypeDirect, nil)
		}()
		wg.Wait()
		_ = scope.Close()
	}
}

func TestOutboundsSnapshotAliasRace(t *testing.T) {
	reg := &dummyRegistry{}
	mgr := NewManager(reg, nil, "")
	ctx := context.Background()
	logger := log.NewNOPFactory().NewLogger("")

	reg.creator = func(tag string, obType string) (adapter.Outbound, error) {
		return &dummyOutbound{tag: tag}, nil
	}
	if err := mgr.Create(ctx, nil, logger, "sub/node", C.TypeDirect, nil); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 5000; i++ {
			for _, outbound := range mgr.Outbounds() {
				_ = outbound.Tag()
			}
		}
	}()
	close(start)
	for i := 0; i < 500; i++ {
		if err := mgr.Replace(ctx, nil, logger, "sub/node", C.TypeDirect, nil); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}
