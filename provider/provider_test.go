package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/socks"
	"github.com/sagernet/sing/common/json/badoption"
	"github.com/sagernet/sing/service"
	"github.com/stretchr/testify/require"
)

type mockOutboundManager struct {
	adapter.OutboundManager
	outbounds   map[string]adapter.Outbound
	cleanups    []func() error
	dynamicTags map[string]struct{}
}

func (m *mockOutboundManager) RegisterDynamicTag(tag string) {
	if m.dynamicTags == nil {
		m.dynamicTags = make(map[string]struct{})
	}
	m.dynamicTags[tag] = struct{}{}
}

func (m *mockOutboundManager) RegisterProviderCleanup(cleanup func() error) {
	m.cleanups = append(m.cleanups, cleanup)
}

func (m *mockOutboundManager) Outbounds() []adapter.Outbound {
	res := make([]adapter.Outbound, 0, len(m.outbounds))
	for _, o := range m.outbounds {
		res = append(res, o)
	}
	return res
}

func (m *mockOutboundManager) Outbound(tag string) (adapter.Outbound, bool) {
	o, ok := m.outbounds[tag]
	return o, ok
}

func (m *mockOutboundManager) Default() adapter.Outbound {
	return nil
}

func (m *mockOutboundManager) Create(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, outboundType string, options any) error {
	m.outbounds[tag] = &mockOutbound{tag: tag, obType: outboundType}
	return nil
}

func (m *mockOutboundManager) Replace(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, outboundType string, options any) error {
	m.outbounds[tag] = &mockOutbound{tag: tag, obType: outboundType}
	return nil
}

func (m *mockOutboundManager) Remove(tag string) error {
	delete(m.outbounds, tag)
	return nil
}

type mockOutbound struct {
	adapter.Outbound
	tag    string
	obType string
}

func (m *mockOutbound) Tag() string  { return m.tag }
func (m *mockOutbound) Type() string { return m.obType }

func TestProviderUpdateAndCacheBehavior(t *testing.T) {
	tmpDir := t.TempDir()
	cachePath := filepath.Join(tmpDir, "test.cache")

	var requestCount int32
	var failRequest atomic.Bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		if failRequest.Load() {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Subscription-Userinfo", "upload=1000; download=2000; total=100000; expire=1700000000")
		// Initial response: node1, node2
		if atomic.LoadInt32(&requestCount) == 1 {
			w.Write([]byte(`[
				{"type":"socks","tag":"node1","server":"127.0.0.1","server_port":1080},
				{"type":"socks","tag":"node2","server":"127.0.0.1","server_port":1081}
			]`))
		} else {
			// Updated response: node2, node3 (node1 removed)
			w.Write([]byte(`[
				{"type":"socks","tag":"node2","server":"127.0.0.1","server_port":1081},
				{"type":"socks","tag":"node3","server":"127.0.0.1","server_port":1082}
			]`))
		}
	}))
	defer server.Close()

	registry := outbound.NewRegistry()
	socks.RegisterOutbound(registry)
	ctx := service.ContextWith[option.OutboundOptionsRegistry](context.Background(), registry)

	logFactory := log.NewNOPFactory()
	obMgr := &mockOutboundManager{outbounds: make(map[string]adapter.Outbound)}

	p := newProvider(ctx, nil, logFactory, "sub1", "remote", server.URL, false, cachePath)
	p.outbound = obMgr

	// 1. Initial startup load (forceRemote = false, but cache is empty, so fetches from server)
	err := p.load(false)
	require.NoError(t, err)
	require.Len(t, p.Outbounds(), 2)
	require.Equal(t, "sub1/node1", p.Outbounds()[0].Tag())
	require.Equal(t, "sub1/node2", p.Outbounds()[1].Tag())
	require.FileExists(t, cachePath)

	// Verify cache file has Subscription-Userinfo header stored as comment
	cachedBytes, err := os.ReadFile(cachePath)
	require.NoError(t, err)
	require.Contains(t, string(cachedBytes), "# upload=1000;")

	// 2. Simulate manual update when server returns 500
	failRequest.Store(true)
	err = p.Update()
	require.Error(t, err, "Update should return error when server fails, rather than falling back to cache")
	// Nodes must remain unchanged
	require.Len(t, p.Outbounds(), 2)

	// 3. Successful manual update (server returns updated node list: node1 removed, node3 added)
	failRequest.Store(false)
	updateTimeBefore := time.Now()
	time.Sleep(10 * time.Millisecond) // ensure timestamp advances
	err = p.Update()
	require.NoError(t, err)

	// Verify node1 is removed from OutboundManager and node3 is registered
	require.Len(t, p.Outbounds(), 2)
	require.Equal(t, "sub1/node2", p.Outbounds()[0].Tag())
	require.Equal(t, "sub1/node3", p.Outbounds()[1].Tag())
	_, node1Exists := obMgr.Outbound("sub1/node1")
	require.False(t, node1Exists, "node1 should have been removed from OutboundManager")
	_, node3Exists := obMgr.Outbound("sub1/node3")
	require.True(t, node3Exists, "node3 should exist in OutboundManager")
	require.True(t, p.UpdatedAt().After(updateTimeBefore) || p.UpdatedAt().Equal(updateTimeBefore))

	// 4. Test core reboot simulation with existing cache file
	// Set the cache file modification time to 1 hour ago
	oneHourAgo := time.Now().Add(-1 * time.Hour)
	require.NoError(t, os.Chtimes(cachePath, oneHourAgo, oneHourAgo))

	p2 := newProvider(ctx, nil, logFactory, "sub1", "remote", server.URL, false, cachePath)
	p2.outbound = &mockOutboundManager{outbounds: make(map[string]adapter.Outbound)}
	err = p2.load(false) // reboot with cache
	require.NoError(t, err)
	require.Len(t, p2.Outbounds(), 2)
	// UpdatedAt should reflect the file mod time (1 hour ago), NOT time.Now()
	require.WithinDuration(t, oneHourAgo, p2.UpdatedAt(), 2*time.Second)
}

const sampleNodesJSON = `[{"type": "socks", "tag": "node1", "server": "1.1.1.1", "server_port": 1080}]`

func TestProviderShutdownTickerCancellation(t *testing.T) {
	tempDir := t.TempDir()
	cachePath := filepath.Join(tempDir, "sub.cache")

	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		w.Write([]byte(sampleNodesJSON))
	}))
	defer server.Close()

	ctx := context.Background()
	logFactory := log.NewNOPFactory()
	obMgr := &mockOutboundManager{outbounds: make(map[string]adapter.Outbound)}

	ctx = service.ContextWith[adapter.OutboundManager](ctx, obMgr)

	// Install provider with 20ms update interval
	err := Install(ctx, nil, logFactory, []option.Provider{
		{
			Type: "remote",
			Tag:  "sub1",
			Options: &option.ProviderRemoteOptions{
				URL:            server.URL,
				Path:           cachePath,
				UpdateInterval: badoption.Duration(20 * time.Millisecond),
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, obMgr.cleanups, 1)

	// Wait for ticker to run at least once
	time.Sleep(50 * time.Millisecond)
	countBefore := requestCount.Load()
	require.GreaterOrEqual(t, countBefore, int32(1))

	// Simulate shutdown by triggering registered cleanups
	for _, cleanup := range obMgr.cleanups {
		_ = cleanup()
	}

	// Sleep and ensure NO more requests are fired after shutdown
	time.Sleep(60 * time.Millisecond)
	countAfter := requestCount.Load()
	require.Equal(t, countBefore, countAfter, "ticker should be stopped after shutdown cleanup")
}

func TestProviderAtomicCacheWriteWithDirectories(t *testing.T) {
	tempDir := t.TempDir()
	// Deep nested directory that does not exist yet
	cachePath := filepath.Join(tempDir, "deep", "nested", "providers", "sub.cache")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Subscription-Userinfo", "upload=100; download=200; total=1000")
		w.Write([]byte(sampleNodesJSON))
	}))
	defer server.Close()

	ctx := context.Background()
	logFactory := log.NewNOPFactory()

	p := newProvider(ctx, nil, logFactory, "sub1", "remote", server.URL, false, cachePath)
	p.outbound = &mockOutboundManager{outbounds: make(map[string]adapter.Outbound)}

	body, userInfo, err := p.read(true)
	require.NoError(t, err)
	require.NotEmpty(t, body)
	require.NotEmpty(t, userInfo)

	// Verify file was written to disk despite nested non-existent directory
	cached, err := os.ReadFile(cachePath)
	require.NoError(t, err)
	require.Contains(t, string(cached), "upload=100")
	require.Contains(t, string(cached), "node1")
}

func TestProviderRegistersDynamicTags(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "nodes.json")
	content := `[{"type":"socks","tag":"node1","server":"127.0.0.1","server_port":1080}]`
	err := os.WriteFile(filePath, []byte(content), 0644)
	require.NoError(t, err)

	registry := outbound.NewRegistry()
	socks.RegisterOutbound(registry)
	ctx := service.ContextWith[option.OutboundOptionsRegistry](context.Background(), registry)
	logger := log.NewNOPFactory()

	p := newProvider(ctx, nil, logger, "test-sub", "local", filePath, true, "")
	mockMgr := &mockOutboundManager{outbounds: make(map[string]adapter.Outbound)}
	p.outbound = mockMgr

	err = p.load(false)
	require.NoError(t, err)

	require.Contains(t, mockMgr.dynamicTags, "test-sub/node1")
}
