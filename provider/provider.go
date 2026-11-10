// Package provider implements built-in config providers (remote/local). It
// reuses the core outbound manager to materialize provider nodes as regular
// outbounds, then exposes them to groups and the Clash API.
//
// lx: sub — built-in config providers implementation
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	provideradapter "github.com/sagernet/sing-box/adapter/provider"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	singjson "github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/filemanager"
)

func RegisterRemote(registry *provideradapter.Registry) {
	provideradapter.Register[option.ProviderRemoteOptions](registry, "remote",
		func(ctx context.Context, router adapter.Router, logFactory log.Factory, tag string, options option.ProviderRemoteOptions) (adapter.Provider, error) {
			p := newProvider(ctx, router, logFactory, tag, "remote", options.URL, false, options.Path)
			p.ua = options.UserAgent
			p.interval = time.Duration(options.UpdateInterval)
			var err error
			if options.Include != "" {
				p.include, err = regexp.Compile(options.Include)
				if err != nil {
					return nil, E.Cause(err, "invalid include regex")
				}
			}
			if options.Exclude != "" {
				p.exclude, err = regexp.Compile(options.Exclude)
				if err != nil {
					return nil, E.Cause(err, "invalid exclude regex")
				}
			}
			if options.HTTPClient != nil {
				p.httpOptions = *options.HTTPClient
			}
			if options.Detour != "" && p.httpOptions.Detour == "" {
				p.httpOptions.Detour = options.Detour
			}
			if err := p.load(false); err != nil {
				p.logger.Warn("initial provider load failed (will retry on refresh): ", err)
			}
			p.startTicker()
			return p, nil
		})
}

func RegisterLocal(registry *provideradapter.Registry) {
	provideradapter.Register[option.ProviderLocalOptions](registry, "local",
		func(ctx context.Context, router adapter.Router, logFactory log.Factory, tag string, options option.ProviderLocalOptions) (adapter.Provider, error) {
			p := newProvider(ctx, router, logFactory, tag, "local", options.Path, true, "")
			var err error
			if options.Include != "" {
				p.include, err = regexp.Compile(options.Include)
				if err != nil {
					return nil, E.Cause(err, "invalid include regex")
				}
			}
			if options.Exclude != "" {
				p.exclude, err = regexp.Compile(options.Exclude)
				if err != nil {
					return nil, E.Cause(err, "invalid exclude regex")
				}
			}
			if err := p.load(false); err != nil {
				p.logger.Warn("initial provider read failed: ", err)
			}
			return p, nil
		})
}

// Context registers provider options and adapter registries into the context.
func Context(ctx context.Context) context.Context {
	registry := provideradapter.NewRegistry()
	RegisterLocal(registry)
	RegisterRemote(registry)
	ctx = service.ContextWith[option.ProviderOptionsRegistry](ctx, registry)
	ctx = service.ContextWith[adapter.ProviderRegistry](ctx, registry)
	return ctx
}

// Install builds the provider manager, registers it in the context (so groups
// and the Clash API can resolve providers), and creates the configured
// providers. It is called by box.New after the outbound manager exists so
// provider nodes can be materialized as outbounds before the config
// outbounds/groups are built.
func Install(ctx context.Context, router adapter.Router, logFactory log.Factory, providers []option.Provider) (err error) {
	registry := service.FromContext[adapter.ProviderRegistry](ctx)
	if registry == nil {
		builtin := provideradapter.NewRegistry()
		RegisterLocal(builtin)
		RegisterRemote(builtin)
		registry = builtin
		ctx = service.ContextWith[adapter.ProviderRegistry](ctx, registry)
		ctx = service.ContextWith[option.ProviderOptionsRegistry](ctx, registry)
	}
	providerCtx, cancelProviders := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			cancelProviders()
		}
	}()
	context.AfterFunc(ctx, cancelProviders)
	if om := service.FromContext[adapter.OutboundManager](ctx); om != nil {
		if cleaner, ok := om.(interface{ RegisterProviderCleanup(func() error) }); ok {
			cleaner.RegisterProviderCleanup(func() error {
				cancelProviders()
				return nil
			})
		}
	}
	manager := provideradapter.NewManager(providerCtx, logFactory.NewLogger("provider"), registry)
	service.MustRegister[adapter.ProviderManager](ctx, manager)
	if len(providers) > 0 {
		if om := service.FromContext[adapter.OutboundManager](ctx); om != nil {
			if _, loaded := om.Outbound(adapter.ProviderFallbackTag); !loaded {
				if registrar, ok := om.(dynamicTagRegistrar); ok {
					registrar.RegisterDynamicTag(adapter.ProviderFallbackTag)
				}
				if err := om.Create(ctx, router, logFactory.NewLogger("provider"), adapter.ProviderFallbackTag, C.TypeDirect, &option.DirectOutboundOptions{}); err != nil {
					return E.Cause(err, "install provider fallback outbound")
				}
			}
		}
	}
	return Setup(providerCtx, router, logFactory, manager, providers)
}

// Setup creates every configured provider on the manager.
func Setup(ctx context.Context, router adapter.Router, logFactory log.Factory, manager adapter.ProviderManager, providers []option.Provider) error {
	for _, providerOptions := range providers {
		if providerOptions.Tag == "" {
			return E.New("provider missing tag")
		}
		if err := manager.Create(ctx, router, logFactory, providerOptions.Tag, providerOptions.Type, providerOptions.Options); err != nil {
			return E.Cause(err, "initialize provider[", providerOptions.Tag, "] ", providerOptions.Type)
		}
	}
	return nil
}

type builtinProvider struct {
	ctx            context.Context
	router         adapter.Router
	logger         log.ContextLogger
	outbound       adapter.OutboundManager
	tag            string
	ptype          string
	source         string
	isFile         bool
	cachePath      string
	ua             string
	interval       time.Duration
	include        *regexp.Regexp
	exclude        *regexp.Regexp
	httpOptions    option.HTTPClientOptions
	loadLock       sync.Mutex
	access         sync.RWMutex
	outbounds      []adapter.Outbound
	byTag          map[string]adapter.Outbound
	appliedOptions map[string][]byte
	updatedAt      time.Time
	subInfo        adapter.SubscriptionInfo
}

type dynamicTagRegistrar interface {
	RegisterDynamicTag(tag string)
}

type outboundRemover interface {
	Remove(tag string) error
}

type outboundReplacer interface {
	Replace(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, outboundType string, options any) error
}

func defaultCachePath(ctx context.Context, tag string) string {
	fileName := "sing-box-provider-" + sanitizeTag(tag) + ".cache"
	if tmpDir := os.Getenv("TMPDIR"); tmpDir != "" {
		return filepath.Join(tmpDir, fileName)
	}
	return filemanager.BasePath(ctx, fileName)
}

func newProvider(ctx context.Context, router adapter.Router, logFactory log.Factory, tag, ptype, source string, isFile bool, cachePath string) *builtinProvider {
	if !isFile {
		if cachePath != "" {
			cachePath = filemanager.BasePath(ctx, cachePath)
		} else {
			cachePath = defaultCachePath(ctx, tag)
		}
	} else if cachePath != "" {
		cachePath = filemanager.BasePath(ctx, cachePath)
	}
	if isFile {
		source = filemanager.BasePath(ctx, source)
	}
	return &builtinProvider{
		ctx:            ctx,
		router:         router,
		logger:         logFactory.NewLogger("provider/" + tag),
		outbound:       service.FromContext[adapter.OutboundManager](ctx),
		tag:            tag,
		ptype:          ptype,
		source:         source,
		isFile:         isFile,
		cachePath:      cachePath,
		byTag:          make(map[string]adapter.Outbound),
		appliedOptions: make(map[string][]byte),
	}
}

func (p *builtinProvider) Type() string { return p.ptype }
func (p *builtinProvider) Tag() string  { return p.tag }

func (p *builtinProvider) Outbounds() []adapter.Outbound {
	p.access.RLock()
	defer p.access.RUnlock()
	return append([]adapter.Outbound(nil), p.outbounds...)
}

func (p *builtinProvider) Outbound(tag string) (adapter.Outbound, bool) {
	p.access.RLock()
	defer p.access.RUnlock()
	o, ok := p.byTag[tag]
	return o, ok
}

func (p *builtinProvider) UpdatedAt() time.Time {
	p.access.RLock()
	defer p.access.RUnlock()
	return p.updatedAt
}

func (p *builtinProvider) SubscriptionInfo() adapter.SubscriptionInfo {
	p.access.RLock()
	defer p.access.RUnlock()
	return p.subInfo
}

func (p *builtinProvider) Update() error { return p.load(true) }

// startTicker re-fetches the provider on its configured interval until the
// owning instance context is cancelled (reload/shutdown).
func (p *builtinProvider) startTicker() {
	if p.interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()
		for {
			select {
			case <-p.ctx.Done():
				return
			case <-ticker.C:
				if err := p.load(true); err != nil {
					p.logger.Warn("provider refresh failed: ", err)
				}
			}
		}
	}()
}

func (p *builtinProvider) read(forceRemote bool) (body []byte, userInfo string, err error) {
	if p.isFile {
		body, err = os.ReadFile(p.source)
		return body, "", err
	}
	// Fast path on startup: if cache already exists, load it immediately
	// to avoid blocking core startup with network HTTP roundtrips.
	if !forceRemote && p.cachePath != "" {
		if cached, cerr := os.ReadFile(p.cachePath); cerr == nil && len(cached) > 0 {
			return cached, "", nil
		}
	}
	body, userInfo, err = p.fetchRemote()
	if err == nil {
		if p.cachePath != "" {
			cacheData := body
			if userInfo != "" && !bytes.HasPrefix(bytes.TrimSpace(body), []byte("#")) {
				cacheData = append([]byte("# "+userInfo+"\n"), body...)
			}
			dir := filepath.Dir(p.cachePath)
			if mkErr := os.MkdirAll(dir, 0o755); mkErr == nil {
				tmpPath := p.cachePath + ".tmp"
				if werr := os.WriteFile(tmpPath, cacheData, 0o600); werr == nil {
					_ = os.Rename(tmpPath, p.cachePath)
				} else {
					p.logger.Warn("write provider cache: ", werr)
				}
			} else {
				p.logger.Warn("create provider cache dir: ", mkErr)
			}
		}
		return body, userInfo, nil
	}
	if forceRemote {
		return nil, "", err
	}
	// Fall back to the cached copy on initial startup so a transient feed
	// outage does not empty the provider.
	if p.cachePath != "" {
		if cached, cerr := os.ReadFile(p.cachePath); cerr == nil && len(cached) > 0 {
			p.logger.Warn("provider fetch failed, using cache: ", err)
			return cached, "", nil
		}
	}
	return nil, "", err
}

func (p *builtinProvider) fetchRemote() ([]byte, string, error) {
	req, err := http.NewRequestWithContext(p.ctx, http.MethodGet, p.source, nil)
	if err != nil {
		return nil, "", err
	}
	userAgent := p.ua
	if userAgent == "" {
		userAgent = "sing-box/" + C.Version
	}
	req.Header.Set("User-Agent", userAgent)

	client := &http.Client{Timeout: 30 * time.Second}
	httpClientManager := service.FromContext[adapter.HTTPClientManager](p.ctx)
	if httpClientManager != nil {
		transport, err := httpClientManager.ResolveTransport(p.ctx, p.logger, p.httpOptions)
		if err != nil {
			return nil, "", E.Cause(err, "resolve transport")
		}
		client.Transport = transport
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, "", E.New("provider fetch status ", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, "", err
	}
	return body, resp.Header.Get("Subscription-Userinfo"), nil
}

func (p *builtinProvider) load(forceRemote bool) error {
	p.loadLock.Lock()
	defer p.loadLock.Unlock()

	content, userInfoHeader, err := p.read(forceRemote)
	if err != nil {
		return err
	}
	nodes, err := parseNodes(content)
	if err != nil {
		return E.Cause(err, "provider ", p.tag)
	}
	subInfo, hasSubInfo := parseUserInfo(content, userInfoHeader)

	p.access.RLock()
	oldTags := make([]string, 0, len(p.byTag))
	for tag := range p.byTag {
		oldTags = append(oldTags, tag)
	}
	p.access.RUnlock()

	var newOutbounds []adapter.Outbound
	newByTag := make(map[string]adapter.Outbound)
	for _, node := range nodes {
		nodeTag, _ := node["tag"].(string)
		if p.include != nil && !p.include.MatchString(nodeTag) {
			continue
		}
		if p.exclude != nil && p.exclude.MatchString(nodeTag) {
			continue
		}
		raw, err := json.Marshal(node)
		if err != nil {
			continue
		}
		var ob option.Outbound
		if err := singjson.UnmarshalContext(p.ctx, raw, &ob); err != nil {
			p.logger.Warn("skip provider node (unmarshal): ", err)
			continue
		}
		if ob.Tag == "" || ob.Type == "" || isIgnoredNodeType(ob.Type) {
			continue
		}
		fullTag := p.tag + "/" + ob.Tag
		if registrar, ok := p.outbound.(dynamicTagRegistrar); ok {
			registrar.RegisterDynamicTag(fullTag)
		}
		prevRaw, hadPrev := p.appliedOptions[fullTag]
		if hadPrev && bytes.Equal(prevRaw, raw) {
			if o, loaded := p.outbound.Outbound(fullTag); loaded {
				newOutbounds = append(newOutbounds, o)
				newByTag[fullTag] = o
				continue
			}
		}

		var createErr error
		if replacer, ok := p.outbound.(outboundReplacer); ok {
			createErr = replacer.Replace(p.ctx, p.router, p.logger, fullTag, ob.Type, ob.Options)
		} else {
			if hadPrev {
				if remover, ok := p.outbound.(outboundRemover); ok {
					_ = remover.Remove(fullTag)
				}
			}
			createErr = p.outbound.Create(p.ctx, p.router, p.logger, fullTag, ob.Type, ob.Options)
		}
		if createErr != nil {
			p.logger.Warn("skip provider node ", ob.Tag, ": ", createErr)
			// Retain existing working node if replacement failed
			if hadPrev {
				if o, loaded := p.outbound.Outbound(fullTag); loaded {
					newOutbounds = append(newOutbounds, o)
					newByTag[fullTag] = o
				}
			}
			continue
		}
		if o, ok := p.outbound.Outbound(fullTag); ok {
			newOutbounds = append(newOutbounds, o)
			newByTag[fullTag] = o
			p.appliedOptions[fullTag] = raw
		}
	}

	// Remove vanished nodes from OutboundManager
	if remover, ok := p.outbound.(outboundRemover); ok {
		for _, oldTag := range oldTags {
			if _, stillPresent := newByTag[oldTag]; !stillPresent {
				_ = remover.Remove(oldTag)
				delete(p.appliedOptions, oldTag)
			}
		}
	}

	updatedAt := time.Now()
	if p.isFile {
		if fi, err := os.Stat(p.source); err == nil {
			updatedAt = fi.ModTime()
		}
	} else if !forceRemote && p.cachePath != "" {
		if fi, err := os.Stat(p.cachePath); err == nil {
			updatedAt = fi.ModTime()
		}
	}

	p.access.Lock()
	p.outbounds = newOutbounds
	p.byTag = newByTag
	p.updatedAt = updatedAt
	if hasSubInfo {
		p.subInfo = subInfo
	}
	p.access.Unlock()

	manager := service.FromContext[adapter.ProviderManager](p.ctx)
	if manager != nil {
		manager.NotifyUpdated(p.tag)
	}
	return nil
}
