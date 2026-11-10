package clashapi

// lx: sub — Clash proxy provider handler implementations

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/render"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/protocol/group"
)

func getProviders(server *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if server.provider == nil {
			render.JSON(w, r, render.M{
				"providers": render.M{},
			})
			return
		}
		providerMap := make(render.M)
		for _, p := range server.provider.Providers() {
			providerMap[p.Tag()] = providerInfo(p)
		}
		render.JSON(w, r, render.M{"providers": providerMap})
	}
}

func getProvider(w http.ResponseWriter, r *http.Request) {
	provider := r.Context().Value(CtxKeyProvider).(adapter.Provider)
	render.JSON(w, r, providerInfo(provider))
}

func updateProvider(w http.ResponseWriter, r *http.Request) {
	provider := r.Context().Value(CtxKeyProvider).(adapter.Provider)
	if err := provider.Update(); err != nil {
		render.Status(r, http.StatusServiceUnavailable)
		render.JSON(w, r, newError(err.Error()))
		return
	}
	render.NoContent(w, r)
}

func healthCheckProvider(server *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		provider := r.Context().Value(CtxKeyProvider).(adapter.Provider)
		query := r.URL.Query()
		url := query.Get("url")
		timeout := 5000
		if t, err := strconv.Atoi(query.Get("timeout")); err == nil && t > 0 {
			timeout = t
		}
		outbounds := provider.Outbounds()
		if len(outbounds) > 0 {
			baseCtx := server.ctx
			if baseCtx == nil {
				baseCtx = r.Context()
			}
			ctx, cancel := context.WithTimeout(baseCtx, time.Millisecond*time.Duration(timeout))
			go func() {
				defer cancel()
				group.URLTestOutbounds(ctx, server.outbound, server.urlTestHistory, server.logger, outbounds, url, 0, true)
			}()
		}
		render.NoContent(w, r)
	}
}

func findProviderByName(server *Server) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			name, _ := r.Context().Value(CtxKeyProviderName).(string)
			if server.provider == nil {
				render.Status(r, http.StatusNotFound)
				render.JSON(w, r, ErrNotFound)
				return
			}
			provider, exist := server.provider.Get(name)
			if !exist {
				render.Status(r, http.StatusNotFound)
				render.JSON(w, r, ErrNotFound)
				return
			}
			ctx := context.WithValue(r.Context(), CtxKeyProvider, provider)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
