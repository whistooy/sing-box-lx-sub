package clashapi

// lx: sub — clash proxy provider endpoints

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func proxyProviderRouter(server *Server) http.Handler {
	r := chi.NewRouter()
	r.Get("/", getProviders(server))

	r.Route("/{name}", func(r chi.Router) {
		r.Use(parseProviderName, findProviderByName(server))
		r.Get("/", getProvider)
		r.Put("/", updateProvider)
		r.Get("/healthcheck", healthCheckProvider(server))
		r.Get("/{proxyName}/healthcheck", getProviderProxyDelay(server))
	})
	return r
}

func parseProviderName(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := getEscapeParam(r, "name")
		ctx := context.WithValue(r.Context(), CtxKeyProviderName, name)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
