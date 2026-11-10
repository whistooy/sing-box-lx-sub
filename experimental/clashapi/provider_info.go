package clashapi

// lx: sub — Clash API provider info representation

import (
	"context"
	"net/http"

	"github.com/go-chi/render"
	"github.com/sagernet/sing-box/adapter"
)

func getProviderProxyDelay(server *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		proxyName := getEscapeParam(r, "proxyName")
		detour, loaded := server.outbound.Outbound(proxyName)
		if !loaded {
			providerName, _ := r.Context().Value(CtxKeyProviderName).(string)
			if providerName != "" {
				detour, loaded = server.outbound.Outbound(providerName + "/" + proxyName)
			}
		}
		if !loaded {
			render.Status(r, http.StatusNotFound)
			render.JSON(w, r, ErrNotFound)
			return
		}
		getProxyDelay(server)(w, r.WithContext(context.WithValue(r.Context(), CtxKeyProxy, detour)))
	}
}

func providerInfo(p adapter.Provider) map[string]any {
	proxies := make([]map[string]any, 0)
	for _, detour := range p.Outbounds() {
		proxies = append(proxies, map[string]any{
			"name":    detour.Tag(),
			"type":    proxyTypeName(detour.Type()),
			"udp":     true,
			"history": []any{},
		})
	}
	info := map[string]any{
		"type":        "Proxy",
		"vehicleType": providerVehicle(p.Type()),
		"name":        p.Tag(),
		"proxies":     proxies,
		"updatedAt":   p.UpdatedAt(),
	}
	if sub, ok := p.(adapter.ProviderSubscriptionInfo); ok {
		info["subscriptionInfo"] = sub.SubscriptionInfo()
	}
	return info
}

func providerVehicle(providerType string) string {
	switch providerType {
	case "remote":
		return "HTTP"
	case "local":
		return "File"
	default:
		return "Compatible"
	}
}

func proxyTypeName(outboundType string) string {
	switch outboundType {
	case "socks":
		return "Socks5"
	case "http":
		return "Http"
	case "vless":
		return "Vless"
	case "vmess":
		return "Vmess"
	case "trojan":
		return "Trojan"
	case "shadowsocks":
		return "Shadowsocks"
	default:
		return outboundType
	}
}
