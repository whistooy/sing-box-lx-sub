package provider

// lx: sub — subscription parser for Clash and sing-box format

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"

	"github.com/sagernet/sing-box/adapter"
	E "github.com/sagernet/sing/common/exceptions"
)

var dummyNodeRegex = regexp.MustCompile(`(?i)剩余流量|过期时间|到期时间|重置时间|套餐到期|官方网站|官网|客服|群组|公告|Notice|Traffic|Expire|Reset|g00gle\.com`)

// isIgnoredNodeType returns true for meta groups or non-proxy outbound types.
func isIgnoredNodeType(t string) bool {
	switch strings.ToLower(t) {
	case "selector", "urltest", "direct", "block", "dns", "pass":
		return true
	default:
		return false
	}
}

// isDummyNode returns true if the tag or server indicates an announcement/traffic placeholder.
func isDummyNode(tag, server string) bool {
	return dummyNodeRegex.MatchString(tag) || dummyNodeRegex.MatchString(server)
}

// stripLeadingComments removes leading '#' and '//' comment lines (metadata
// headers such as "# upload=...; download=..."), so base64 / json / text
// parsers operate directly on payload bodies.
func stripLeadingComments(content []byte) []byte {
	for len(content) > 0 {
		idx := bytes.IndexByte(content, '\n')
		line := content
		if idx != -1 {
			line = content[:idx]
		}
		trimmed := bytes.TrimSpace(line)
		if !bytes.HasPrefix(trimmed, []byte("#")) && !bytes.HasPrefix(trimmed, []byte("//")) {
			break
		}
		if idx == -1 {
			return nil
		}
		content = content[idx+1:]
	}
	return bytes.TrimSpace(content)
}

// parseNodes normalizes a provider JSON body into a list of sing-box outbound dictionaries.
func parseNodes(content []byte) ([]map[string]any, error) {
	cleaned := stripLeadingComments(content)
	if len(cleaned) == 0 {
		return nil, E.New("empty provider content")
	}

	// Try base64 decoding if the body does not start with JSON characters
	if !bytes.HasPrefix(cleaned, []byte("{")) && !bytes.HasPrefix(cleaned, []byte("[")) {
		if decoded, err := base64.StdEncoding.DecodeString(string(cleaned)); err == nil && len(decoded) > 0 {
			cleaned = stripLeadingComments(decoded)
		} else if decoded, err := base64.RawStdEncoding.DecodeString(string(cleaned)); err == nil && len(decoded) > 0 {
			cleaned = stripLeadingComments(decoded)
		}
	}

	var rawList []map[string]any

	// 1. Try JSON array: [ {"type": "vless", ...}, ... ]
	if err := json.Unmarshal(cleaned, &rawList); err != nil {
		// 2. Try JSON object: { "outbounds": [ ... ] }
		var wrapper struct {
			Outbounds []map[string]any `json:"outbounds"`
		}
		if errWrap := json.Unmarshal(cleaned, &wrapper); errWrap == nil && len(wrapper.Outbounds) > 0 {
			rawList = wrapper.Outbounds
		} else {
			return nil, E.Cause(err, "provider body is not valid sing-box JSON outbounds")
		}
	}

	nodes := make([]map[string]any, 0, len(rawList))
	for _, outbound := range rawList {
		if outbound == nil {
			continue
		}
		nodeType, _ := outbound["type"].(string)
		if nodeType == "" || isIgnoredNodeType(nodeType) {
			continue
		}
		tag, _ := outbound["tag"].(string)
		server, _ := outbound["server"].(string)
		if isDummyNode(tag, server) {
			continue
		}
		nodes = append(nodes, outbound)
	}

	if len(nodes) == 0 {
		return nil, E.New("no valid outbound nodes parsed from provider JSON")
	}
	return nodes, nil
}

// parseUserInfo extracts upload/download/total/expire (in bytes) from the
// Subscription-Userinfo header or the leading "# ..." body comment.
func parseUserInfo(body []byte, header string) (adapter.SubscriptionInfo, bool) {
	raw := header
	if raw == "" {
		rem := body
		for len(rem) > 0 {
			var line []byte
			idx := bytes.IndexByte(rem, '\n')
			if idx != -1 {
				line = rem[:idx]
			} else {
				line = rem
				rem = nil
			}
			line = bytes.TrimSpace(line)
			if len(line) == 0 {
				continue
			}
			if bytes.HasPrefix(line, []byte("#")) {
				raw = string(line)
			}
			break
		}
	}
	if raw == "" {
		return adapter.SubscriptionInfo{}, false
	}
	var info adapter.SubscriptionInfo
	found := false
	for _, part := range strings.FieldsFunc(raw, func(r rune) bool {
		return r == ';' || r == ',' || r == '#' || r == '/' || r == '\n' || r == ' '
	}) {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "upload":
			info.Upload, found = n, true
		case "download":
			info.Download, found = n, true
		case "total":
			info.Total, found = n, true
		case "expire":
			info.Expire, found = n, true
		}
	}
	return info, found
}

func sanitizeTag(tag string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, tag)
}
