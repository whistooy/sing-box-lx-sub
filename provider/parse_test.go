package provider

import (
	"encoding/base64"
	"testing"
)

func TestStripLeadingComments(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "hash comment",
			input:    "# upload=100; download=200\n[{\"type\":\"vmess\"}]",
			expected: "[{\"type\":\"vmess\"}]",
		},
		{
			name:     "slash comment",
			input:    "// clash config\n[{\"type\":\"vless\"}]",
			expected: "[{\"type\":\"vless\"}]",
		},
		{
			name:     "multiple comments and whitespace",
			input:    "  # first comment\n  // second comment\n\n[{\"type\":\"shadowsocks\"}]",
			expected: "[{\"type\":\"shadowsocks\"}]",
		},
		{
			name:     "no comments",
			input:    "[{\"type\":\"trojan\"}]",
			expected: "[{\"type\":\"trojan\"}]",
		},
		{
			name:     "only comments",
			input:    "# comment 1\n// comment 2\n",
			expected: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := string(stripLeadingComments([]byte(tc.input)))
			if got != tc.expected {
				t.Fatalf("expected %q, got %q", tc.expected, got)
			}
		})
	}
}

func TestParseUserInfo(t *testing.T) {
	t.Run("from header", func(t *testing.T) {
		header := "upload=1024; download=2048; total=1048576; expire=1700000000"
		info, ok := parseUserInfo(nil, header)
		if !ok {
			t.Fatal("expected ok=true")
		}
		if info.Upload != 1024 || info.Download != 2048 || info.Total != 1048576 || info.Expire != 1700000000 {
			t.Fatalf("unexpected info: %+v", info)
		}
	})

	t.Run("from leading comment", func(t *testing.T) {
		body := []byte("# upload=500, download=1500, total=5000\n[{\"type\":\"direct\"}]")
		info, ok := parseUserInfo(body, "")
		if !ok {
			t.Fatal("expected ok=true")
		}
		if info.Upload != 500 || info.Download != 1500 || info.Total != 5000 {
			t.Fatalf("unexpected info: %+v", info)
		}
	})

	t.Run("empty", func(t *testing.T) {
		_, ok := parseUserInfo([]byte("no info here"), "")
		if ok {
			t.Fatal("expected ok=false")
		}
	})
}

func TestIsDummyNode(t *testing.T) {
	dummies := []struct {
		tag    string
		server string
	}{
		{"剩余流量: 100GB", "1.1.1.1"},
		{"套餐到期: 2026-12-31", "1.1.1.1"},
		{"官网：https://example.com", "1.1.1.1"},
		{"Notice: maintenance", "1.1.1.1"},
		{"HK-01", "g00gle.com"},
		{"HK-02", "traffic.reset.com"},
	}

	for _, d := range dummies {
		if !isDummyNode(d.tag, d.server) {
			t.Errorf("expected dummy for tag=%q server=%q", d.tag, d.server)
		}
	}

	reals := []struct {
		tag    string
		server string
	}{
		{"HK-01", "hk01.example.com"},
		{"US-Node", "104.16.1.1"},
		{"JP-Tokyo", "jp.node.net"},
	}

	for _, r := range reals {
		if isDummyNode(r.tag, r.server) {
			t.Errorf("unexpected dummy for tag=%q server=%q", r.tag, r.server)
		}
	}
}

func TestSanitizeTag(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{"HK 01: Fast / Stable", "HK_01__Fast___Stable"},
		{"Node-1_US", "Node-1_US"},
		{"A+B=C", "A_B_C"},
	}

	for _, tc := range cases {
		got := sanitizeTag(tc.input)
		if got != tc.expected {
			t.Errorf("sanitizeTag(%q) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

func TestParseNodes(t *testing.T) {
	t.Run("json array with dummy and valid nodes", func(t *testing.T) {
		body := []byte(`[
			{"type": "shadowsocks", "tag": "HK-Fast", "server": "1.2.3.4", "server_port": 8388, "method": "aes-128-gcm", "password": "pass"},
			{"type": "direct", "tag": "direct-node"},
			{"type": "shadowsocks", "tag": "剩余流量 50GB", "server": "1.2.3.4", "server_port": 8388, "method": "aes-128-gcm", "password": "pass"}
		]`)
		nodes, err := parseNodes(body)
		if err != nil {
			t.Fatalf("parseNodes failed: %v", err)
		}
		if len(nodes) != 1 {
			t.Fatalf("expected 1 node, got %d", len(nodes))
		}
		if nodes[0]["tag"] != "HK-Fast" {
			t.Fatalf("expected HK-Fast, got %v", nodes[0]["tag"])
		}
	})

	t.Run("object wrapper with outbounds field", func(t *testing.T) {
		body := []byte(`{
			"outbounds": [
				{"type": "vless", "tag": "US-Vless", "server": "1.2.3.4", "server_port": 443, "uuid": "00000000-0000-0000-0000-000000000000"},
				{"type": "selector", "tag": "select", "outbounds": ["US-Vless"]}
			]
		}`)
		nodes, err := parseNodes(body)
		if err != nil {
			t.Fatalf("parseNodes failed: %v", err)
		}
		if len(nodes) != 1 {
			t.Fatalf("expected 1 node, got %d", len(nodes))
		}
		if nodes[0]["tag"] != "US-Vless" {
			t.Fatalf("expected US-Vless, got %v", nodes[0]["tag"])
		}
	})

	t.Run("base64 json subscription content", func(t *testing.T) {
		raw := `[{"type": "trojan", "tag": "Node-Trojan", "server": "1.2.3.4", "server_port": 443, "password": "pass"}]`
		encoded := base64.StdEncoding.EncodeToString([]byte(raw))
		nodes, err := parseNodes([]byte(encoded))
		if err != nil {
			t.Fatalf("parseNodes base64 failed: %v", err)
		}
		if len(nodes) != 1 {
			t.Fatalf("expected 1 node, got %d", len(nodes))
		}
		if nodes[0]["tag"] != "Node-Trojan" {
			t.Fatalf("expected Node-Trojan, got %v", nodes[0]["tag"])
		}
	})

	t.Run("empty body returns error", func(t *testing.T) {
		_, err := parseNodes([]byte(""))
		if err == nil {
			t.Fatal("expected error on empty body")
		}
	})
}
