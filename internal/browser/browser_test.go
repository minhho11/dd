package browser

import (
	"strings"
	"testing"
	"time"
)

func TestParseFlow(t *testing.T) {
	t.Run("empty params yields empty flow", func(t *testing.T) {
		f, err := ParseFlow("")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(f.Steps) != 0 {
			t.Fatalf("want 0 steps, got %d", len(f.Steps))
		}
	})

	t.Run("valid steps", func(t *testing.T) {
		params := `{"steps":[
			{"action":"fill","selector":"#email","value":"{{randEmail}}"},
			{"action":"click","selector":"button[type=submit]"},
			{"action":"assertText","selector":".msg","contains":"Welcome"}
		]}`
		f, err := ParseFlow(params)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(f.Steps) != 3 {
			t.Fatalf("want 3 steps, got %d", len(f.Steps))
		}
		if f.Steps[0].Action != "fill" || f.Steps[0].Selector != "#email" {
			t.Errorf("step 0 = %+v", f.Steps[0])
		}
		if f.Steps[2].Contains != "Welcome" {
			t.Errorf("step 2 contains = %q", f.Steps[2].Contains)
		}
	})

	t.Run("malformed JSON errors", func(t *testing.T) {
		if _, err := ParseFlow(`{"steps": [`); err == nil {
			t.Fatal("want error for malformed JSON, got nil")
		}
	})
}

func TestSplitProxy(t *testing.T) {
	tests := []struct {
		in               string
		addr, user, pass string
	}{
		{"", "", "", ""},
		{"http://host:8080", "http://host:8080", "", ""},
		{"http://user:pass@host:8080", "http://host:8080", "user", "pass"},
		{"socks5://10.0.0.1:1080", "socks5://10.0.0.1:1080", "", ""},
		// A bare host:port (no scheme) is not a usable proxy URL — url.Parse reads
		// "host" as the scheme — so it resolves to direct. dd stores proxies as full
		// URLs (http://…), so this is the expected, consistent behavior.
		{"host:8080", "", "", ""},
	}
	for _, tt := range tests {
		addr, user, pass := splitProxy(tt.in)
		if addr != tt.addr || user != tt.user || pass != tt.pass {
			t.Errorf("splitProxy(%q) = (%q,%q,%q), want (%q,%q,%q)",
				tt.in, addr, user, pass, tt.addr, tt.user, tt.pass)
		}
	}
}

func TestParseDur(t *testing.T) {
	tests := []struct {
		in   string
		want time.Duration
	}{
		{"", 0},
		{"500ms", 500 * time.Millisecond},
		{"2s", 2 * time.Second},
		{"1500", 1500 * time.Millisecond}, // bare integer = ms
		{"bogus", 0},
	}
	for _, tt := range tests {
		if got := parseDur(tt.in); got != tt.want {
			t.Errorf("parseDur(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestStepAction(t *testing.T) {
	known := []string{
		"navigate", "goto", "fill", "type", "sendkeys", "setvalue", "select",
		"click", "submit", "waitVisible", "assertVisible", "waitReady",
		"sleep", "wait", "assertText",
	}
	for _, action := range known {
		s := Step{Action: action, Selector: "#x", Value: "v", URL: "https://a", Contains: "y"}
		if _, err := stepAction(s, nil); err != nil {
			t.Errorf("stepAction(%q) errored: %v", action, err)
		}
	}

	t.Run("unknown action errors", func(t *testing.T) {
		if _, err := stepAction(Step{Action: "frobnicate"}, nil); err == nil {
			t.Error("want error for unknown action")
		}
	})

	t.Run("navigate without url errors", func(t *testing.T) {
		if _, err := stepAction(Step{Action: "navigate"}, nil); err == nil {
			t.Error("want error for navigate with no url")
		}
	})
}

func TestVars(t *testing.T) {
	t.Run("resolveVars expands once", func(t *testing.T) {
		got := resolveVars(map[string]string{"pw": "{{randString:12}}", "lit": "hello"})
		if len(got["pw"]) != 12 {
			t.Errorf("pw = %q, want 12 chars", got["pw"])
		}
		if got["lit"] != "hello" {
			t.Errorf("lit = %q, want hello", got["lit"])
		}
	})

	t.Run("same var reference yields identical value across fields", func(t *testing.T) {
		vars := resolveVars(map[string]string{"pw": "{{randString:16}}"})
		a := expandValue("{{pw}}", vars)
		b := expandValue("{{pw}}", vars)
		if a == "" || a != b {
			t.Errorf("password/confirm mismatch: %q vs %q", a, b)
		}
	})

	t.Run("generators still expand alongside vars", func(t *testing.T) {
		vars := resolveVars(map[string]string{"pw": "{{randString:8}}"})
		got := expandValue(`{"password":"{{pw}}","token":"{{uuid}}"}`, vars)
		if strings.Contains(got, "{{") {
			t.Errorf("unexpanded token left: %q", got)
		}
	})

	t.Run("flow parses vars", func(t *testing.T) {
		f, err := ParseFlow(`{"vars":{"pw":"{{randString:12}}"},"steps":[{"action":"fill","selector":"#p","value":"{{pw}}"}]}`)
		if err != nil {
			t.Fatal(err)
		}
		if f.Vars["pw"] == "" {
			t.Error("vars not parsed")
		}
	})
}

func TestIsNavStep(t *testing.T) {
	for _, a := range []string{"navigate", "goto", "GOTO", " navigate "} {
		if !isNavStep(a) {
			t.Errorf("isNavStep(%q) = false, want true", a)
		}
	}
	for _, a := range []string{"click", "fill", "assertText", ""} {
		if isNavStep(a) {
			t.Errorf("isNavStep(%q) = true, want false", a)
		}
	}
}
