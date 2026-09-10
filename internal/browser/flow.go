// Package browser executes a scripted sequence of browser actions (a "flow")
// against a target using a headless Chromium via chromedp: navigate a page, fill
// and submit forms, wait for elements, and assert on the result. It is the
// browser-mode counterpart to the HTTP request path — a urls row with
// mode='browser' carries its flow as a {"steps":[...]} JSON object in the params
// column, and step values support the same {{...}} generators (internal/tmpl) as
// HTTP params, expanded fresh on every run so each submission gets unique data.
package browser

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Step is one action in a flow. Which fields matter depends on Action:
//
//   - navigate/goto:            url (or value) — the page to load
//   - fill/type/sendkeys:       selector, value — type value into the element
//   - setvalue:                 selector, value — set the element's value directly
//   - select:                   selector, value — set a <select>'s value
//   - click:                    selector — click the element
//   - submit:                   selector — submit the form containing the element
//   - waitVisible/waitReady:    selector — wait for the element
//   - assertVisible:            selector — fail the run if not visible in time
//   - assertText:               selector, contains — fail unless the element's
//     text contains the (literal) substring
//   - sleep/wait:               value — a duration ("500ms", "2s") to pause
//
// value and url are expanded through internal/tmpl per run ({{randEmail}}, etc.);
// contains is compared literally. timeout overrides the default per-step timeout
// for slow steps (a Go duration string, e.g. "10s").
type Step struct {
	Action   string `json:"action"`
	Selector string `json:"selector,omitempty"`
	Value    string `json:"value,omitempty"`
	Contains string `json:"contains,omitempty"`
	URL      string `json:"url,omitempty"`
	Timeout  string `json:"timeout,omitempty"`
}

// Flow is an ordered list of steps, decoded from a target's params JSON. Vars are
// named values generated once per run and referenced from step values as
// {{name}}, so several fields can share one value — e.g. a password and its
// confirmation. Each var's value is itself expanded through internal/tmpl once at
// the start of the run (so {"pw":"{{randString:12}}"} picks one random string for
// the whole flow), then substituted wherever {{pw}} appears.
type Flow struct {
	Vars  map[string]string `json:"vars,omitempty"`
	Steps []Step            `json:"steps"`
}

// ParseFlow decodes a browser flow from a target's params string, which must be a
// JSON object of the form {"steps":[{"action":...}, ...]}. An empty params string
// yields an empty flow (the entry URL is still navigated). It returns an error for
// malformed JSON or a flow with no steps and no way to act.
func ParseFlow(params string) (Flow, error) {
	params = strings.TrimSpace(params)
	if params == "" {
		return Flow{}, nil
	}
	var f Flow
	if err := json.Unmarshal([]byte(params), &f); err != nil {
		return Flow{}, fmt.Errorf("parse browser flow: %w", err)
	}
	return f, nil
}
