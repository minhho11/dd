package pool

import (
	"encoding/json"
	"fmt"
	"strings"
)

// bodyMethods take the params JSON as the request body (Content-Type
// application/json). Everything else (GET, HEAD, DELETE) takes params as query.
var bodyMethods = map[string]bool{"POST": true, "PUT": true, "PATCH": true}

// normalizeMethod upper-cases the method and defaults an empty one to GET.
func normalizeMethod(method string) string {
	m := strings.ToUpper(strings.TrimSpace(method))
	if m == "" {
		return "GET"
	}
	return m
}

// jsonToQuery decodes a JSON object into string query parameters, stringifying
// scalar values. Non-object or invalid JSON yields no params (and an error the
// caller may ignore). Nested objects/arrays are JSON-encoded back into a string.
func jsonToQuery(params string) (map[string]string, error) {
	params = strings.TrimSpace(params)
	if params == "" {
		return nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(params), &m); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		switch val := v.(type) {
		case string:
			out[k] = val
		case float64:
			// json numbers are float64; render integers without a trailing .0
			if val == float64(int64(val)) {
				out[k] = fmt.Sprintf("%d", int64(val))
			} else {
				out[k] = fmt.Sprintf("%g", val)
			}
		case bool:
			out[k] = fmt.Sprintf("%t", val)
		case nil:
			out[k] = ""
		default:
			b, _ := json.Marshal(val)
			out[k] = string(b)
		}
	}
	return out, nil
}
