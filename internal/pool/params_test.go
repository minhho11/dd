package pool

import "testing"

func TestNormalizeMethod(t *testing.T) {
	cases := map[string]string{
		"":       "GET",
		"get":    "GET",
		" post ": "POST",
		"Put":    "PUT",
	}
	for in, want := range cases {
		if got := normalizeMethod(in); got != want {
			t.Errorf("normalizeMethod(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestJSONToQuery(t *testing.T) {
	q, err := jsonToQuery(`{"a":"x","n":5,"f":1.5,"b":true,"z":null}`)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	want := map[string]string{"a": "x", "n": "5", "f": "1.5", "b": "true", "z": ""}
	for k, v := range want {
		if q[k] != v {
			t.Errorf("q[%q] = %q, want %q", k, q[k], v)
		}
	}
}

func TestJSONToQueryEmptyAndInvalid(t *testing.T) {
	if q, err := jsonToQuery(""); err != nil || q != nil {
		t.Errorf("empty: got %v, %v", q, err)
	}
	if _, err := jsonToQuery("not json"); err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestBodyMethods(t *testing.T) {
	for _, m := range []string{"POST", "PUT", "PATCH"} {
		if !bodyMethods[m] {
			t.Errorf("%s should be a body method", m)
		}
	}
	for _, m := range []string{"GET", "HEAD", "DELETE"} {
		if bodyMethods[m] {
			t.Errorf("%s should not be a body method", m)
		}
	}
}
