package tmpl

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func TestExpandGenerators(t *testing.T) {
	t.Run("randString length", func(t *testing.T) {
		got := Expand(`{{randString:8}}`)
		if len(got) != 8 {
			t.Errorf("len = %d, want 8 (%q)", len(got), got)
		}
	})

	t.Run("randInt in range", func(t *testing.T) {
		for i := 0; i < 200; i++ {
			n, err := strconv.Atoi(Expand(`{{randInt:5:10}}`))
			if err != nil || n < 5 || n > 10 {
				t.Fatalf("randInt out of range: %d (err %v)", n, err)
			}
		}
	})

	t.Run("randDigits", func(t *testing.T) {
		got := Expand(`{{randDigits:5}}`)
		if !regexp.MustCompile(`^[0-9]{5}$`).MatchString(got) {
			t.Errorf("randDigits = %q, want 5 digits", got)
		}
	})

	t.Run("randEmail default and custom domain", func(t *testing.T) {
		if !strings.HasSuffix(Expand(`{{randEmail}}`), "@example.com") {
			t.Error("default domain wrong")
		}
		got := Expand(`{{randEmail:load.test}}`)
		if !regexp.MustCompile(`^[a-z0-9]{10}@load\.test$`).MatchString(got) {
			t.Errorf("randEmail = %q", got)
		}
	})

	t.Run("uuid v4 shape", func(t *testing.T) {
		got := Expand(`{{uuid}}`)
		if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(got) {
			t.Errorf("uuid = %q", got)
		}
	})

	t.Run("multiple placeholders in JSON body", func(t *testing.T) {
		got := Expand(`{"email":"{{randEmail}}","code":{{randInt:1:9}},"id":"{{uuid}}"}`)
		if strings.Contains(got, "{{") {
			t.Errorf("unexpanded placeholder left: %q", got)
		}
	})

	t.Run("unknown generator left untouched", func(t *testing.T) {
		if got := Expand(`{{bogus}}`); got != `{{bogus}}` {
			t.Errorf("unknown generator = %q, want left as-is", got)
		}
	})

	t.Run("no placeholders unchanged", func(t *testing.T) {
		in := `{"a":"b"}`
		if got := Expand(in); got != in {
			t.Errorf("got %q, want %q", got, in)
		}
	})

	t.Run("fresh value per call", func(t *testing.T) {
		seen := map[string]bool{}
		for i := 0; i < 50; i++ {
			seen[Expand(`{{randString:12}}`)] = true
		}
		if len(seen) < 45 { // allow a rare collision, but they must vary
			t.Errorf("values not varying per call: %d distinct of 50", len(seen))
		}
	})
}
