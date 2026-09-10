// Package tmpl expands {{...}} placeholders in a string into freshly generated
// random values, so each use gets unique data. It is shared by the HTTP request
// path (internal/pool) and the browser automation path (internal/browser) so both
// support the same generators in their params/step values.
package tmpl

import (
	"fmt"
	"math/rand/v2"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// tmplRe matches placeholders of the form {{name}} or {{name:arg1:arg2}}. Names
// are letters only; args are everything up to the closing }}.
var tmplRe = regexp.MustCompile(`\{\{\s*([a-zA-Z]+)(?::([^}]*))?\s*\}\}`)

const (
	alnumChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	lowerAlnum = "abcdefghijklmnopqrstuvwxyz0123456789"
	digitChars = "0123456789"
	hexChars   = "0123456789abcdef"
)

// Expand replaces every {{...}} placeholder in s with a freshly generated value,
// so each call gets unique data. Strings without "{{" are returned unchanged.
// Unknown placeholders are left as-is so typos are visible. Safe for concurrent
// use; meant to run once per request/flow.
func Expand(s string) string {
	if !strings.Contains(s, "{{") {
		return s
	}
	return tmplRe.ReplaceAllStringFunc(s, func(match string) string {
		sub := tmplRe.FindStringSubmatch(match)
		name := strings.ToLower(sub[1])
		var args []string
		if sub[2] != "" {
			args = strings.Split(sub[2], ":")
			for i := range args {
				args[i] = strings.TrimSpace(args[i])
			}
		}
		if v, ok := generate(name, args); ok {
			return v
		}
		return match // unknown generator: leave the placeholder untouched
	})
}

// generate produces one value for a placeholder name + args, reporting whether the
// name is a known generator.
func generate(name string, args []string) (string, bool) {
	argInt := func(i, def int) int {
		if i < len(args) {
			if n, err := strconv.Atoi(args[i]); err == nil {
				return n
			}
		}
		return def
	}
	switch name {
	case "randstring", "randstr", "randalnum":
		return randChars(alnumChars, argInt(0, 10)), true
	case "randdigits", "randnumber", "randnum":
		return randChars(digitChars, argInt(0, 6)), true
	case "randhex":
		return randChars(hexChars, argInt(0, 12)), true
	case "randint":
		min, max := argInt(0, 0), argInt(1, 100)
		if min > max {
			min, max = max, min
		}
		return strconv.Itoa(min + rand.IntN(max-min+1)), true
	case "randemail":
		domain := "example.com"
		if len(args) > 0 && args[0] != "" {
			domain = args[0]
		}
		return randChars(lowerAlnum, 10) + "@" + domain, true
	case "randbool":
		return strconv.FormatBool(rand.IntN(2) == 1), true
	case "uuid":
		return randUUIDv4(), true
	case "timestamp", "unix":
		return strconv.FormatInt(time.Now().Unix(), 10), true
	default:
		return "", false
	}
}

func randChars(charset string, n int) string {
	if n < 1 {
		n = 1
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[rand.IntN(len(charset))]
	}
	return string(b)
}

// randUUIDv4 returns a random RFC 4122 version-4 UUID.
func randUUIDv4() string {
	var b [16]byte
	for i := range b {
		b[i] = byte(rand.IntN(256))
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
