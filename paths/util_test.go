package paths

import "testing"

func TestJoin(t *testing.T) {

	const prefix = "app/Config/Groups/"

	expected := map[string]string{
		"admins":            "app/Config/Groups/admins",
		"../Server/Host":    "app/Config/Groups/Server/Host",
		"../../Other/Thing": "app/Config/Groups/Other/Thing",
		"":                  "app/Config/Groups",
		"a//b":              "app/Config/Groups/a/b",
		"a/../../b":         "app/Config/Groups/b",
	}

	for k, v := range expected {
		if got := Join(prefix, k); got != v {
			t.Fatalf("Did not match: expected %q. got %q", v, got)
		}
	}

	if got := Join("", "a"); got != "a" {
		t.Fatalf("Did not match: expected \"a\". got %q", got)
	}
}
