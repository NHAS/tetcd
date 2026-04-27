package paths_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/NHAS/tetcd/codecs"
	"github.com/NHAS/tetcd/paths"
)

func TestMapSlicePath_Prefix(t *testing.T) {
	m := paths.NewMapSlicePath("wag/Thing", codecs.NewJsonCodec[string](), false)
	if got := m.Prefix(); got != "wag/Thing/" {
		t.Errorf("Prefix() = %q, want %q", got, "wag/Thing")
	}
}

func TestMapSlicePath_Key_ReturnsMapPath(t *testing.T) {
	m := paths.NewMapSlicePath("wag/Thing", codecs.NewJsonCodec[string](), false)
	mp := m.Key("bloop")
	if got := mp.Prefix(); got != "wag/Thing/bloop/" {
		t.Errorf("Key().Prefix() = %q, want %q", got, "wag/Thing/bloop/")
	}
}

func TestMapSlicePath_List_Empty(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	m := paths.NewMapSlicePath("wag/Thing/Empty", codecs.NewJsonCodec[string](), false)

	result, err := m.List(ctx, cli)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(result.Values) != 0 {
		t.Errorf("List() = %v, want empty map", result.Values)
	}
}

func TestMapSlicePath_List(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	m := paths.NewMapSlicePath("wag/Thing/List", codecs.NewJsonCodec[string](), false)

	want := map[string]map[string]string{
		"bloop": {"noot1": "data1", "noot2": "data2"},
		"blarp": {"noot3": "data3"},
	}

	for outerK, inner := range want {
		for innerK, v := range inner {
			if err := m.Key(outerK).Key(innerK).Put(ctx, cli, v); err != nil {
				t.Fatalf("Put() error = %v", err)
			}
		}
	}

	result, err := m.List(ctx, cli)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if len(result.Values) != len(want) {
		t.Fatalf("List() returned %d outer keys, want %d", len(result.Values), len(want))
	}
	for outerK, innerWant := range want {
		innerGot, ok := result.Values[outerK]
		if !ok {
			t.Errorf("List() missing outer key %q", outerK)
			continue
		}
		for innerK, v := range innerWant {
			if innerGot[innerK] != v {
				t.Errorf("List()[%q][%q] = %q, want %q", outerK, innerK, innerGot[innerK], v)
			}
		}
	}
}

func TestMapSlicePath_List_SkipsInvalidDepth(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	m := paths.NewMapSlicePath("wag/Thing/Depth", codecs.NewJsonCodec[string](), false)

	// Write a key that only has one level (no inner key) — should be skipped
	if _, err := cli.Put(ctx, "wag/Thing/Depth/onlyone", `"orphan"`); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	if err := m.Key("outer").Key("inner").Put(ctx, cli, "valid"); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	result, err := m.List(ctx, cli)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if _, exists := result.Values["onlyone"]; exists {
		t.Error("List() included a single-level key, expected it to be skipped")
	}
	if result.Values["outer"]["inner"] != "valid" {
		t.Errorf("List()[outer][inner] = %q, want %q", result.Values["outer"]["inner"], "valid")
	}
}

func TestMapSlicePath_DeleteAll(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	m := paths.NewMapSlicePath("wag/Thing/DeleteAll", codecs.NewJsonCodec[string](), false)

	for outerK, inner := range map[string]map[string]string{
		"bloop": {"noot1": "data1", "noot2": "data2"},
		"blarp": {"noot3": "data3"},
	} {
		for innerK, v := range inner {
			if err := m.Key(outerK).Key(innerK).Put(ctx, cli, v); err != nil {
				t.Fatalf("Put(%q) error = %v", m.Key(outerK).Key(innerK).Key(), err)
			}
		}
	}

	num, err := m.DeleteAll(ctx, cli)
	if err != nil {
		t.Fatalf("DeleteAll() error = %v", err)
	}

	if num.Count != 3 {
		t.Errorf("DeleteAll() deleted %d keys, want %d", num.Count, 3)
	}

	result, err := m.List(ctx, cli)
	if err != nil {
		t.Fatalf("List() after DeleteAll() error = %v", err)
	}
	if len(result.Values) != 0 {
		t.Errorf("List() after DeleteAll() = %v, want empty", result.Values)
	}
}

func TestMapSlicePath_DeleteAll_Empty(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	m := paths.NewMapSlicePath("wag/Thing/DeleteAllEmpty", codecs.NewJsonCodec[string](), false)

	if num, err := m.DeleteAll(ctx, cli); err != nil {
		t.Fatalf("DeleteAll() on empty prefix error = %v", err)
	} else if num.Count != 0 {
		t.Errorf("DeleteAll() on empty prefix deleted %d keys, want 0", num.Count)
	}
}

func TestMapSlicePath_Key_NestedList(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	m := paths.NewMapSlicePath("wag/Thing/Nested", codecs.NewJsonCodec[string](), false)

	if err := m.Key("outer").Key("inner").Put(ctx, cli, "hello"); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	// Use the inner MapPath directly to list
	result, err := m.Key("outer").List(ctx, cli)
	if err != nil {
		t.Fatalf("inner List() error = %v", err)
	}
	if result.Values["inner"] != "hello" {
		t.Errorf("inner List()[inner] = %q, want %q", result.Values["inner"], "hello")
	}
}

func TestMapSlicePathApply(t *testing.T) {
	type mapSliceTestValue struct {
		Name  string `json:"name,omitempty"`
		Count int    `json:"count,omitempty"`
	}

	cli, cleanup := setupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	codec := codecs.NewJsonCodec[mapSliceTestValue]()

	m := paths.NewMapSlicePath("things", codec, false)

	put := func(key string, value mapSliceTestValue) {
		t.Helper()

		data, err := codec.Encode(value)
		if err != nil {
			t.Fatalf("encode %q: %v", key, err)
		}

		if _, err := cli.Put(ctx, key, string(data)); err != nil {
			t.Fatalf("seed %q: %v", key, err)
		}
	}

	put("things/group1/item1", mapSliceTestValue{Name: "before", Count: 1})
	put("things/group1/item3", mapSliceTestValue{Name: "delete-me", Count: 3})
	put("things/group2/item9", mapSliceTestValue{Name: "remove-a", Count: 9})
	put("things/group2/item10", mapSliceTestValue{Name: "remove-b", Count: 10})

	patch := json.RawMessage(`{
        "group1": {
            "item1": { "count": 2 },
            "item2": { "name": "new", "count": 7 },
            "item3": null
        },
        "group2": null
    }`)

	ops, err := m.Apply(ctx, cli, patch)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	if len(ops) != 4 {
		t.Fatalf("Apply() returned %d ops, want 4", len(ops))
	}

	if _, err := cli.Txn(ctx).Then(ops...).Commit(); err != nil {
		t.Fatalf("commit ops: %v", err)
	}

	got, err := m.Key("group1").Key("item1").Get(ctx, cli)
	if err != nil {
		t.Fatalf("expected things/group1/item1 to exist: %v", err)
	}

	want := mapSliceTestValue{Name: "before", Count: 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("things/group1/item1 = %+v, want %+v", got, want)
	}

	got, err = m.Key("group1").Key("item2").Get(ctx, cli)
	if err != nil {
		t.Fatalf("expected things/group1/item2 to exist: %v", err)
	}

	want = mapSliceTestValue{Name: "new", Count: 7}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("things/group1/item2 = %+v, want %+v", got, want)
	}

	if _, err := m.Key("group1").Key("item3").Get(ctx, cli); err == nil {
		t.Fatalf("expected things/group1/item3 to be deleted")
	}

	resp, err := m.Key("group2").Entries(context.Background(), cli)
	if err != nil {
		t.Fatalf("get deleted subtree: %v", err)
	}

	if len(resp) != 0 {
		t.Fatalf("expected things/group2/ subtree to be deleted, got %d keys", len(resp))
	}
}
