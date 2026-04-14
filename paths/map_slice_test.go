package paths_test

import (
	"context"
	"testing"

	"github.com/NHAS/tetcd/codecs"
	"github.com/NHAS/tetcd/paths"
)

func TestMapSlicePath_Prefix(t *testing.T) {
	m := paths.NewMapSlicePath("wag/Thing", codecs.NewJsonCodec[string]())
	if got := m.Prefix(); got != "wag/Thing" {
		t.Errorf("Prefix() = %q, want %q", got, "wag/Thing")
	}
}

func TestMapSlicePath_Key_ReturnsMapPath(t *testing.T) {
	m := paths.NewMapSlicePath("wag/Thing", codecs.NewJsonCodec[string]())
	mp := m.Key("bloop")
	if got := mp.Prefix(); got != "wag/Thing/bloop" {
		t.Errorf("Key().Prefix() = %q, want %q", got, "wag/Thing/bloop")
	}
}

func TestMapSlicePath_List_Empty(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	m := paths.NewMapSlicePath("wag/Thing/Empty", codecs.NewJsonCodec[string]())

	got, err := m.List(ctx, cli)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("List() = %v, want empty map", got)
	}
}

func TestMapSlicePath_List(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	m := paths.NewMapSlicePath("wag/Thing/List", codecs.NewJsonCodec[string]())

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

	got, err := m.List(ctx, cli)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("List() returned %d outer keys, want %d", len(got), len(want))
	}
	for outerK, innerWant := range want {
		innerGot, ok := got[outerK]
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

	m := paths.NewMapSlicePath("wag/Thing/Depth", codecs.NewJsonCodec[string]())

	// Write a key that only has one level (no inner key) — should be skipped
	if _, err := cli.Put(ctx, "wag/Thing/Depth/onlyone", `"orphan"`); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	if err := m.Key("outer").Key("inner").Put(ctx, cli, "valid"); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	got, err := m.List(ctx, cli)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if _, exists := got["onlyone"]; exists {
		t.Error("List() included a single-level key, expected it to be skipped")
	}
	if got["outer"]["inner"] != "valid" {
		t.Errorf("List()[outer][inner] = %q, want %q", got["outer"]["inner"], "valid")
	}
}

func TestMapSlicePath_DeleteAll(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	m := paths.NewMapSlicePath("wag/Thing/DeleteAll", codecs.NewJsonCodec[string]())

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

	if err := m.DeleteAll(ctx, cli); err != nil {
		t.Fatalf("DeleteAll() error = %v", err)
	}

	got, err := m.List(ctx, cli)
	if err != nil {
		t.Fatalf("List() after DeleteAll() error = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("List() after DeleteAll() = %v, want empty", got)
	}
}

func TestMapSlicePath_DeleteAll_Empty(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	m := paths.NewMapSlicePath("wag/Thing/DeleteAllEmpty", codecs.NewJsonCodec[string]())

	if err := m.DeleteAll(ctx, cli); err != nil {
		t.Fatalf("DeleteAll() on empty prefix error = %v", err)
	}
}

func TestMapSlicePath_Key_NestedList(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	m := paths.NewMapSlicePath("wag/Thing/Nested", codecs.NewJsonCodec[string]())

	if err := m.Key("outer").Key("inner").Put(ctx, cli, "hello"); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	// Use the inner MapPath directly to list
	inner, err := m.Key("outer").List(ctx, cli)
	if err != nil {
		t.Fatalf("inner List() error = %v", err)
	}
	if inner["inner"] != "hello" {
		t.Errorf("inner List()[inner] = %q, want %q", inner["inner"], "hello")
	}
}
