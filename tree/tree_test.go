package tree_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/NHAS/tetcd/codecs"
	"github.com/NHAS/tetcd/paths"
	"github.com/NHAS/tetcd/testhelpers"
	tetree "github.com/NHAS/tetcd/tree"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type capturingApplier struct {
	tetree.Applier
	changes []json.RawMessage
}

func (c *capturingApplier) Apply(ctx context.Context, cli *clientv3.Client, change json.RawMessage) ([]clientv3.Op, error) {
	c.changes = append(c.changes, append(json.RawMessage(nil), change...))
	return c.Applier.Apply(ctx, cli, change)
}

type trieTestConfig struct {
	Wag    trieTestWag `json:"wag"`
	Simple string
}

type trieTestWag struct {
	Acls trieTestAcls `json:"Acls"`
}

type trieTestAcls struct {
	Groups  map[string]trieGroupValue             `json:"Groups"`
	Members map[string]map[string]trieMemberValue `json:"Members"`
}

type trieGroupValue struct {
	Name  string `json:"name"`
	Level int    `json:"level"`
}

type trieMemberValue struct {
	Name   string `json:"name"`
	Weight int    `json:"weight"`
}

func TestTree_Apply_PassesExpectedMergeSubtreesToMapAppliers(t *testing.T) {
	cli, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	groups := paths.NewMapPath("wag/Acls/Groups", codecs.NewJsonCodec[trieGroupValue](), false)
	members := paths.NewMapSlicePath("wag/Acls/Members", codecs.NewJsonCodec[trieMemberValue](), false)
	simple := paths.NewPath("Simple", codecs.NewJsonCodec[string]())

	groupsCapture := &capturingApplier{Applier: groups}
	membersCapture := &capturingApplier{Applier: members}
	simpleCapture := &capturingApplier{Applier: simple}

	tr := tetree.NewTree[trieTestConfig]()
	tr.Register(groupsCapture)
	tr.Register(membersCapture)
	tr.Register(simpleCapture)

	original := trieTestConfig{
		Simple: "yes",
		Wag: trieTestWag{
			Acls: trieTestAcls{
				Groups: map[string]trieGroupValue{
					"admins":  {Name: "alice", Level: 1},
					"viewers": {Name: "carol", Level: 2},
				},
				Members: map[string]map[string]trieMemberValue{
					"team1": {
						"alice": {Name: "alice", Weight: 1},
						"bob":   {Name: "bob", Weight: 2},
					},
					"team2": {
						"eve": {Name: "eve", Weight: 3},
					},
				},
			},
		},
	}

	modified := trieTestConfig{
		Simple: "no",
		Wag: trieTestWag{
			Acls: trieTestAcls{
				Groups: map[string]trieGroupValue{
					"admins": {Name: "alice", Level: 2},
					"guests": {Name: "dave", Level: 3},
				},
				Members: map[string]map[string]trieMemberValue{
					"team1": {
						"alice": {Name: "alice", Weight: 10},
						"carol": {Name: "carol", Weight: 4},
					},
					"team3": {
						"dan": {Name: "dan", Weight: 5},
					},
				},
			},
		},
	}

	err := simple.Put(ctx, cli, original.Simple)
	if err != nil {
		t.Fatal(err)
	}
	seedMap(t, ctx, cli, groups, original.Wag.Acls.Groups)
	seedMapSlice(t, ctx, cli, members, original.Wag.Acls.Members)

	if err := tr.Apply(ctx, cli, mustMarshalJSON(t, original), mustMarshalJSON(t, modified)); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	if len(simpleCapture.changes) != 1 {
		t.Fatalf("simple applier saw %d changes, want 1", len(simpleCapture.changes))
	}

	assertJSONEqual(t, simpleCapture.changes[0], `"no"`)

	if len(groupsCapture.changes) != 1 {
		t.Fatalf("groups applier saw %d changes, want 1", len(groupsCapture.changes))
	}

	assertJSONEqual(t, groupsCapture.changes[0], `{
        "admins": {"level": 2},
        "viewers": null,
        "guests": {"name": "dave", "level": 3}
    }`)

	if len(membersCapture.changes) != 1 {
		t.Fatalf("members applier saw %d changes, want 1", len(membersCapture.changes))
	}

	assertJSONEqual(t, membersCapture.changes[0], `{
        "team1": {
            "alice": {"weight": 10},
            "bob": null,
            "carol": {"name": "carol", "weight": 4}
        },
        "team2": null,
        "team3": {
            "dan": {"name": "dan", "weight": 5}
        }
    }`)

	groupResult, err := groups.List(ctx, cli)
	if err != nil {
		t.Fatalf("groups.List() error = %v", err)
	}

	if !reflect.DeepEqual(groupResult.Values, modified.Wag.Acls.Groups) {
		t.Fatalf("groups.List().Values = %#v, want %#v", groupResult.Values, modified.Wag.Acls.Groups)
	}

	memberResult, err := members.List(ctx, cli)
	if err != nil {
		t.Fatalf("members.List() error = %v", err)
	}

	if !reflect.DeepEqual(memberResult.Values, modified.Wag.Acls.Members) {
		t.Fatalf("members.List().Values = %#v, want %#v", memberResult.Values, modified.Wag.Acls.Members)
	}
}

func mustMarshalJSON(t *testing.T, v any) []byte {
	t.Helper()

	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	return data
}

func assertJSONEqual(t *testing.T, got []byte, want string) {
	t.Helper()

	var gotValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("json.Unmarshal(got) error = %v", err)
	}

	var wantValue any
	if err := json.Unmarshal([]byte(want), &wantValue); err != nil {
		t.Fatalf("json.Unmarshal(want) error = %v", err)
	}

	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("got JSON %s, want %s", string(got), want)
	}
}

func seedMap[T any](t *testing.T, ctx context.Context, cli *clientv3.Client, m paths.MapPath[T], data map[string]T) {
	t.Helper()

	for k, v := range data {
		if err := m.Key(k).Put(ctx, cli, v); err != nil {
			t.Fatalf("seeding %q under %q: %v", k, m.Prefix(), err)
		}
	}
}

func seedMapSlice[T any](t *testing.T, ctx context.Context, cli *clientv3.Client, m paths.MapSlicePath[T], data map[string]map[string]T) {
	t.Helper()

	for outerKey, inner := range data {
		seedMap(t, ctx, cli, m.Key(outerKey), inner)
	}
}
