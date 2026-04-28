package tree_test

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
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

func TestTree_Plan_ReturnsExpectedChangedKeys(t *testing.T) {
	groups := paths.NewMapPath("wag/Acls/Groups", codecs.NewJsonCodec[trieGroupValue](), false)
	members := paths.NewMapSlicePath("wag/Acls/Members", codecs.NewJsonCodec[trieMemberValue](), false)
	simple := paths.NewPath("Simple", codecs.NewJsonCodec[string]())

	tr := tetree.NewTree[trieTestConfig]()
	tr.Register(groups)
	tr.Register(members)
	tr.Register(simple)

	plan, err := tr.Plan(
		context.Background(),
		mustMarshalJSON(t, trieTestOriginalConfig()),
		mustMarshalJSON(t, trieTestModifiedConfig()),
	)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}

	got := append([]string(nil), plan.KeysChanged()...)
	sort.Strings(got)

	want := []string{"Simple", "wag/Acls/Groups", "wag/Acls/Members"}
	sort.Strings(want)

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Plan().KeysChanged() = %v, want %v", got, want)
	}
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

	original := trieTestOriginalConfig()
	modified := trieTestModifiedConfig()

	err := simple.Put(ctx, cli, original.Simple)
	if err != nil {
		t.Fatal(err)
	}
	seedMap(t, ctx, cli, groups, original.Wag.Acls.Groups)
	seedMapSlice(t, ctx, cli, members, original.Wag.Acls.Members)

	plan, err := tr.Plan(ctx, mustMarshalJSON(t, original), mustMarshalJSON(t, modified))
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}

	if err := tr.Apply(ctx, cli, plan); err != nil {
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

func TestTree_PlanAndApply_NoChanges_NoOps(t *testing.T) {
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

	original := trieTestOriginalConfig()

	if err := simple.Put(ctx, cli, original.Simple); err != nil {
		t.Fatal(err)
	}
	seedMap(t, ctx, cli, groups, original.Wag.Acls.Groups)
	seedMapSlice(t, ctx, cli, members, original.Wag.Acls.Members)

	plan, err := tr.Plan(ctx, mustMarshalJSON(t, original), mustMarshalJSON(t, original))
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}

	if got := plan.KeysChanged(); len(got) != 0 {
		t.Fatalf("Plan().KeysChanged() = %v, want no changes", got)
	}

	if err := tr.Apply(ctx, cli, plan); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	if len(simpleCapture.changes) != 0 {
		t.Fatalf("simple applier saw %d changes, want 0", len(simpleCapture.changes))
	}
	if len(groupsCapture.changes) != 0 {
		t.Fatalf("groups applier saw %d changes, want 0", len(groupsCapture.changes))
	}
	if len(membersCapture.changes) != 0 {
		t.Fatalf("members applier saw %d changes, want 0", len(membersCapture.changes))
	}

	groupResult, err := groups.List(ctx, cli)
	if err != nil {
		t.Fatalf("groups.List() error = %v", err)
	}

	if !reflect.DeepEqual(groupResult.Values, original.Wag.Acls.Groups) {
		t.Fatalf("groups.List().Values = %#v, want %#v", groupResult.Values, original.Wag.Acls.Groups)
	}

	memberResult, err := members.List(ctx, cli)
	if err != nil {
		t.Fatalf("members.List() error = %v", err)
	}

	if !reflect.DeepEqual(memberResult.Values, original.Wag.Acls.Members) {
		t.Fatalf("members.List().Values = %#v, want %#v", memberResult.Values, original.Wag.Acls.Members)
	}
}

func TestTree_PlanAndApply_EmptyOriginalJSON_AppliesFullConfig(t *testing.T) {
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

	modified := trieTestModifiedConfig()

	plan, err := tr.Plan(ctx, []byte{}, mustMarshalJSON(t, modified))
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}

	got := append([]string(nil), plan.KeysChanged()...)
	sort.Strings(got)

	want := []string{"Simple", "wag/Acls/Groups", "wag/Acls/Members"}
	sort.Strings(want)

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Plan().KeysChanged() = %v, want %v", got, want)
	}

	if err := tr.Apply(ctx, cli, plan); err != nil {
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
        "admins": {"name": "alice", "level": 2},
        "guests": {"name": "dave", "level": 3}
    }`)

	if len(membersCapture.changes) != 1 {
		t.Fatalf("members applier saw %d changes, want 1", len(membersCapture.changes))
	}
	assertJSONEqual(t, membersCapture.changes[0], `{
        "team1": {
            "alice": {"name": "alice", "weight": 10},
            "carol": {"name": "carol", "weight": 4}
        },
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

func trieTestOriginalConfig() trieTestConfig {
	return trieTestConfig{
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
}

func trieTestModifiedConfig() trieTestConfig {
	return trieTestConfig{
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
