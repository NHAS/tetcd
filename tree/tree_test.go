package tree_test

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/NHAS/tetcd/codecs"
	"github.com/NHAS/tetcd/paths"
	"github.com/NHAS/tetcd/testhelpers"
	tetree "github.com/NHAS/tetcd/tree"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type trieTestConfig struct {
	Wag    trieTestWag `json:"wag"`
	Simple string

	// an array stored as a key per element, the elements are the keys and have no value
	Tags []string
	// an array of keys under each map key
	Sets map[string][]string
	// an array stored as a single value
	Compressed []string `tetcd:"compress"`

	// the same again, in structs below the top level
	Deep trieTestDeep `json:"deep"`
}

type trieTestDeep struct {
	Inner trieTestInner `json:"inner"`
}

type trieTestInner struct {
	Name       string
	Tags       []string
	Sets       map[string][]string
	Compressed []string `tetcd:"compress"`
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

// testTree registers an applier for each way a value can be stored
type testTree struct {
	*tetree.Tree[trieTestConfig]

	simple     paths.Path[string]
	groups     paths.MapPath[trieGroupValue]
	members    paths.MapSlicePath[trieMemberValue]
	tags       paths.MapPath[string]
	sets       paths.MapSlicePath[string]
	compressed paths.Path[[]string]

	deepName       paths.Path[string]
	deepTags       paths.MapPath[string]
	deepSets       paths.MapSlicePath[string]
	deepCompressed paths.Path[[]string]
}

func newTestTree() testTree {
	tt := testTree{
		Tree:       tetree.NewTree[trieTestConfig](""),
		simple:     paths.NewPath("Simple", codecs.NewJsonCodec[string]()),
		groups:     paths.NewMapPath("wag/Acls/Groups", codecs.NewJsonCodec[trieGroupValue](), false),
		members:    paths.NewMapSlicePath("wag/Acls/Members", codecs.NewJsonCodec[trieMemberValue](), false),
		tags:       paths.NewMapPath("Tags", codecs.NewJsonCodec[string](), true),
		sets:       paths.NewMapSlicePath("Sets", codecs.NewJsonCodec[string](), true),
		compressed: paths.NewPath("Compressed", codecs.NewJsonCodec[[]string]()),

		deepName:       paths.NewPath("deep/inner/Name", codecs.NewJsonCodec[string]()),
		deepTags:       paths.NewMapPath("deep/inner/Tags", codecs.NewJsonCodec[string](), true),
		deepSets:       paths.NewMapSlicePath("deep/inner/Sets", codecs.NewJsonCodec[string](), true),
		deepCompressed: paths.NewPath("deep/inner/Compressed", codecs.NewJsonCodec[[]string]()),
	}

	tt.Register(tt.simple)
	tt.Register(tt.groups)
	tt.Register(tt.members)
	tt.Register(tt.tags)
	tt.Register(tt.sets)
	tt.Register(tt.compressed)
	tt.Register(tt.deepName)
	tt.Register(tt.deepTags)
	tt.Register(tt.deepSets)
	tt.Register(tt.deepCompressed)

	return tt
}

func TestTree_Plan_ReturnsAddsAndRemoves(t *testing.T) {
	tt := newTestTree()

	plan, err := tt.Plan(context.Background(), mustMarshalJSON(t, trieTestOriginalConfig()), mustMarshalJSON(t, trieTestModifiedConfig()))
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}

	// an edited entry is put whole over the stored one, so it is an add and not a remove followed by an add
	assertChanges(t, plan.Changes(),
		added("Simple", `"no"`),

		added("wag/Acls/Groups/admins", `{"name": "alice", "level": 2}`),
		removed("wag/Acls/Groups/viewers", `{"name": "carol", "level": 2}`),
		added("wag/Acls/Groups/guests", `{"name": "dave", "level": 3}`),

		added("wag/Acls/Members/team1/alice", `{"name": "alice", "weight": 10}`),
		removed("wag/Acls/Members/team1/bob", `{"name": "bob", "weight": 2}`),
		added("wag/Acls/Members/team1/carol", `{"name": "carol", "weight": 4}`),
		removed("wag/Acls/Members/team2/eve", `{"name": "eve", "weight": 3}`),
		added("wag/Acls/Members/team3/dan", `{"name": "dan", "weight": 5}`),

		// each element is its own key, so "c" and "a" are left alone, and what happened to their position doesn't matter
		removed("Tags/b", `"b"`),
		added("Tags/d", `"d"`),

		removed("Sets/team1/a", `"a"`),
		added("Sets/team1/c", `"c"`),
		added("Sets/team2/z", `"z"`),

		// a compressed array is one value, so any change puts all of it
		added("Compressed", `["x", "z"]`),

		// below the top level, behaving the same
		added("deep/inner/Name", `"n2"`),
		removed("deep/inner/Tags/p", `"p"`),
		added("deep/inner/Tags/r", `"r"`),
		added("deep/inner/Sets/s1/n", `"n"`),
		added("deep/inner/Compressed", `["k1"]`),
	)
}

func TestTree_Plan_EmptyOriginalJSON_AddsFullConfig(t *testing.T) {
	tt := newTestTree()

	plan, err := tt.Plan(context.Background(), []byte{}, mustMarshalJSON(t, trieTestModifiedConfig()))
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}

	assertChanges(t, plan.Changes(),
		added("Simple", `"no"`),
		added("wag/Acls/Groups/admins", `{"name": "alice", "level": 2}`),
		added("wag/Acls/Groups/guests", `{"name": "dave", "level": 3}`),
		added("wag/Acls/Members/team1/alice", `{"name": "alice", "weight": 10}`),
		added("wag/Acls/Members/team1/carol", `{"name": "carol", "weight": 4}`),
		added("wag/Acls/Members/team3/dan", `{"name": "dan", "weight": 5}`),
		added("Tags/a", `"a"`),
		added("Tags/c", `"c"`),
		added("Tags/d", `"d"`),
		added("Sets/team1/b", `"b"`),
		added("Sets/team1/c", `"c"`),
		added("Sets/team2/z", `"z"`),
		added("Compressed", `["x", "z"]`),
		added("deep/inner/Name", `"n2"`),
		added("deep/inner/Tags/q", `"q"`),
		added("deep/inner/Tags/r", `"r"`),
		added("deep/inner/Sets/s1/m", `"m"`),
		added("deep/inner/Sets/s1/n", `"n"`),
		added("deep/inner/Compressed", `["k1"]`),
	)
}

func TestTree_Plan_RemovedArrays_RemoveTheirKeys(t *testing.T) {
	tt := newTestTree()

	plan, err := tt.Plan(context.Background(), []byte(`{"Tags":["a","b"],"Sets":{"t":["c"]},"Compressed":["x"]}`), []byte(`{}`))
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}

	assertChanges(t, plan.Changes(),
		removed("Tags/a", `"a"`),
		removed("Tags/b", `"b"`),
		removed("Sets/t/c", `"c"`),
		removed("Compressed", `["x"]`),
	)
}

func TestTree_Plan_ArrayOrder(t *testing.T) {
	tt := newTestTree()

	before := `{"Tags":["a","b"],"Sets":{"t":["a","b"]},"Compressed":["x","y"]}`
	after := `{"Tags":["b","a"],"Sets":{"t":["b","a"]},"Compressed":["y","x"]}`

	plan, err := tt.Plan(context.Background(), []byte(before), []byte(after))
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}

	// arrays held as keys have no order, a compressed array is a value that has changed
	assertChanges(t, plan.Changes(), added("Compressed", `["y", "x"]`))
}

func TestTree_Plan_NestedStructs(t *testing.T) {
	const full = `{"deep":{"inner":{"Name":"n","Tags":["a"],"Sets":{"s":["b"]},"Compressed":["c"]}}}`

	everything := []wantChange{
		added("deep/inner/Name", `"n"`),
		added("deep/inner/Tags/a", `"a"`),
		added("deep/inner/Sets/s/b", `"b"`),
		added("deep/inner/Compressed", `["c"]`),
	}

	tests := []struct {
		name          string
		before, after string
		want          []wantChange
	}{
		{"struct added whole", `{}`, full, everything},
		{"struct removed whole", full, `{}`, []wantChange{
			removed("deep/inner/Name", `"n"`),
			removed("deep/inner/Tags/a", `"a"`),
			removed("deep/inner/Sets/s/b", `"b"`),
			removed("deep/inner/Compressed", `["c"]`),
		}},
		{"inner struct added to an existing one", `{"deep":{}}`, full, everything},
		{"fields added to an existing struct", `{"deep":{"inner":{"Name":"n"}}}`, full, []wantChange{
			added("deep/inner/Tags/a", `"a"`),
			added("deep/inner/Sets/s/b", `"b"`),
			added("deep/inner/Compressed", `["c"]`),
		}},
		{"only one field changed", full, `{"deep":{"inner":{"Name":"n","Tags":["a","z"],"Sets":{"s":["b"]},"Compressed":["c"]}}}`, []wantChange{
			added("deep/inner/Tags/z", `"z"`),
		}},
		{"nothing changed", full, full, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := newTestTree().Plan(context.Background(), []byte(tt.before), []byte(tt.after))
			if err != nil {
				t.Fatalf("Plan() error = %v", err)
			}

			assertChanges(t, plan.Changes(), tt.want...)
		})
	}
}

func TestTree_Plan_NoChanges_IsEmpty(t *testing.T) {
	tt := newTestTree()

	original := mustMarshalJSON(t, trieTestOriginalConfig())

	plan, err := tt.Plan(context.Background(), original, original)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}

	if plan.Changed() || len(plan.Changes()) != 0 {
		t.Fatalf("Plan().Changes() = %+v, want no changes", plan.Changes())
	}
}

func TestTree_Plan_UnknownKey_Errors(t *testing.T) {
	tt := newTestTree()

	if _, err := tt.Plan(context.Background(), []byte(`{}`), []byte(`{"Simple":"yes","Unknown":1}`)); err == nil {
		t.Fatal("Plan() should fail for a key that no applier matches")
	}
}

func TestTree_Apply_UpdatesStore(t *testing.T) {
	cli, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tt := newTestTree()

	original, modified := trieTestOriginalConfig(), trieTestModifiedConfig()

	if err := tt.simple.Put(ctx, cli, original.Simple); err != nil {
		t.Fatal(err)
	}
	seedMap(t, ctx, cli, tt.groups, original.Wag.Acls.Groups)
	seedMapSlice(t, ctx, cli, tt.members, original.Wag.Acls.Members)
	seedKeys(t, ctx, cli, "Tags", original.Tags)
	for outer, elements := range original.Sets {
		seedKeys(t, ctx, cli, "Sets/"+outer, elements)
	}
	if err := tt.compressed.Put(ctx, cli, original.Compressed); err != nil {
		t.Fatal(err)
	}

	inner := original.Deep.Inner
	if err := tt.deepName.Put(ctx, cli, inner.Name); err != nil {
		t.Fatal(err)
	}
	seedKeys(t, ctx, cli, "deep/inner/Tags", inner.Tags)
	for outer, elements := range inner.Sets {
		seedKeys(t, ctx, cli, "deep/inner/Sets/"+outer, elements)
	}
	if err := tt.deepCompressed.Put(ctx, cli, inner.Compressed); err != nil {
		t.Fatal(err)
	}

	plan, err := tt.Plan(ctx, mustMarshalJSON(t, original), mustMarshalJSON(t, modified))
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}

	if err := tt.Apply(ctx, cli, plan); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	simpleResult, err := tt.simple.Get(ctx, cli)
	if err != nil {
		t.Fatalf("simple.Get() error = %v", err)
	}

	if simpleResult != modified.Simple {
		t.Fatalf("simple.Get() = %q, want %q", simpleResult, modified.Simple)
	}

	groupResult, err := tt.groups.List(ctx, cli)
	if err != nil {
		t.Fatalf("groups.List() error = %v", err)
	}

	if !reflect.DeepEqual(groupResult.Values, modified.Wag.Acls.Groups) {
		t.Fatalf("groups.List().Values = %#v, want %#v", groupResult.Values, modified.Wag.Acls.Groups)
	}

	memberResult, err := tt.members.List(ctx, cli)
	if err != nil {
		t.Fatalf("members.List() error = %v", err)
	}

	if !reflect.DeepEqual(memberResult.Values, modified.Wag.Acls.Members) {
		t.Fatalf("members.List().Values = %#v, want %#v", memberResult.Values, modified.Wag.Acls.Members)
	}

	assertKeys(t, ctx, cli, "Tags/", "Tags/a", "Tags/c", "Tags/d")
	assertKeys(t, ctx, cli, "Sets/", "Sets/team1/b", "Sets/team1/c", "Sets/team2/z")

	compressedResult, err := tt.compressed.Get(ctx, cli)
	if err != nil {
		t.Fatalf("compressed.Get() error = %v", err)
	}

	if !reflect.DeepEqual(compressedResult, modified.Compressed) {
		t.Fatalf("compressed.Get() = %v, want %v", compressedResult, modified.Compressed)
	}

	deepName, err := tt.deepName.Get(ctx, cli)
	if err != nil {
		t.Fatalf("deepName.Get() error = %v", err)
	}

	if deepName != modified.Deep.Inner.Name {
		t.Fatalf("deepName.Get() = %q, want %q", deepName, modified.Deep.Inner.Name)
	}

	assertKeys(t, ctx, cli, "deep/inner/Tags/", "deep/inner/Tags/q", "deep/inner/Tags/r")
	assertKeys(t, ctx, cli, "deep/inner/Sets/", "deep/inner/Sets/s1/m", "deep/inner/Sets/s1/n")

	deepCompressed, err := tt.deepCompressed.Get(ctx, cli)
	if err != nil {
		t.Fatalf("deepCompressed.Get() error = %v", err)
	}

	if !reflect.DeepEqual(deepCompressed, modified.Deep.Inner.Compressed) {
		t.Fatalf("deepCompressed.Get() = %v, want %v", deepCompressed, modified.Deep.Inner.Compressed)
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
		Tags:       []string{"a", "b", "c"},
		Sets:       map[string][]string{"team1": {"a", "b"}},
		Compressed: []string{"x", "y"},
		Deep: trieTestDeep{Inner: trieTestInner{
			Name:       "n1",
			Tags:       []string{"p", "q"},
			Sets:       map[string][]string{"s1": {"m"}},
			Compressed: []string{"k1", "k2"},
		}},
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
		Tags:       []string{"c", "a", "d"},
		Sets:       map[string][]string{"team1": {"b", "c"}, "team2": {"z"}},
		Compressed: []string{"x", "z"},
		Deep: trieTestDeep{Inner: trieTestInner{
			Name:       "n2",
			Tags:       []string{"q", "r"},
			Sets:       map[string][]string{"s1": {"m", "n"}},
			Compressed: []string{"k1"},
		}},
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

// wantChange is a change a plan is expected to hold, Value is JSON
type wantChange struct {
	Type  paths.ChangeType
	Key   string
	Value string
}

func added(key, value string) wantChange   { return wantChange{paths.Add, key, value} }
func removed(key, value string) wantChange { return wantChange{paths.Remove, key, value} }

// assertChanges checks the changes in a plan, in any order
func assertChanges(t *testing.T, got []paths.Change, want ...wantChange) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("got %d changes, want %d: %+v", len(got), len(want), got)
	}

	got = slices.Clone(got)
	slices.SortFunc(got, func(a, b paths.Change) int { return strings.Compare(a.Key, b.Key) })
	slices.SortFunc(want, func(a, b wantChange) int { return strings.Compare(a.Key, b.Key) })

	for i := range want {
		if got[i].Type != want[i].Type || got[i].Key != want[i].Key {
			t.Fatalf("change %d = %v %q, want %v %q", i, got[i].Type, got[i].Key, want[i].Type, want[i].Key)
		}

		assertJSONEqual(t, got[i].Value, want[i].Value)
	}
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

// seedKeys stores an array as it is in the store, a key under prefix for each element with no value
func seedKeys(t *testing.T, ctx context.Context, cli *clientv3.Client, prefix string, elements []string) {
	t.Helper()

	for _, element := range elements {
		if _, err := cli.Put(ctx, prefix+"/"+element, ""); err != nil {
			t.Fatalf("seeding %q: %v", prefix+"/"+element, err)
		}
	}
}

// assertKeys checks that the keys under prefix are exactly want
func assertKeys(t *testing.T, ctx context.Context, cli *clientv3.Client, prefix string, want ...string) {
	t.Helper()

	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		t.Fatalf("listing keys under %q: %v", prefix, err)
	}

	got := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		got = append(got, string(kv.Key))
	}

	slices.Sort(got)
	slices.Sort(want)

	if !slices.Equal(got, want) {
		t.Fatalf("keys under %q = %v, want %v", prefix, got, want)
	}
}
