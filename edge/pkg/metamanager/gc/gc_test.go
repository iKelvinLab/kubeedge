/*
Copyright 2026 The KubeEdge Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao/models"
)

var testGVR = schema.GroupVersionResource{Group: "cilium.io", Version: "v2", Resource: "ciliumidentities"}

func identityRow(name, uid string) models.MetaV2 {
	return models.MetaV2{
		Key:                  fmt.Sprintf("/cilium.io/v2/ciliumidentities/null/%s", name),
		GroupVersionResource: testGVR.String(),
		Namespace:            models.NullNamespace,
		Name:                 name,
		ResourceVersion:      100,
		Value: fmt.Sprintf(`{"apiVersion":"cilium.io/v2","kind":"CiliumIdentity",`+
			`"metadata":{"name":"%s","uid":"%s"}}`, name, uid),
	}
}

func cloudItem(name string) unstructured.Unstructured {
	return unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "cilium.io/v2",
		"kind":       "CiliumIdentity",
		"metadata":   map[string]interface{}{"name": name},
	}}
}

type fakeLister struct {
	list *unstructured.UnstructuredList
	err  error
}

func (f fakeLister) ListCloud(_ context.Context, _ schema.GroupVersionResource) (*unstructured.UnstructuredList, error) {
	return f.list, f.err
}

type fakeRowStore struct {
	gvrs    []schema.GroupVersionResource
	rows    map[schema.GroupVersionResource][]models.MetaV2
	deleted []string
}

func (f *fakeRowStore) ListGVRs() ([]schema.GroupVersionResource, error) {
	return f.gvrs, nil
}

func (f *fakeRowStore) ListRows(gvr schema.GroupVersionResource) ([]models.MetaV2, error) {
	return f.rows[gvr], nil
}

func (f *fakeRowStore) DeleteRow(_ context.Context, key string) error {
	f.deleted = append(f.deleted, key)
	return nil
}

func newHarness(t *testing.T, lister CloudLister, rows *fakeRowStore, dryRun bool) (*GC, *[]watch.Event) {
	t.Helper()
	events := make([]watch.Event, 0)
	g := NewGC(lister, rows, dryRun, func(e watch.Event) { events = append(events, e) })
	return g, &events
}

// TestReconcileEvictsOrphanRowWithDeletedEvent: rows whose object is gone
// upstream are deleted and a Deleted event with full GVK is synthesized.
func TestReconcileEvictsOrphanRowWithDeletedEvent(t *testing.T) {
	rows := &fakeRowStore{
		gvrs: []schema.GroupVersionResource{testGVR},
		rows: map[schema.GroupVersionResource][]models.MetaV2{
			testGVR: {identityRow("alive", "uid-a"), identityRow("orphan", "uid-o")},
		},
	}
	lister := fakeLister{list: &unstructured.UnstructuredList{Items: []unstructured.Unstructured{cloudItem("alive")}}}
	g, events := newHarness(t, lister, rows, false)

	g.ReconcileOnce(context.TODO())

	if len(rows.deleted) != 1 || rows.deleted[0] != identityRow("orphan", "uid-o").Key {
		t.Fatalf("deleted rows = %v, want exactly the orphan row", rows.deleted)
	}
	if len(*events) != 1 {
		t.Fatalf("synthesized events = %d, want 1", len(*events))
	}
	ev := (*events)[0]
	if ev.Type != watch.Deleted {
		t.Errorf("event type = %v, want Deleted", ev.Type)
	}
	obj := ev.Object.(*unstructured.Unstructured)
	if obj.GetKind() != "CiliumIdentity" || obj.GetName() != "orphan" {
		t.Errorf("event object = %v/%v, want CiliumIdentity/orphan", obj.GetKind(), obj.GetName())
	}
}

// TestReconcileSkipsOnListError: a list failure must never be interpreted as
// an empty cloud (would wipe live rows).
func TestReconcileSkipsOnListError(t *testing.T) {
	rows := &fakeRowStore{
		gvrs: []schema.GroupVersionResource{testGVR},
		rows: map[schema.GroupVersionResource][]models.MetaV2{
			testGVR: {identityRow("x", "uid-x")},
		},
	}
	lister := fakeLister{err: apierrors.NewForbidden(testGVR.GroupResource(), "", errors.New("rbac"))}
	g, _ := newHarness(t, lister, rows, false)

	g.ReconcileOnce(context.TODO())

	if len(rows.deleted) != 0 {
		t.Fatalf("deleted rows = %v, want none on list error", rows.deleted)
	}
}

// TestGVRNotFoundRequiresConfirmation: whole-type 404 wipes local rows only
// after consecutive confirmations.
func TestGVRNotFoundRequiresConfirmation(t *testing.T) {
	rows := &fakeRowStore{
		gvrs: []schema.GroupVersionResource{testGVR},
		rows: map[schema.GroupVersionResource][]models.MetaV2{
			testGVR: {identityRow("x", "uid-x")},
		},
	}
	lister := fakeLister{err: apierrors.NewNotFound(testGVR.GroupResource(), "")}
	g, _ := newHarness(t, lister, rows, false)

	g.ReconcileOnce(context.TODO())
	if len(rows.deleted) != 0 {
		t.Fatalf("first 404 round deleted %v, want none", rows.deleted)
	}

	g.ReconcileOnce(context.TODO())
	if len(rows.deleted) != 1 {
		t.Fatalf("second 404 round deleted %v, want the row", rows.deleted)
	}
}

// TestDryRunDeletesNothing.
func TestDryRunDeletesNothing(t *testing.T) {
	rows := &fakeRowStore{
		gvrs: []schema.GroupVersionResource{testGVR},
		rows: map[schema.GroupVersionResource][]models.MetaV2{
			testGVR: {identityRow("orphan", "uid-o")},
		},
	}
	lister := fakeLister{list: &unstructured.UnstructuredList{}}
	g, events := newHarness(t, lister, rows, true)

	g.ReconcileOnce(context.TODO())

	if len(rows.deleted) != 0 || len(*events) != 0 {
		t.Fatalf("dry-run deleted %v events %d, want none", rows.deleted, len(*events))
	}
}

// TestClusterScopedNamespaceNormalization: meta_v2 stores cluster-scoped
// rows with the "null" namespace sentinel; cloud items report "" — they must
// match so live cluster-scoped rows are not evicted.
func TestClusterScopedNamespaceNormalization(t *testing.T) {
	rows := &fakeRowStore{
		gvrs: []schema.GroupVersionResource{testGVR},
		rows: map[schema.GroupVersionResource][]models.MetaV2{
			testGVR: {identityRow("alive", "uid-a")},
		},
	}
	lister := fakeLister{list: &unstructured.UnstructuredList{Items: []unstructured.Unstructured{cloudItem("alive")}}}
	g, _ := newHarness(t, lister, rows, false)

	g.ReconcileOnce(context.TODO())

	if len(rows.deleted) != 0 {
		t.Fatalf("cluster-scoped live row deleted: %v", rows.deleted)
	}
}

// TestDeletedEventFromRowBadValue: undecodable rows are still deleted but
// yield no watch event.
func TestDeletedEventFromRowBadValue(t *testing.T) {
	row := models.MetaV2{Key: "k", Value: "{not json"}
	if _, ok := deletedEventFromRow(row); ok {
		t.Fatalf("bad row value should not produce an event")
	}
	row2 := models.MetaV2{Key: "k", Value: `{"metadata":{"name":"n"}}`}
	if _, ok := deletedEventFromRow(row2); ok {
		t.Fatalf("row without GVK should not produce an event")
	}
}
