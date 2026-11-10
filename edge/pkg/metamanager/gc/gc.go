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

// Package gc implements the edge-side meta_v2 reconciliation against the
// cloud (fix topic edge-cache-delete-consistency, F2): for every GVR present
// in the local meta_v2 store, the cloud is LISTed and local rows whose object
// no longer exists upstream are deleted, with a Deleted watch event
// synthesized so local informers (e.g. cilium's identity cache) converge
// without a restart. This is the backstop for deletes whose reliable-sync
// accounting (ObjectSync) was already garbage-collected — those rows are
// invisible to the cloud-side 5s reconcile.
package gc

import (
	"context"
	"encoding/json"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/klog/v2"

	connect "github.com/kubeedge/kubeedge/edge/pkg/common/cloudconnection"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao/dbclient"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao/models"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/metaserver/agent"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/metaserver/kubernetes/storage/sqlite/imitator"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/metaserver/kubernetes/storage/sqlite/imitator/watchhook"
	"github.com/kubeedge/kubeedge/pkg/metaserver"
)

// gvrMissingConfirmRounds is how many consecutive rounds a GVR must 404 on
// the cloud (resource type itself not found, e.g. CRD deleted) before all its
// local rows are wiped. Protects against a single transient NotFound.
const gvrMissingConfirmRounds = 2

// CloudLister lists all objects of a GVR from the cloud, as seen by this node.
type CloudLister interface {
	ListCloud(ctx context.Context, gvr schema.GroupVersionResource) (*unstructured.UnstructuredList, error)
}

// RowStore is the local meta_v2 access the GC needs (injected for tests).
type RowStore interface {
	ListGVRs() ([]schema.GroupVersionResource, error)
	ListRows(gvr schema.GroupVersionResource) ([]models.MetaV2, error)
	DeleteRow(ctx context.Context, key string) error
}

// GC reconciles the local meta_v2 store against the cloud.
type GC struct {
	lister CloudLister
	rows   RowStore
	dryRun bool
	// notify is called with the synthesized Deleted event; production wires
	// it to watchhook.Trigger.
	notify func(e watch.Event)

	// gvrMissing counts consecutive whole-GVR-404 rounds.
	gvrMissing map[schema.GroupVersionResource]int
}

// NewGC builds a GC over the injected dependencies.
func NewGC(lister CloudLister, rows RowStore, dryRun bool, notify func(e watch.Event)) *GC {
	return &GC{
		lister:     lister,
		rows:       rows,
		dryRun:     dryRun,
		notify:     notify,
		gvrMissing: map[schema.GroupVersionResource]int{},
	}
}

// ReconcileOnce runs one full reconciliation pass.
func (g *GC) ReconcileOnce(ctx context.Context) {
	gvrs, err := g.rows.ListGVRs()
	if err != nil {
		klog.Errorf("[meta_v2 gc] failed to list local GVRs: %v", err)
		return
	}
	for _, gvr := range gvrs {
		g.reconcileGVR(ctx, gvr)
	}
}

func (g *GC) reconcileGVR(ctx context.Context, gvr schema.GroupVersionResource) {
	list, err := g.lister.ListCloud(ctx, gvr)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// The whole resource type is gone upstream (e.g. CRD deleted).
			g.gvrMissing[gvr]++
			if g.gvrMissing[gvr] >= gvrMissingConfirmRounds {
				klog.Infof("[meta_v2 gc] resource %v confirmed missing upstream for %d rounds, wiping local rows",
					gvr, g.gvrMissing[gvr])
				g.wipeGVR(ctx, gvr)
				delete(g.gvrMissing, gvr)
			}
		} else {
			// Transient or authorization error: never interpret as "all
			// gone"; skip this GVR for this round.
			klog.Infof("[meta_v2 gc] skip %v, cloud list failed: %v", gvr, err)
		}
		return
	}
	g.gvrMissing[gvr] = 0

	cloud := map[string]struct{}{}
	for i := range list.Items {
		cloud[objectKey(list.Items[i].GetNamespace(), list.Items[i].GetName())] = struct{}{}
	}

	rows, err := g.rows.ListRows(gvr)
	if err != nil {
		klog.Errorf("[meta_v2 gc] failed to list local rows for %v: %v", gvr, err)
		return
	}
	for _, row := range rows {
		if _, ok := cloud[objectKey(normalizeNamespace(row.Namespace), row.Name)]; ok {
			continue
		}
		g.evictRow(ctx, row)
	}
}

// wipeGVR deletes every local row of a GVR whose type no longer exists.
func (g *GC) wipeGVR(ctx context.Context, gvr schema.GroupVersionResource) {
	rows, err := g.rows.ListRows(gvr)
	if err != nil {
		klog.Errorf("[meta_v2 gc] failed to list local rows for wipe of %v: %v", gvr, err)
		return
	}
	for _, row := range rows {
		g.evictRow(ctx, row)
	}
}

func (g *GC) evictRow(ctx context.Context, row models.MetaV2) {
	if g.dryRun {
		klog.Infof("[meta_v2 gc] dry-run: would delete orphan row %s", row.Key)
		return
	}
	if err := g.rows.DeleteRow(ctx, row.Key); err != nil {
		klog.Errorf("[meta_v2 gc] failed to delete orphan row %s: %v", row.Key, err)
		return
	}
	klog.Infof("[meta_v2 gc] deleted orphan row %s (object missing upstream)", row.Key)
	if event, ok := deletedEventFromRow(row); ok && g.notify != nil {
		g.notify(event)
	}
}

// deletedEventFromRow rebuilds a minimal Deleted watch event from the stored
// object JSON so local informers learn about the eviction. Rows whose value
// cannot be decoded yield no event (the row removal itself still converges
// caches on the next client LIST).
func deletedEventFromRow(row models.MetaV2) (watch.Event, bool) {
	obj := &unstructured.Unstructured{}
	if err := json.Unmarshal([]byte(row.Value), obj); err != nil {
		klog.Warningf("[meta_v2 gc] cannot decode stored row %s for watch event: %v", row.Key, err)
		return watch.Event{}, false
	}
	if obj.GroupVersionKind().Empty() || obj.GetName() == "" {
		klog.Warningf("[meta_v2 gc] stored row %s lacks GVK/name, skip watch event", row.Key)
		return watch.Event{}, false
	}
	return watch.Event{Type: watch.Deleted, Object: obj}, true
}

func objectKey(namespace, name string) string {
	return namespace + "/" + name
}

// normalizeNamespace maps the meta_v2 null-namespace sentinel to the empty
// namespace used by cluster-scoped API objects.
func normalizeNamespace(ns string) string {
	if ns == models.NullNamespace {
		return ""
	}
	return ns
}

// agentCloudLister lists the cloud through the metaserver application
// channel (same path an edge client LIST takes).
type agentCloudLister struct{}

func (agentCloudLister) ListCloud(ctx context.Context, gvr schema.GroupVersionResource) (*unstructured.UnstructuredList, error) {
	prefix := "apis"
	if gvr.Group == "" {
		prefix = "api"
	}
	reqCtx := apirequest.WithRequestInfo(ctx, &apirequest.RequestInfo{
		IsResourceRequest: true,
		APIPrefix:         prefix,
		APIGroup:          gvr.Group,
		APIVersion:        gvr.Version,
		Resource:          gvr.Resource,
	})
	app, err := agent.DefaultAgent.Generate(reqCtx, metaserver.List, metav1.ListOptions{}, nil)
	if err != nil {
		return nil, err
	}
	if err := agent.DefaultAgent.Apply(app); err != nil {
		app.Close()
		return nil, err
	}
	defer app.Close()

	list := &unstructured.UnstructuredList{}
	if err := json.Unmarshal(app.RespBody, list); err != nil {
		return nil, err
	}
	return list, nil
}

// metaV2RowStore is the production RowStore over dao + imitator.
type metaV2RowStore struct{}

func (metaV2RowStore) ListGVRs() ([]schema.GroupVersionResource, error) {
	return dbclient.NewMetaV2Service().ListGVRs()
}

func (metaV2RowStore) ListRows(gvr schema.GroupVersionResource) ([]models.MetaV2, error) {
	rows, err := dbclient.NewMetaV2Service().RawMetaByGVRNN(gvr, "", "")
	if err != nil {
		return nil, err
	}
	return *rows, nil
}

func (metaV2RowStore) DeleteRow(ctx context.Context, key string) error {
	return imitator.DefaultV2Client.Delete(ctx, key)
}

// Run starts the periodic reconciliation until done is closed. interval <= 0
// disables it. The first pass runs immediately.
func Run(done <-chan struct{}, interval time.Duration, dryRun bool) {
	if interval <= 0 {
		return
	}
	g := NewGC(agentCloudLister{}, metaV2RowStore{}, dryRun, watchhook.Trigger)
	klog.Infof("[meta_v2 gc] starting edge cache reconciliation against cloud, interval=%v dryRun=%v", interval, dryRun)
	wait.Until(func() {
		// The first pass races edgehub's reconnect on startup; skip the
		// whole round with a single line instead of one skip log per GVR.
		if !connect.IsConnected() {
			klog.Infof("[meta_v2 gc] cloud connection not ready, skip this round")
			return
		}
		g.ReconcileOnce(context.TODO())
	}, interval, done)
}
