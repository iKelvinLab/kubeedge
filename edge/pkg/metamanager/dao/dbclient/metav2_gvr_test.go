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

package dbclient

import (
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"

	edgecorev1alpha2 "github.com/kubeedge/api/apis/componentconfig/edgecore/v1alpha2"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao/models"
)

func TestListGVRs(t *testing.T) {
	dao.Init(filepath.Join(t.TempDir(), "meta.db"), &edgecorev1alpha2.MetaManager{Enable: true})
	svc := NewMetaV2Service()

	grouped := schema.GroupVersionResource{Group: "cilium.io", Version: "v2", Resource: "ciliumidentities"}
	core := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	// Seed with REAL gvr.String() products — the production writer
	// (imitator.InsertOrUpdateObj) stores gvr.String(), whose format is
	// "group/version, Resource=resource" (or "version, Resource=resource"
	// for core). An earlier version of this test hand-wrote slash-separated
	// triplets, matched the buggy parser, and missed the bug entirely.
	seed := []models.MetaV2{
		{Key: "/cilium.io/v2/ciliumidentities/null/1", GroupVersionResource: grouped.String(), Namespace: models.NullNamespace, Name: "1"},
		{Key: "/cilium.io/v2/ciliumidentities/null/2", GroupVersionResource: grouped.String(), Namespace: models.NullNamespace, Name: "2"},
		{Key: "/core/v1/pods/default/p", GroupVersionResource: core.String(), Namespace: "default", Name: "p"},
		// pass-through rows carry no GVR and must be excluded
		{Key: "/version", GroupVersionResource: "", Namespace: "", Name: ""},
		// unparsable GVR strings are skipped, not fatal
		{Key: "/x/y", GroupVersionResource: "garbage", Namespace: "", Name: "x"},
	}
	for i := range seed {
		if err := svc.InsertOrReplaceMetaV2(&seed[i]); err != nil {
			t.Fatalf("seed row %d: %v", i, err)
		}
	}

	gvrs, err := svc.ListGVRs()
	if err != nil {
		t.Fatalf("ListGVRs() error: %v", err)
	}

	want := map[schema.GroupVersionResource]bool{
		grouped: false,
		core:    false,
	}
	for _, gvr := range gvrs {
		if _, ok := want[gvr]; !ok {
			t.Errorf("unexpected GVR %v", gvr)
			continue
		}
		want[gvr] = true
	}
	for gvr, seen := range want {
		if !seen {
			t.Errorf("GVR %v missing from result", gvr)
		}
	}
}
