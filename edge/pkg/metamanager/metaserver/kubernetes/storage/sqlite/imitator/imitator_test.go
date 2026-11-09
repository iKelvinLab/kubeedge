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

package imitator

import (
	"context"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	edgecorev1alpha2 "github.com/kubeedge/api/apis/componentconfig/edgecore/v1alpha2"
	"github.com/kubeedge/beehive/pkg/core/model"

	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao"
)

// TestInjectSurfacesDecodeFailure pins the contract that a message which
// cannot be decoded into an unstructured object (e.g. a delete stub without
// apiVersion/kind) makes Inject return an error instead of being silently
// dropped. Before this contract existed, the metamanager still answered OK,
// cloudhub garbage-collected the ObjectSync accounting, and the meta_v2 row
// became a permanent orphan (fix topic edge-cache-delete-consistency G1/G2).
func TestInjectSurfacesDecodeFailure(t *testing.T) {
	body := map[string]interface{}{
		"metadata": map[string]interface{}{"name": "32212"},
	}
	msg := model.NewMessage("").
		BuildRouter("edgecontroller", "resource", "default/ciliumidentities/32212", model.DeleteOperation).
		FillBody(body)

	if err := DefaultV2Client.Inject(*msg); err == nil {
		t.Errorf("Inject() with GVK-less delete body: got nil error, want decode error")
	}
}

// TestEventSkipsStatusAndLease ensures the skip path is a legitimate empty
// result, not an error, so error surfacing does not break these messages.
func TestEventSkipsStatusAndLease(t *testing.T) {
	s := &imitator{}
	msg := model.NewMessage("").
		BuildRouter("edgecontroller", "resource", "default/nodestatus/node1", model.UpdateOperation).
		FillBody(map[string]interface{}{})

	events, err := s.Event(msg)
	if err != nil {
		t.Errorf("Event() skip path: got error %v, want nil", err)
	}
	if len(events) != 0 {
		t.Errorf("Event() skip path: got %d events, want 0", len(events))
	}
}

// TestInjectDeleteWithGVKRemovesRow verifies the happy path end to end: a
// well-formed delete message removes the meta_v2 row and returns no error.
func TestInjectDeleteWithGVKRemovesRow(t *testing.T) {
	dao.Init(filepath.Join(t.TempDir(), "meta.db"), &edgecorev1alpha2.MetaManager{Enable: true})

	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "cilium.io/v2",
		"kind":       "CiliumIdentity",
		"metadata": map[string]interface{}{
			"name":      "32212",
			"namespace": "default",
		},
	}}
	key := "/cilium.io/v2/ciliumidentities/default/32212"

	if err := DefaultV2Client.InsertOrUpdateObj(context.TODO(), obj); err != nil {
		t.Fatalf("InsertOrUpdateObj() failed: %v", err)
	}
	if _, err := DefaultV2Client.Get(context.TODO(), key); err != nil {
		t.Fatalf("row not present before delete: %v", err)
	}

	msg := model.NewMessage("").
		BuildRouter("edgecontroller", "resource", "default/ciliumidentities/32212", model.DeleteOperation).
		FillBody(obj.Object)
	if err := DefaultV2Client.Inject(*msg); err != nil {
		t.Fatalf("Inject() delete: got error %v, want nil", err)
	}
	if _, err := DefaultV2Client.Get(context.TODO(), key); err == nil {
		t.Fatalf("row still present after delete inject")
	}
}
