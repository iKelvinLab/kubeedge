/*
Copyright 2025 The KubeEdge Authors.

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

package application

import (
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/cloud/pkg/common/messagelayer"
)

// captureMessageLayer records the last message sent through sendObj.
type captureMessageLayer struct {
	msg  model.Message
	sent bool
}

func (c *captureMessageLayer) Send(message model.Message) error  { c.msg, c.sent = message, true; return nil }
func (c *captureMessageLayer) Receive() (model.Message, error)   { return model.Message{}, nil }
func (c *captureMessageLayer) Response(message model.Message) error { return nil }

var _ messagelayer.MessageLayer = (*captureMessageLayer)(nil)

func newTestListener(gvr schema.GroupVersionResource) *SelectorListener {
	return &SelectorListener{
		id:       "test-listener",
		nodeName: "node1",
		gvr:      gvr,
		selector: LabelFieldSelector{Label: labels.Everything(), Field: fields.Everything()},
	}
}

// TestSendObjMultiWordKindGVK is the regression test for the canary incident
// where the GVK was stamped from UnsafeResourceToKind: "ciliumidentities"
// title-cases to "Ciliumidentity", which the edge typed clientset fails to
// decode ("no kind Ciliumidentity is registered"). The body must keep the
// object's own GVK for unstructured objects and gain the scheme-derived GVK
// for typed objects - never a heuristically re-cased kind.
func TestSendObjMultiWordKindGVK(t *testing.T) {
	t.Run("unstructured multi-word kind keeps its own GVK", func(t *testing.T) {
		identity := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "cilium.io/v2",
			"kind":       "CiliumIdentity",
			"metadata":   map[string]interface{}{"name": "7013"},
		}}
		layer := &captureMessageLayer{}
		l := newTestListener(schema.GroupVersionResource{Group: "cilium.io", Version: "v2", Resource: "ciliumidentities"})
		l.sendObj(watch.Event{Type: watch.Added, Object: identity}, layer)

		if !layer.sent {
			t.Fatal("no message sent")
		}
		body, ok := layer.msg.Content.(*unstructured.Unstructured)
		if !ok {
			t.Fatalf("content is %T, want *unstructured.Unstructured", layer.msg.Content)
		}
		gvk := body.GetObjectKind().GroupVersionKind()
		if gvk.Group != "cilium.io" || gvk.Version != "v2" || gvk.Kind != "CiliumIdentity" {
			t.Errorf("body GVK = %q, want cilium.io/v2 CiliumIdentity (kind case must be preserved)", gvk)
		}
		if got := identity.GetKind(); got != "CiliumIdentity" {
			t.Errorf("input object kind mutated: %q", got)
		}
	})

	t.Run("typed multi-word kind gets scheme-derived GVK", func(t *testing.T) {
		// informer cache typed objects carry an empty TypeMeta
		role := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "role1"}}
		layer := &captureMessageLayer{}
		l := newTestListener(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "clusterroles"})
		l.sendObj(watch.Event{Type: watch.Added, Object: role}, layer)

		if !layer.sent {
			t.Fatal("no message sent")
		}
		body, ok := layer.msg.Content.(*unstructured.Unstructured)
		if !ok {
			t.Fatalf("content is %T, want *unstructured.Unstructured", layer.msg.Content)
		}
		gvk := body.GetObjectKind().GroupVersionKind()
		// "clusterroles" heuristically re-cases to "Clusterrole" (wrong);
		// the scheme lookup must yield "ClusterRole".
		if gvk.Version != "v1" || gvk.Kind != "ClusterRole" {
			t.Errorf("body GVK = %q, want v1 ClusterRole", gvk)
		}
	})
}
