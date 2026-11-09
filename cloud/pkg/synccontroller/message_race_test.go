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

package synccontroller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kubeedge/api/apis/reliablesyncs/v1alpha1"
	"github.com/kubeedge/beehive/pkg/common"
	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	commonhub "github.com/kubeedge/kubeedge/cloud/pkg/cloudhub/common"
	tf "github.com/kubeedge/kubeedge/cloud/pkg/cloudhub/common/testing"
	"github.com/kubeedge/kubeedge/cloud/pkg/common/modules"
	"github.com/kubeedge/kubeedge/pkg/metaserver/util"
)

// TestSendClusterObjectSyncEventContentIsolated is the race oracle for the
// 2026-11-05 cloudcore panic (fatal error: concurrent map read and map write).
//
// It runs the real production path sendClusterObjectSyncEvent -> buildEdgeControllerMessage
// -> FillBody against the real reader path of the crash stack
// (DispatchDownstream -> enqueueAckMessage -> isDeleteMessage ->
// GetMessageDeletionTimestamp -> unstructured.NestedFieldNoCopy).
//
// ORACLE REQUIREMENT: the shared object is created ONCE and the very same
// pointer is passed to every producer call. If a fresh object were created per
// iteration, the happens-before edges of the channel hand-off would fully
// order the reads and writes and -race would stay green even without the fix.
//
// Run with: go test -race -gcflags "all=-N -l" -count=1 ./cloud/pkg/synccontroller/...
// Expected: race report before the fix, clean pass after.
func TestSendClusterObjectSyncEventContentIsolated(t *testing.T) {
	backup := setupTest()
	defer backup.restore()

	const iterations = 2000

	shared := createTestPod("race-pod", "race-uid", "1000")
	sync := createTestSync("node1-pod-12345", "race-pod", "Pod", "v1", "500")

	msgs := make(chan model.Message, iterations+16)
	sendToEdge = func(_ string, msg model.Message) { msgs <- msg }

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < iterations; i++ {
			sendClusterObjectSyncEvent("node1", sync, "pod", "1000", shared)
		}
	}()

drain:
	for {
		select {
		case msg := <-msgs:
			if _, err := commonhub.GetMessageDeletionTimestamp(&msg); err != nil {
				t.Fatalf("GetMessageDeletionTimestamp: %v", err)
			}
			if _, err := commonhub.GetMessageUID(msg); err != nil {
				t.Fatalf("GetMessageUID: %v", err)
			}
		case <-done:
			break drain
		}
	}
	// Drain leftovers handed over before close (no race: producer is done).
	for len(msgs) > 0 {
		msg := <-msgs
		if _, err := commonhub.GetMessageDeletionTimestamp(&msg); err != nil {
			t.Fatalf("GetMessageDeletionTimestamp: %v", err)
		}
	}
}

// TestBuildEdgeControllerMessageCopiesAndStampsGVK asserts the ownership
// invariant: the message body must be a private deep copy carrying the GVK,
// and the input (informer cache) object must never be mutated.
func TestBuildEdgeControllerMessageCopiesAndStampsGVK(t *testing.T) {
	// Informer cache objects of built-in resources carry an empty TypeMeta.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "pod-copy",
			Namespace:       "default",
			UID:             "uid-copy",
			ResourceVersion: "1000",
		},
	}

	msg := buildEdgeControllerMessage("node1", "default", "pod", "pod-copy", model.UpdateOperation, pod)
	if msg == nil {
		t.Fatal("expected a message, got nil")
	}

	body, ok := msg.Content.(runtime.Object)
	if !ok {
		t.Fatalf("message content is not a runtime.Object: %T", msg.Content)
	}
	if body == runtime.Object(pod) {
		t.Error("message content shares the informer cache object: expected a private deep copy")
	}

	gvk := body.GetObjectKind().GroupVersionKind()
	if gvk.Group != "" || gvk.Version != "v1" || gvk.Kind != "Pod" {
		t.Errorf("message body GVK = %q, want v1/Pod", gvk)
	}

	if pod.APIVersion != "" || pod.Kind != "" {
		t.Errorf("input cache object was mutated: apiVersion=%q kind=%q", pod.APIVersion, pod.Kind)
	}

	if msg.GetResourceVersion() != "1000" {
		t.Errorf("message resourceVersion = %q, want 1000", msg.GetResourceVersion())
	}
}

// TestSendEventsBodyCarriesGVK covers the namespaced path end to end: the
// edge decodes unstructured bodies through their in-body apiVersion/kind, so
// sync messages must carry the GVK (the pre-fix namespaced path never stamped
// it, which made the edge silently drop such objects).
func TestSendEventsBodyCarriesGVK(t *testing.T) {
	cloudHub := &common.ModuleInfo{
		ModuleName: modules.CloudHubModuleName,
		ModuleType: common.MsgCtxTypeChannel,
	}
	beehiveContext.InitContext([]string{common.MsgCtxTypeChannel})
	beehiveContext.AddModule(cloudHub)
	beehiveContext.AddModuleGroup(modules.CloudHubModuleName, modules.CloudHubModuleGroup)

	sync := &v1alpha1.ObjectSync{
		ObjectMeta: metav1.ObjectMeta{Name: "node1.uid-1", Namespace: tf.TestNamespace},
		Spec: v1alpha1.ObjectSyncSpec{
			ObjectKind:       "Pod",
			ObjectAPIVersion: "v1",
			ObjectName:       tf.TestPodName,
		},
		Status: v1alpha1.ObjectSyncStatus{ObjectResourceVersion: "1"},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            tf.TestPodName,
			Namespace:       tf.TestNamespace,
			UID:             types.UID(tf.TestPodUID),
			ResourceVersion: "2",
		},
	}

	go sendEvents(tf.TestNodeID, sync, "pod", "2", pod)

	message, err := beehiveContext.Receive(modules.CloudHubModuleName)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}

	body, ok := message.Content.(runtime.Object)
	if !ok {
		t.Fatalf("message content is not a runtime.Object: %T", message.Content)
	}
	gvk := body.GetObjectKind().GroupVersionKind()
	if gvk.Version != "v1" || gvk.Kind != "Pod" {
		t.Errorf("namespaced sync message body missing GVK: got %q, want v1/Pod", gvk)
	}
}

// TestGCOrphanedObjectSyncDeleteStubCarriesGVK: the gc delete stub is decoded
// by the edge the same way; without apiVersion/kind the delete is silently
// lost at the edge.
func TestGCOrphanedObjectSyncDeleteStubCarriesGVK(t *testing.T) {
	cloudHub := &common.ModuleInfo{
		ModuleName: modules.CloudHubModuleName,
		ModuleType: common.MsgCtxTypeChannel,
	}
	beehiveContext.InitContext([]string{common.MsgCtxTypeChannel})
	beehiveContext.AddModule(cloudHub)
	beehiveContext.AddModuleGroup(modules.CloudHubModuleName, modules.CloudHubModuleGroup)

	sync := tf.NewObjectSync(tf.NewTestPodResource(tf.TestPodName, tf.TestPodUID, "1"), "Pod")
	if sync.Spec.ObjectAPIVersion == "" {
		sync.Spec.ObjectAPIVersion = "v1"
	}

	go func() {
		(&SyncController{}).gcOrphanedObjectSync(sync)
	}()

	message, err := beehiveContext.Receive(modules.CloudHubModuleName)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}

	if got := util.GetMessageAPIVersion(&message); got != sync.Spec.ObjectAPIVersion {
		t.Errorf("gc delete message apiVersion = %q, want %q", got, sync.Spec.ObjectAPIVersion)
	}
	if got := util.GetMessageResourceType(&message); got != sync.Spec.ObjectKind {
		t.Errorf("gc delete message kind = %q, want %q", got, sync.Spec.ObjectKind)
	}
}

// TestGCOrphanedClusterObjectSyncDeleteStubCarriesGVK is the cluster-scoped
// counterpart of the test above. It deliberately does NOT stub
// buildEdgeControllerMessageFunc: the existing cluster gc tests stub the
// builder to an empty message, so the real FillBody path is never covered.
func TestGCOrphanedClusterObjectSyncDeleteStubCarriesGVK(t *testing.T) {
	backup := setupTest()
	defer backup.restore()

	sync := createTestSync("node1-pod-12345", "race-pod", "Pod", "v1", "500")

	msgs := make(chan model.Message, 1)
	sendToEdge = func(_ string, msg model.Message) { msgs <- msg }

	gcOrphanedClusterObjectSyncFunc(&SyncController{}, sync)

	select {
	case msg := <-msgs:
		if got := util.GetMessageAPIVersion(&msg); got != sync.Spec.ObjectAPIVersion {
			t.Errorf("gc delete message apiVersion = %q, want %q", got, sync.Spec.ObjectAPIVersion)
		}
		if got := util.GetMessageResourceType(&msg); got != sync.Spec.ObjectKind {
			t.Errorf("gc delete message kind = %q, want %q", got, sync.Spec.ObjectKind)
		}
	default:
		t.Fatal("no delete message captured")
	}
}
