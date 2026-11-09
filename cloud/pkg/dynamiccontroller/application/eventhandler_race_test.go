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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

// TestObjToEventRace is the race oracle for the dynamiccontroller write side
// of the 2026-11-05 cloudcore panic: objToEvent stamps the GVK on the shared
// informer cache object in place, racing with any concurrent reader of the
// same object.
//
// ORACLE REQUIREMENT (same as TestSendClusterObjectSyncEventContentIsolated):
// the shared object is created once and the same pointer is passed to every
// call, so the writer keeps mutating an object the reader may still hold.
//
// Run with: go test -race -gcflags "all=-N -l" -count=1 ./cloud/pkg/dynamiccontroller/application/...
// Expected: race report before the fix, clean pass after.
func TestObjToEventRace(t *testing.T) {
	handler := &CommonResourceEventHandler{events: make(chan watch.Event, 2048)}

	shared := createTestObject("race-obj", map[string]string{"app": "race"})

	const iterations = 2000
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < iterations; i++ {
			handler.objToEvent(watch.Modified, shared)
		}
	}()

drain:
	for {
		select {
		case ev := <-handler.events:
			readObjectMeta(t, ev.Object)
		case <-done:
			break drain
		}
	}
	for len(handler.events) > 0 {
		ev := <-handler.events
		readObjectMeta(t, ev.Object)
	}
}

// readObjectMeta performs the same unstructured map reads as the crash-stack
// reader path (meta.Accessor alone only type-asserts; the getters are what
// actually read the map).
func readObjectMeta(t *testing.T, obj runtime.Object) {
	t.Helper()
	accessor, err := meta.Accessor(obj)
	if err != nil {
		t.Fatalf("meta.Accessor: %v", err)
	}
	accessor.GetDeletionTimestamp() // unstructured -> NestedFieldNoCopy map read
	accessor.GetUID()
	accessor.GetName()
}

// TestObjToEventSendsCopyWithGVK asserts the ownership invariant for the
// watch path: the event must carry a private copy with the GVK set, and the
// input (shared cache) object must not be mutated.
func TestObjToEventSendsCopyWithGVK(t *testing.T) {
	t.Run("unstructured object", func(t *testing.T) {
		handler := &CommonResourceEventHandler{events: make(chan watch.Event, 1)}
		obj := createTestObject("copy-obj", map[string]string{"app": "copy"})

		handler.objToEvent(watch.Added, obj)

		select {
		case ev := <-handler.events:
			if ev.Object == runtime.Object(obj) {
				t.Error("event carries the shared cache object: expected a private deep copy")
			}
			gvk := ev.Object.GetObjectKind().GroupVersionKind()
			if gvk.Group != "apps" || gvk.Version != "v1" || gvk.Kind != "Deployment" {
				t.Errorf("event object GVK = %q, want apps/v1 Deployment", gvk)
			}
		case <-time.After(time.Second):
			t.Fatal("no event received")
		}
	})

	t.Run("typed object with empty TypeMeta", func(t *testing.T) {
		handler := &CommonResourceEventHandler{events: make(chan watch.Event, 1)}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "typed-pod", Namespace: "default"},
		}

		handler.objToEvent(watch.Added, pod)

		select {
		case ev := <-handler.events:
			if ev.Object == runtime.Object(pod) {
				t.Error("event carries the shared cache object: expected a private deep copy")
			}
			gvk := ev.Object.GetObjectKind().GroupVersionKind()
			if gvk.Version != "v1" || gvk.Kind != "Pod" {
				t.Errorf("event object GVK = %q, want v1/Pod", gvk)
			}
			if pod.APIVersion != "" || pod.Kind != "" {
				t.Errorf("input cache object was mutated: apiVersion=%q kind=%q", pod.APIVersion, pod.Kind)
			}
		case <-time.After(time.Second):
			t.Fatal("no event received")
		}
	})
}
