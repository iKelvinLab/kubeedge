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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/kubeedge/beehive/pkg/core/model"
)

// newLeaseUpstreamController builds a minimal UpstreamController that only
// runs the createOrUpdateLease loop, backed by the given fake clientset.
func newLeaseUpstreamController(t *testing.T, kubeClient *fake.Clientset) (*UpstreamController, *MockMessageLayer) {
	t.Helper()

	layer := &MockMessageLayer{}
	uc := &UpstreamController{
		kubeClient:      kubeClient,
		messageLayer:    layer,
		createLeaseChan: make(chan model.Message, 5),
	}
	go uc.createOrUpdateLease()
	return uc, layer
}

// sendLeaseUpdateMessage pushes an update-op lease message onto the
// createLeaseChan and waits until exactly one response has been relayed
// through the message layer.
func sendLeaseUpdateMessage(t *testing.T, uc *UpstreamController, layer *MockMessageLayer, lease *coordinationv1.Lease) {
	t.Helper()

	leaseData, err := json.Marshal(lease)
	require.NoError(t, err)

	nodeID := defaultNodeID
	resource := fmt.Sprintf("node/%s/%s/%s/%s", nodeID, lease.Namespace, model.ResourceTypeLease, lease.Name)
	msg := model.Message{
		Header: model.MessageHeader{ID: "test-update-lease"},
		Router: model.MessageRoute{
			Resource:  resource,
			Operation: model.UpdateOperation,
		},
		Content: string(leaseData),
	}
	uc.createLeaseChan <- msg

	require.Eventually(t, func() bool {
		return len(layer.ResponseMessages) == 1
	}, 5*time.Second, 10*time.Millisecond, "timed out waiting for the lease update response")
}

// leaseObjectResp decodes the relayed ObjectResp of the first response.
// ObjectResp.Err is an error interface, so an apierrors.StatusError travels
// on the wire as {"ErrStatus": {...}}.
func leaseObjectResp(t *testing.T, msg model.Message) (*coordinationv1.Lease, error) {
	t.Helper()

	data, err := msg.GetContentData()
	require.NoError(t, err)

	var resp struct {
		Object *coordinationv1.Lease
		Err    *struct {
			ErrStatus metav1.Status
		}
	}
	require.NoError(t, json.Unmarshal(data, &resp))
	if resp.Err != nil && len(resp.Err.ErrStatus.Message) > 0 {
		return resp.Object, fmt.Errorf("%s", resp.Err.ErrStatus.Message)
	}
	return resp.Object, nil
}

// TestUpstreamLeaseRenewUsesMergePatch verifies that a renewal (lease carrying
// renewTime) is relayed to the apiserver as a JSON Merge Patch on spec.renewTime
// only, with no resourceVersion involved at all.
func TestUpstreamLeaseRenewUsesMergePatch(t *testing.T) {
	now := metav1.NewMicroTime(time.Now())
	namespace := defaultNamespace
	leaseName := "patch-lease"

	seeded := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: namespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ToString("seeded-holder"),
			LeaseDurationSeconds: ToInt32(40),
		},
	}
	kubeClient := fake.NewSimpleClientset(seeded)
	uc, layer := newLeaseUpstreamController(t, kubeClient)

	// The edge may carry a stale resourceVersion; the patch must not depend
	// on it in any way.
	incoming := seeded.DeepCopy()
	incoming.ResourceVersion = "100"
	incoming.Spec.RenewTime = &now

	sendLeaseUpdateMessage(t, uc, layer, incoming)

	var patchAction k8stesting.PatchAction
	patchCount := 0
	for _, action := range kubeClient.Actions() {
		switch a := action.(type) {
		case k8stesting.PatchAction:
			patchCount++
			patchAction = a
		default:
			t.Fatalf("unexpected action %v for a renew-only lease update", action)
		}
	}
	require.Equal(t, 1, patchCount, "renewal must be relayed as exactly one patch")
	require.Equal(t, types.MergePatchType, patchAction.GetPatchType())

	var patchBody map[string]interface{}
	require.NoError(t, json.Unmarshal(patchAction.GetPatch(), &patchBody))
	assert.NotContains(t, patchBody, "resourceVersion", "patch must not carry resourceVersion")
	assert.NotContains(t, patchBody, "metadata", "patch must not touch metadata")
	spec, ok := patchBody["spec"].(map[string]interface{})
	require.True(t, ok, "patch body must contain spec.renewTime: %v", patchBody)
	assert.Contains(t, spec, "renewTime")
	assert.Len(t, spec, 1, "patch must only touch spec.renewTime: %v", spec)

	// The relayed response must carry the latest lease object error-free so
	// the edge can advance its in-memory latestLease.
	obj, err := leaseObjectResp(t, layer.ResponseMessages[0])
	assert.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, leaseName, obj.Name)
	assert.NotNil(t, obj.Spec.RenewTime, "renewed lease must carry a renewTime")
	assert.Equal(t, "seeded-holder", *obj.Spec.HolderIdentity, "renewal must not clobber other spec fields")

	// The apiserver-side object must be renewed as well.
	stored, getErr := kubeClient.CoordinationV1().Leases(namespace).Get(context.TODO(), leaseName, metav1.GetOptions{})
	require.NoError(t, getErr)
	assert.NotNil(t, stored.Spec.RenewTime)
}

// TestUpstreamLeaseUpdateWithoutRenewTimeKeepsUpdate verifies the fallback:
// an update that carries no renewTime still goes through the full Update
// path (replace semantics).
func TestUpstreamLeaseUpdateWithoutRenewTimeKeepsUpdate(t *testing.T) {
	namespace := defaultNamespace
	leaseName := "update-lease"

	seeded := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: namespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ToString("old-holder"),
			LeaseDurationSeconds: ToInt32(40),
		},
	}
	kubeClient := fake.NewSimpleClientset(seeded)
	uc, layer := newLeaseUpstreamController(t, kubeClient)

	incoming := seeded.DeepCopy()
	incoming.Spec.HolderIdentity = ToString("new-holder")

	sendLeaseUpdateMessage(t, uc, layer, incoming)

	updateCount := 0
	for _, action := range kubeClient.Actions() {
		switch a := action.(type) {
		case k8stesting.UpdateAction:
			updateCount++
		case k8stesting.PatchAction:
			t.Fatalf("unexpected patch action %v for a renew-less lease update", a)
		}
	}
	require.Equal(t, 1, updateCount)

	obj, err := leaseObjectResp(t, layer.ResponseMessages[0])
	assert.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, "new-holder", *obj.Spec.HolderIdentity)
}

// TestUpstreamLeaseConflictRetrySuccess verifies the LAN-local conflict
// absorption: an Update hitting 409 is followed by one re-GET and a single
// retry, and the edge receives an error-free response.
func TestUpstreamLeaseConflictRetrySuccess(t *testing.T) {
	namespace := defaultNamespace
	leaseName := "conflict-lease"

	seeded := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: namespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ToString("old-holder"),
			LeaseDurationSeconds: ToInt32(40),
		},
	}
	kubeClient := fake.NewSimpleClientset(seeded)

	updateCalls := 0
	kubeClient.PrependReactor("update", "leases", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updateCalls++
		if updateCalls == 1 {
			return true, nil, apierrors.NewConflict(
				schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"},
				leaseName, fmt.Errorf("the object has been modified"))
		}
		return false, nil, nil
	})

	uc, layer := newLeaseUpstreamController(t, kubeClient)

	incoming := seeded.DeepCopy()
	incoming.Spec.HolderIdentity = ToString("new-holder")

	sendLeaseUpdateMessage(t, uc, layer, incoming)

	// Action sequence must be: update(409) -> get -> update(retry).
	var kinds []string
	for _, action := range kubeClient.Actions() {
		kinds = append(kinds, action.GetVerb())
	}
	assert.Equal(t, []string{"update", "get", "update"}, kinds)

	obj, err := leaseObjectResp(t, layer.ResponseMessages[0])
	assert.NoError(t, err, "conflict must be absorbed in the cloud, edge must not see it")
	require.NotNil(t, obj)
	assert.Equal(t, "new-holder", *obj.Spec.HolderIdentity)
}

// TestUpstreamLeaseConflictRetryGetFails verifies that when the re-GET after
// a conflict fails, the error is still relayed back to the edge.
func TestUpstreamLeaseConflictRetryGetFails(t *testing.T) {
	namespace := defaultNamespace
	leaseName := "conflict-get-fail-lease"

	seeded := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: namespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: ToString("old-holder"),
		},
	}
	kubeClient := fake.NewSimpleClientset(seeded)

	kubeClient.PrependReactor("update", "leases", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(
			schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"},
			leaseName, fmt.Errorf("the object has been modified"))
	})
	kubeClient.PrependReactor("get", "leases", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(
			schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, leaseName)
	})

	uc, layer := newLeaseUpstreamController(t, kubeClient)

	incoming := seeded.DeepCopy()
	incoming.Spec.HolderIdentity = ToString("new-holder")

	sendLeaseUpdateMessage(t, uc, layer, incoming)

	var kinds []string
	for _, action := range kubeClient.Actions() {
		kinds = append(kinds, action.GetVerb())
	}
	// update(409) -> get(fails); no retry update must happen.
	assert.Equal(t, []string{"update", "get"}, kinds)

	_, err := leaseObjectResp(t, layer.ResponseMessages[0])
	assert.Error(t, err, "re-GET failure must be relayed to the edge")
}
