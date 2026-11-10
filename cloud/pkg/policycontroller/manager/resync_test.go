package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	policyv1alpha1 "github.com/kubeedge/api/apis/policy/v1alpha1"
	"github.com/kubeedge/beehive/pkg/core/model"
)

func newResyncTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := policyv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("failed to register policy scheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("failed to register core scheme: %v", err)
	}
	return s
}

func newSaAccessForTest(namespace, name, saName string, nodes []string) *policyv1alpha1.ServiceAccountAccess {
	return &policyv1alpha1.ServiceAccountAccess{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       namespace,
			Name:            name,
			ResourceVersion: "100",
		},
		Spec: policyv1alpha1.AccessSpec{
			ServiceAccount: corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: saName},
			},
		},
		Status: policyv1alpha1.AccessStatus{
			NodeList: nodes,
		},
	}
}

// TestSaAccessResyncAll verifies that the resync loop pushes one update
// message per recorded node for every healthy ServiceAccountAccess and skips
// CRs that are being deleted or have no target nodes.
func TestSaAccessResyncAll(t *testing.T) {
	now := metav1.Now()
	deleting := newSaAccessForTest("kube-system", "deleting", "cilium", []string{"n1"})
	deleting.DeletionTimestamp = &now
	deleting.Finalizers = []string{"example.kubeedge.io/keeper"}
	noNodes := newSaAccessForTest("kube-system", "no-nodes", "cilium", nil)
	healthy := newSaAccessForTest("kube-system", "cilium", "cilium", []string{"node1", "node2"})

	fakeClient := fake.NewClientBuilder().
		WithScheme(newResyncTestScheme(t)).
		WithObjects(healthy, deleting, noNodes).
		Build()
	layer := &recordingMessageLayer{}

	runner := &SaAccessResyncRunner{Reader: fakeClient, MessageLayer: layer}
	runner.resyncAll()

	if got := len(layer.messages); got != 2 {
		t.Fatalf("expected 2 messages (one per node of the healthy CR), got %d: %+v", got, layer.messages)
	}
	wantResources := map[string]bool{
		"node/node1/kube-system/serviceaccountaccess/cilium": true,
		"node/node2/kube-system/serviceaccountaccess/cilium": true,
	}
	for _, msg := range layer.messages {
		if got := msg.GetResource(); !wantResources[got] {
			t.Errorf("unexpected message resource %q", got)
		}
		if got := msg.GetOperation(); got != model.UpdateOperation {
			t.Errorf("message operation = %q, want %q", got, model.UpdateOperation)
		}
		acc, ok := msg.GetContent().(*policyv1alpha1.ServiceAccountAccess)
		if !ok {
			t.Fatalf("message content type = %T, want *policyv1alpha1.ServiceAccountAccess", msg.GetContent())
		}
		if got := acc.GroupVersionKind().Kind; got != "ServiceAccountAccess" {
			t.Errorf("message body GVK kind = %q, want ServiceAccountAccess", got)
		}
		if len(acc.Status.NodeList) != 0 {
			t.Errorf("message body should carry an empty node list, got %v", acc.Status.NodeList)
		}
	}
}

// TestSaAccessResyncNodeListTolerance verifies the resync loop inherits
// send2Edge's behavior for malformed node lists: empty node names are
// rejected by BuildResource (message skipped), duplicates are not deduped.
func TestSaAccessResyncNodeListTolerance(t *testing.T) {
	acc := newSaAccessForTest("kube-system", "cilium", "cilium", []string{"", "node1", "node1", "node2"})

	fakeClient := fake.NewClientBuilder().
		WithScheme(newResyncTestScheme(t)).
		WithObjects(acc).
		Build()
	layer := &recordingMessageLayer{}

	runner := &SaAccessResyncRunner{Reader: fakeClient, MessageLayer: layer}
	runner.resyncAll()

	// The empty node name fails BuildResource; the duplicated node1 is sent twice.
	if got := len(layer.messages); got != 3 {
		t.Fatalf("expected 3 messages, got %d", got)
	}
	for _, msg := range layer.messages {
		if got := msg.GetResource(); got == "node//kube-system/serviceaccountaccess/cilium" {
			t.Errorf("message for empty node name should have been skipped, got resource %q", got)
		}
	}
}

// TestSaAccessResyncListFailure verifies a list failure does not panic and
// simply skips the round.
func TestSaAccessResyncListFailure(t *testing.T) {
	// A client whose scheme does not know the type fails every List call.
	emptySchemeClient := fake.NewClientBuilder().WithScheme(runtime.NewScheme()).Build()
	layer := &recordingMessageLayer{}

	runner := &SaAccessResyncRunner{Reader: emptySchemeClient, MessageLayer: layer}
	runner.resyncAll() // must not panic

	if got := len(layer.messages); got != 0 {
		t.Fatalf("expected no messages after a failed list, got %d", got)
	}
}
