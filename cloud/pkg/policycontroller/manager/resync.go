package controller

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	policyv1alpha1 "github.com/kubeedge/api/apis/policy/v1alpha1"
	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/cloud/pkg/common/messagelayer"
)

// saAccessResyncPeriod is how often every ServiceAccountAccess is re-pushed
// to the nodes recorded in its status. The event-driven reconcile path only
// sends messages when the cloud-side desired state changes, so an edge node
// that lost its local sa-access row would never receive the record again.
// Periodic re-push closes that gap; repeated messages are absorbed by the
// idempotent edge-side upserts (meta and meta_v2 stores).
const saAccessResyncPeriod = 60 * time.Second

// SaAccessResyncRunner periodically re-pushes every ServiceAccountAccess to
// its recorded nodes, compensating for edge-side record loss.
type SaAccessResyncRunner struct {
	client.Reader
	MessageLayer messagelayer.MessageLayer
}

// Start implements manager.Runnable; the resync loop runs until ctx is done.
func (r *SaAccessResyncRunner) Start(ctx context.Context) error {
	go wait.Until(r.resyncAll, saAccessResyncPeriod, ctx.Done())
	return nil
}

// resyncAll re-pushes the current state of every ServiceAccountAccess to all
// of its recorded nodes. Unlike the reconcile path it does not diff against
// the desired state: the cloud cannot observe edge-side loss, so the push is
// unconditional and relies on edge-side idempotency. CRs being deleted or
// without target nodes are skipped; their reconcile path already handles
// them.
func (r *SaAccessResyncRunner) resyncAll() {
	accList := &policyv1alpha1.ServiceAccountAccessList{}
	if err := r.List(context.TODO(), accList); err != nil {
		klog.Errorf("[sa-access resync] failed to list serviceaccountaccess: %v", err)
		return
	}
	for i := range accList.Items {
		acc := &accList.Items[i]
		if !acc.GetDeletionTimestamp().IsZero() || len(acc.Status.NodeList) == 0 {
			continue
		}
		sendSaAccess2Edge(r.MessageLayer, acc, acc.Status.NodeList, model.UpdateOperation)
		klog.V(4).Infof("[sa-access resync] pushed serviceaccountaccess %s/%s to %d nodes",
			acc.Namespace, acc.Name, len(acc.Status.NodeList))
	}
}
