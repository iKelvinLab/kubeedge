/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

@CHANGELOG
KubeEdge Authors: To make a bridge between kubeclient and metaclient,
This file is derived from K8S client-go code with reduced set of methods
Changes done are
1. Package v1 got some functions from "k8s.io/client-go/kubernetes/typed/coordination/v1/fake/fake_lease.go"
and made some variant
2. KubeEdge Authors: added a consecutive-failure circuit breaker. The vendored
node lease controller retries lease operations in unbounded loops
(backoffEnsureLease has no exit condition); when the underlying cloud session
is half-dead every call just blocks until timeout. The bridge now counts
consecutive failures, asks edgehub to reset the cloud session once the
threshold is reached, and surfaces a distinct circuit-breaker error.
*/

package v1

import (
	"context"
	"errors"
	"fmt"
	"sync"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	typedcoordinationv1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/klog/v2"

	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/edge/pkg/common/message"
	"github.com/kubeedge/kubeedge/edge/pkg/common/modules"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/client"
)

const (
	// maxConsecutiveLeaseFailures is the circuit-breaker threshold for lease
	// operations bridged to the metamanager. Once this many consecutive
	// operations fail, the underlying cloud session is presumed half-dead
	// (e.g. a WAN write stall): edgehub is asked to tear it down and
	// reconnect, possibly landing on another CloudCore replica.
	maxConsecutiveLeaseFailures = 5
)

// ErrLeaseCircuitBreaker is returned by the lease bridge when consecutive
// failures reached maxConsecutiveLeaseFailures and a cloud session reset has
// been requested from edgehub. It lets callers (and tests) distinguish a
// deliberate circuit-break trip from an ordinary request failure.
var ErrLeaseCircuitBreaker = errors.New("lease circuit breaker triggered: consecutive lease failures exceeded threshold, cloud session reset requested")

// LeaseBridge implements LeaseInterface
type LeaseBridge struct {
	typedcoordinationv1.LeaseInterface
	ns         string
	MetaClient client.CoreInterface

	failLock            sync.Mutex
	consecutiveFailures int
}

// recordSuccess resets the consecutive-failure counter.
func (c *LeaseBridge) recordSuccess() {
	c.failLock.Lock()
	defer c.failLock.Unlock()
	c.consecutiveFailures = 0
}

// trackResult maintains the consecutive-failure counter for one lease
// operation. On success the counter is reset. On failure it is incremented;
// once it reaches maxConsecutiveLeaseFailures a cloud session reset is
// requested from edgehub, the counter restarts, and ErrLeaseCircuitBreaker is
// returned on top of the last error. Any other failure is passed through
// unchanged so callers keep seeing the real cause.
func (c *LeaseBridge) trackResult(op string, err error) error {
	if err == nil {
		c.recordSuccess()
		return nil
	}

	c.failLock.Lock()
	c.consecutiveFailures++
	count := c.consecutiveFailures
	if count >= maxConsecutiveLeaseFailures {
		c.consecutiveFailures = 0
	}
	c.failLock.Unlock()

	klog.Warningf("lease %s via metamanager failed (%d/%d consecutive failures): %v",
		op, count, maxConsecutiveLeaseFailures, err)
	if count >= maxConsecutiveLeaseFailures {
		c.requestCloudSessionReset(op)
		return fmt.Errorf("%w: last error: %v", ErrLeaseCircuitBreaker, err)
	}
	return err
}

// requestCloudSessionReset asks edgehub to tear down the current cloud
// session and reconnect. The control message is consumed by edgehub itself
// and never reaches the cloud.
func (c *LeaseBridge) requestCloudSessionReset(op string) {
	klog.Warningf("lease %s failed %d times in a row, requesting edgehub to reset the cloud session",
		op, maxConsecutiveLeaseFailures)
	msg := model.NewMessage("").
		BuildRouter(modules.EdgedModuleName, modules.HubGroup,
			message.ResourceTypeConnection, message.OperationReconnect)
	beehiveContext.Send(modules.EdgeHubModuleName, *msg)
}

func (c *LeaseBridge) Create(ctx context.Context, lease *coordinationv1.Lease, _ metav1.CreateOptions) (result *coordinationv1.Lease, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	result, err = c.MetaClient.Leases(c.ns).Create(lease)
	return result, c.trackResult("create", err)
}

func (c *LeaseBridge) Update(ctx context.Context, lease *coordinationv1.Lease, _ metav1.UpdateOptions) (result *coordinationv1.Lease, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	result, err = c.MetaClient.Leases(c.ns).Update(lease)
	return result, c.trackResult("update", err)
}

func (c *LeaseBridge) Get(ctx context.Context, name string, _ metav1.GetOptions) (result *coordinationv1.Lease, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	result, err = c.MetaClient.Leases(c.ns).Get(name)
	return result, c.trackResult("get", err)
}
