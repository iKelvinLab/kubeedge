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

package v1

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeedge/beehive/pkg/common"
	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/edge/pkg/common/message"
	"github.com/kubeedge/kubeedge/edge/pkg/common/modules"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/client"
)

func init() {
	beehiveContext.InitContext([]string{common.MsgCtxTypeChannel})
	beehiveContext.AddModule(&common.ModuleInfo{
		ModuleName: modules.EdgeHubModuleName,
		ModuleType: common.MsgCtxTypeChannel,
	})

	// A single persistent receiver records every message that reaches the
	// edgehub module channel, so tests can drain and assert on it without
	// leaving orphaned receivers behind that would steal messages.
	go func() {
		for {
			msg, err := beehiveContext.Receive(modules.EdgeHubModuleName)
			if err != nil {
				return
			}
			select {
			case edgeHubMessages <- msg:
			default: // drop overflow; tests never need this many
			}
		}
	}()
}

// edgeHubMessages buffers everything the persistent receiver gets from the
// edgehub module channel.
var edgeHubMessages = make(chan model.Message, 256)

var errLeaseTimeout = errors.New("timed out waiting for the condition")

// fakeLeases implements client.LeasesInterface with controllable failures:
// the first failFirst calls return errLeaseTimeout, later calls succeed.
type fakeLeases struct {
	failFirst int
	calls     int
}

func (f *fakeLeases) maybeFail() error {
	f.calls++
	if f.calls <= f.failFirst {
		return errLeaseTimeout
	}
	return nil
}

func (f *fakeLeases) Create(lease *coordinationv1.Lease) (*coordinationv1.Lease, error) {
	if err := f.maybeFail(); err != nil {
		return nil, err
	}
	return lease, nil
}

func (f *fakeLeases) Update(lease *coordinationv1.Lease) (*coordinationv1.Lease, error) {
	if err := f.maybeFail(); err != nil {
		return nil, err
	}
	return lease, nil
}

func (f *fakeLeases) Get(name string) (*coordinationv1.Lease, error) {
	if err := f.maybeFail(); err != nil {
		return nil, err
	}
	return &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name}}, nil
}

// fakeCoreClient implements client.CoreInterface by embedding it and routing
// only the Leases getter to the fake.
type fakeCoreClient struct {
	client.CoreInterface
	leases         *fakeLeases
	lastLeasesNsOf string
}

func (f *fakeCoreClient) Leases(namespace string) client.LeasesInterface {
	f.lastLeasesNsOf = namespace
	return f.leases
}

func newTestLeaseBridge(fl *fakeLeases) (*LeaseBridge, *fakeCoreClient) {
	core := &fakeCoreClient{leases: fl}
	return &LeaseBridge{
		ns:         "kube-node-lease",
		MetaClient: core,
	}, core
}

// nextEdgeHubMessage returns the next recorded edgehub message, waiting at
// most the given duration.
func nextEdgeHubMessage(d time.Duration) (model.Message, bool) {
	select {
	case msg := <-edgeHubMessages:
		return msg, true
	case <-time.After(d):
		return model.Message{}, false
	}
}

// drainEdgeHubMessages removes messages recorded by earlier tests so negative
// assertions start from a clean slate.
func drainEdgeHubMessages(t *testing.T) {
	t.Helper()

	for {
		select {
		case <-edgeHubMessages:
		default:
			return
		}
	}
}

func TestLeaseBridgeCircuitBreakerTriggersReconnect(t *testing.T) {
	drainEdgeHubMessages(t)

	fl := &fakeLeases{failFirst: 1000}
	bridge, core := newTestLeaseBridge(fl)
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}

	var err error
	for i := 1; i < maxConsecutiveLeaseFailures; i++ {
		_, err = bridge.Update(context.Background(), lease, metav1.UpdateOptions{})
		require.Error(t, err)
		assert.NotErrorIs(t, err, ErrLeaseCircuitBreaker,
			"failures below the threshold must surface the original error")
	}

	// 5th consecutive failure trips the breaker.
	_, err = bridge.Update(context.Background(), lease, metav1.UpdateOptions{})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrLeaseCircuitBreaker)
	assert.Equal(t, maxConsecutiveLeaseFailures, fl.calls)
	assert.Equal(t, "kube-node-lease", core.lastLeasesNsOf)

	msg, ok := nextEdgeHubMessage(2 * time.Second)
	require.True(t, ok, "circuit breaker must ask edgehub to reset the session")
	assert.Equal(t, modules.EdgedModuleName, msg.GetSource())
	assert.Equal(t, modules.HubGroup, msg.GetGroup())
	assert.Equal(t, message.ResourceTypeConnection, msg.GetResource())
	assert.Equal(t, message.OperationReconnect, msg.GetOperation())

	// The counter restarted: the next single failure is a plain error again.
	_, err = bridge.Update(context.Background(), lease, metav1.UpdateOptions{})
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrLeaseCircuitBreaker)
}

func TestLeaseBridgeSuccessResetsCounter(t *testing.T) {
	drainEdgeHubMessages(t)

	fl := &fakeLeases{failFirst: 4}
	bridge, _ := newTestLeaseBridge(fl)
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}

	// 4 failures (one below the threshold)...
	for i := 0; i < 4; i++ {
		_, err := bridge.Update(context.Background(), lease, metav1.UpdateOptions{})
		require.Error(t, err)
		assert.NotErrorIs(t, err, ErrLeaseCircuitBreaker)
	}
	// ...one success resets the counter...
	got, err := bridge.Update(context.Background(), lease, metav1.UpdateOptions{})
	require.NoError(t, err)
	assert.Equal(t, lease, got)
	// ...so 4 more failures still do not trip the breaker.
	fl.failFirst = 1000
	for i := 0; i < 4; i++ {
		_, err := bridge.Update(context.Background(), lease, metav1.UpdateOptions{})
		require.Error(t, err)
		assert.NotErrorIs(t, err, ErrLeaseCircuitBreaker)
	}

	_, ok := nextEdgeHubMessage(150 * time.Millisecond)
	assert.False(t, ok, "no reconnect request may be sent while below the threshold")
}

func TestLeaseBridgeGetTripsCircuitBreaker(t *testing.T) {
	drainEdgeHubMessages(t)

	fl := &fakeLeases{failFirst: 1000}
	bridge, _ := newTestLeaseBridge(fl)

	var err error
	for i := 0; i < maxConsecutiveLeaseFailures; i++ {
		_, err = bridge.Get(context.Background(), "node-1", metav1.GetOptions{})
	}
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrLeaseCircuitBreaker)

	_, ok := nextEdgeHubMessage(2 * time.Second)
	require.True(t, ok)
}

func TestLeaseBridgeHonorsContextCancellation(t *testing.T) {
	drainEdgeHubMessages(t)

	fl := &fakeLeases{}
	bridge, _ := newTestLeaseBridge(fl)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
	_, err := bridge.Create(ctx, lease, metav1.CreateOptions{})
	assert.ErrorIs(t, err, context.Canceled)
	_, err = bridge.Update(ctx, lease, metav1.UpdateOptions{})
	assert.ErrorIs(t, err, context.Canceled)
	_, err = bridge.Get(ctx, "node-1", metav1.GetOptions{})
	assert.ErrorIs(t, err, context.Canceled)

	assert.Equal(t, 0, fl.calls, "a canceled context must not reach the metaclient")
}

func TestLeaseBridgeSuccessPath(t *testing.T) {
	drainEdgeHubMessages(t)

	fl := &fakeLeases{}
	bridge, _ := newTestLeaseBridge(fl)
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}

	gotCreate, err := bridge.Create(context.Background(), lease, metav1.CreateOptions{})
	require.NoError(t, err)
	assert.Equal(t, lease, gotCreate)

	gotUpdate, err := bridge.Update(context.Background(), lease, metav1.UpdateOptions{})
	require.NoError(t, err)
	assert.Equal(t, lease, gotUpdate)

	gotGet, err := bridge.Get(context.Background(), "node-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "node-1", gotGet.Name)
}
