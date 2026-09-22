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

package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/square/go-jose.v2"
	"gopkg.in/square/go-jose.v2/jwt"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/authenticator"

	edgecorev1alpha2 "github.com/kubeedge/api/apis/componentconfig/edgecore/v1alpha2"
	"github.com/kubeedge/beehive/pkg/core/model"
	metaclient "github.com/kubeedge/kubeedge/edge/pkg/metamanager/client"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao/dbclient"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao/models"
)

var authTestDBPath string

func TestMain(m *testing.M) {
	tempDir, err := os.MkdirTemp("", "kubeedge-token-auth-test-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	authTestDBPath = filepath.Join(tempDir, "meta.db")
	exitCode := m.Run()
	if err := os.RemoveAll(tempDir); err != nil && exitCode == 0 {
		fmt.Fprintln(os.Stderr, err)
		exitCode = 1
	}
	os.Exit(exitCode)
}

func TestAuthenticateExtendedTokenAfterStartupGC(t *testing.T) {
	dao.Init(authTestDBPath, &edgecorev1alpha2.MetaManager{Enable: true})
	metaService := dbclient.NewMetaService()
	current := time.Now().Truncate(time.Second)
	originalNow := now
	now = func() time.Time { return current }
	t.Cleanup(func() { now = originalNow })

	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name: "test-sa", Namespace: "test-ns", UID: "test-sa-uid",
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "test-pod", Namespace: sa.Namespace, UID: "test-pod-uid",
	}}
	private := privateClaims{Kubernetes: kubernetes{
		Namespace: sa.Namespace,
		Svcacct:   ref{Name: sa.Name, UID: string(sa.UID)},
		Pod:       &ref{Name: pod.Name, UID: string(pod.UID)},
	}}
	signingKey := []byte("test-only-service-account-signing-key")
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.HS256, Key: signingKey}, nil)
	if err != nil {
		t.Fatal(err)
	}
	claims := jwt.Claims{
		Issuer: "test-issuer", Audience: []string{"test-audience"},
		IssuedAt: jwt.NewNumericDate(current.Add(-2 * time.Hour)),
		Expiry:   jwt.NewNumericDate(current.Add(24 * time.Hour)),
	}
	token, err := jwt.Signed(signer).Claims(claims).Claims(private).CompactSerialize()
	if err != nil {
		t.Fatal(err)
	}
	response := authenticationv1.TokenRequest{
		Spec: authenticationv1.TokenRequestSpec{
			BoundObjectRef: &authenticationv1.BoundObjectReference{Kind: "Pod", Name: pod.Name, UID: pod.UID},
		},
		Status: authenticationv1.TokenRequestStatus{
			Token: token, ExpirationTimestamp: metav1.NewTime(current.Add(-time.Hour)),
		},
	}
	data, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	key := metaclient.KeyFunc(sa.Name, sa.Namespace, &response)
	if err := metaService.InsertOrUpdate(&models.Meta{
		Key: key, Type: model.ResourceTypeServiceAccountToken, Value: string(data),
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaService.DeleteMetaByKey(key) })
	newAuthenticator := func() authenticator.Token {
		return JWTTokenAuthenticator(nil, []string{claims.Issuer}, nil,
			authenticator.Audiences(claims.Audience), NewValidator(fakeGetter{serviceAccount: sa, pod: pod}))
	}
	if _, ok, err := newAuthenticator().AuthenticateToken(context.Background(), token); err != nil || !ok {
		t.Fatalf("authentication before restart: ok=%v, err=%v", ok, err)
	}

	// Run the initial collection synchronously, then recreate the authenticator
	// as MetaServer does on restart. The closed channel stops subsequent ticks.
	stop := make(chan struct{})
	close(stop)
	metaclient.RunExpiredTokenGC(stop)
	authn := newAuthenticator()
	if _, ok, err := authn.AuthenticateToken(context.Background(), token); err != nil || !ok {
		t.Fatalf("authentication after startup GC: ok=%v, err=%v", ok, err)
	}

	// Retaining the cached JWT must not bypass expiry or bound Pod validation.
	current = claims.Expiry.Time().Add(jwt.DefaultLeeway + time.Second)
	if _, ok, err := authn.AuthenticateToken(context.Background(), token); ok || errString(err) != "service account token has expired" {
		t.Fatalf("expired token authentication: ok=%v, err=%v", ok, err)
	}
	current = time.Now()
	pod.UID = "replacement-pod-uid"
	if _, ok, err := authn.AuthenticateToken(context.Background(), token); ok || err == nil {
		t.Fatalf("replacement Pod authentication: ok=%v, err=%v", ok, err)
	}
	pod.UID = response.Spec.BoundObjectRef.UID
	metaclient.New().ServiceAccountToken().DeleteServiceAccountToken(pod.UID)
	if _, ok, err := authn.AuthenticateToken(context.Background(), token); ok || errString(err) != "tokenData not found when authenticating" {
		t.Fatalf("deleted Pod token authentication: ok=%v, err=%v", ok, err)
	}
}
