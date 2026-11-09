package synccontroller

import (
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"github.com/kubeedge/api/apis/reliablesyncs/v1alpha1"
	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/cloud/pkg/common/messagelayer"
	"github.com/kubeedge/kubeedge/cloud/pkg/common/modules"
	edgectrconst "github.com/kubeedge/kubeedge/cloud/pkg/edgecontroller/constants"
	commonconst "github.com/kubeedge/kubeedge/common/constants"
	"github.com/kubeedge/kubeedge/pkg/metaserver/util"
)

func (sctl *SyncController) reconcileObjectSync(sync *v1alpha1.ObjectSync) {
	var object metav1.Object

	gv, err := schema.ParseGroupVersion(sync.Spec.ObjectAPIVersion)
	if err != nil {
		klog.ErrorS(err, "Failed to parse ObjectSync API version",
			"objectSync", klog.KObj(sync),
			"apiVersion", sync.Spec.ObjectAPIVersion,
			"kind", sync.Spec.ObjectKind)
		return
	}
	if sync.Spec.ObjectKind == "" {
		klog.ErrorS(nil, "ObjectSync kind is empty", "objectSync", klog.KObj(sync))
		return
	}
	resource := util.UnsafeKindToResource(sync.Spec.ObjectKind)
	gvr := gv.WithResource(resource)
	nodeName := getNodeName(sync.Name)
	resourceType := strings.ToLower(sync.Spec.ObjectKind)

	lister, err := sctl.informerManager.GetLister(gvr)
	if err != nil {
		klog.ErrorS(err, "Failed to get ObjectSync resource lister",
			"objectSync", klog.KObj(sync),
			"resource", gvr.String())
		return
	}
	if lister == nil {
		klog.ErrorS(nil, "ObjectSync resource lister is nil",
			"objectSync", klog.KObj(sync),
			"resource", gvr.String())
		return
	}

	ret, err := lister.ByNamespace(sync.Namespace).Get(sync.Spec.ObjectName)

	if apierrors.IsNotFound(err) {
		sctl.gcOrphanedObjectSync(sync)
		return
	} else if err != nil || ret == nil {
		klog.Errorf("failed to get obj(gvr:%v,namespace:%v,name:%v), %v", gvr, sync.Namespace, sync.Spec.ObjectName, err)
		return
	}

	object, err = meta.Accessor(ret)
	if err != nil {
		klog.ErrorS(err, "Failed to access ObjectSync resource metadata",
			"objectSync", klog.KObj(sync),
			"resource", gvr.String())
		return
	}

	syncObjUID := getObjectUID(sync.Name)
	if syncObjUID != string(object.GetUID()) {
		sctl.gcOrphanedObjectSync(sync)
		return
	}

	sendEvents(nodeName, sync, resourceType, object.GetResourceVersion(), object)
}

// gcOrphanedObjectSync try to send delete message to the edge node
// to make sure that the resource is deleted in the edge node. After the
// message ACK is received by `cloudHub`, the objectSync will be deleted
// directly in the `cloudHub`. But if message build failed, the objectSync
// will be deleted directly in the `syncController`.
func (sctl *SyncController) gcOrphanedObjectSync(sync *v1alpha1.ObjectSync) {
	resourceType := strings.ToLower(sync.Spec.ObjectKind)
	nodeName := getNodeName(sync.Name)
	klog.V(4).Infof("%s: %s has been deleted in K8s, send the delete event to edge in sync loop", resourceType, sync.Spec.ObjectName)

	object := &unstructured.Unstructured{}
	object.SetNamespace(sync.Namespace)
	object.SetName(sync.Spec.ObjectName)
	object.SetUID(types.UID(getObjectUID(sync.Name)))
	// The edge decodes the delete stub through its apiVersion/kind; without
	// them the delete is silently dropped at the edge.
	object.SetAPIVersion(sync.Spec.ObjectAPIVersion)
	object.SetKind(sync.Spec.ObjectKind)
	if msg := buildEdgeControllerMessage(nodeName, sync.Namespace, resourceType, sync.Spec.ObjectName, model.DeleteOperation, object); msg != nil {
		beehiveContext.Send(commonconst.DefaultContextSendModuleName, *msg)
		return
	}
	// Message build failed: keep the ObjectSync so the next 5s reconcile
	// re-attempts the delete. Deleting the accounting here would tell the
	// edge nothing while destroying the only channel that can still
	// propagate the deletion (permanent orphan row).
	klog.Warningf("failed to build delete message for orphaned objectSync %s, will retry in next sync loop", sync.Name)
}

func sendEvents(nodeName string, sync *v1alpha1.ObjectSync, resourceType string,
	objectResourceVersion string, obj interface{}) {
	if sync.Status.ObjectResourceVersion == "" {
		klog.Errorf("The ObjectResourceVersion is empty in status of objectsync: %s", sync.Name)
		return
	}

	if CompareResourceVersion(objectResourceVersion, sync.Status.ObjectResourceVersion) > 0 {
		// trigger the update event
		klog.V(4).Infof("The resourceVersion: %s of %s in K8s is greater than in edgenode: %s, send the update event", objectResourceVersion, resourceType, sync.Status.ObjectResourceVersion)
		msg := buildEdgeControllerMessage(nodeName, sync.Namespace, resourceType, sync.Spec.ObjectName, model.UpdateOperation, obj)
		if msg == nil {
			return
		}
		beehiveContext.Send(commonconst.DefaultContextSendModuleName, *msg)
	}
}

func buildEdgeControllerMessage(nodeName, namespace, resourceType, resourceName, operationType string, obj interface{}) *model.Message {
	resource, err := messagelayer.BuildResource(nodeName, namespace, resourceType, resourceName)
	if err != nil {
		klog.Warningf("build message resource failed with error: %s", err)
		return nil
	}

	// Informer cache objects are shared read-only: send a private deep copy
	// with the GVK stamped on it. The edge decodes unstructured bodies
	// through their in-body apiVersion/kind, and in-place stamping of the
	// cache object caused the concurrent map read/write panic.
	body := util.SetMetaTypeOnCopy(obj)
	if body == nil {
		klog.Errorf("failed to build message body for resource %s/%s", resourceType, resourceName)
		return nil
	}

	msg := model.NewMessage("").
		BuildRouter(modules.EdgeControllerModuleName, edgectrconst.GroupResource, resource, operationType).
		FillBody(body).
		SetResourceVersion(GetObjectResourceVersion(body))

	return msg
}

// GetObjectResourceVersion returns the resourceVersion of the object in message
func GetObjectResourceVersion(obj interface{}) string {
	if obj == nil {
		klog.Error("object is nil")
		return ""
	}

	accessor, err := meta.Accessor(obj)
	if err != nil {
		klog.Errorf("Failed to get resourceVersion of the object: %v", obj)
		return ""
	}

	return accessor.GetResourceVersion()
}

// CompareResourceVersion compares resourceversions, resource versions are actually
// ints, so we can easily compare them.
// If rva>rvb, return 1; rva=rvb, return 0; rva<rvb, return -1
func CompareResourceVersion(rva, rvb string) int {
	a, err := strconv.ParseUint(rva, 10, 64)
	if err != nil {
		// coder error
		panic(err)
	}
	b, err := strconv.ParseUint(rvb, 10, 64)
	if err != nil {
		// coder error
		panic(err)
	}

	if a > b {
		return 1
	}
	if a == b {
		return 0
	}
	return -1
}
