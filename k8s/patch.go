// Copyright The Helm Authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Adapted from https://github.com/helm/helm/blob/master/pkg/kube/client.go
// and https://github.com/helm/helm/blob/master/pkg/kube/converter.go

package k8s

import (
	"context"
	"log"
	"sync"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var k8sNativeScheme *runtime.Scheme
var k8sNativeSchemeOnce sync.Once

func patch(c client.Client, target, original, current *unstructured.Unstructured) error {
	patch, patchType, err := createPatch(target, original, current)
	if err != nil {
		return errors.Wrap(err, "failed to create patch")
	}

	if patch == nil || string(patch) == "{}" {
		log.Printf("Looks like there are no changes for %s %q", target.GroupVersionKind().String(), target.GetName())
		return nil
	}

	// send patch to server
	if err = c.Patch(context.TODO(), current, client.RawPatch(patchType, patch)); err != nil {
		return errors.Wrapf(err, "cannot patch %q with kind %s", target.GroupVersionKind().String(), target.GetName())
	}
	return nil
}

func createPatch(target, original, current *unstructured.Unstructured) ([]byte, types.PatchType, error) {
	oldData, err := json.Marshal(current)
	if err != nil {
		return nil, types.StrategicMergePatchType, errors.Wrap(err, "serializing current configuration")
	}
	originalData, err := json.Marshal(original)
	if err != nil {
		return nil, types.StrategicMergePatchType, errors.Wrap(err, "serializing original configuration")
	}
	newData, err := json.Marshal(target)
	if err != nil {
		return nil, types.StrategicMergePatchType, errors.Wrap(err, "serializing target configuration")
	}

	versionedObject := convert(target)

	// Unstructured objects, such as CRDs, may not have an not registered error
	// returned from ConvertToVersion. Anything that's unstructured should
	// use the jsonpatch.CreateMergePatch. Strategic Merge Patch is not supported
	// on objects like CRDs.
	_, isUnstructured := versionedObject.(runtime.Unstructured)

	if isUnstructured {
		// fall back to generic JSON merge patch
		patch, err := jsonpatch.CreateMergePatch(oldData, newData)
		return patch, types.MergePatchType, err
	}

	patchMeta, err := strategicpatch.NewPatchMetaFromStruct(versionedObject)
	if err != nil {
		return nil, types.StrategicMergePatchType, errors.Wrap(err, "unable to create patch metadata from object")
	}

	patch, err := strategicpatch.CreateThreeWayMergePatch(originalData, newData, oldData, patchMeta, true)
	return patch, types.StrategicMergePatchType, err
}

func convert(obj *unstructured.Unstructured) runtime.Object {
	s := kubernetesNativeScheme()
	if obj, err := runtime.ObjectConvertor(s).ConvertToVersion(obj, obj.GroupVersionKind().GroupVersion()); err == nil {
		return obj
	}
	return obj
}

// kubernetesNativeScheme returns a clean *runtime.Scheme with _only_ Kubernetes
// native resources added to it. This is required to break free of custom resources
// that may have been added to scheme.Scheme due to Helm being used as a package in
// combination with e.g. a versioned kube client. If we would not do this, the client
// may attempt to perform e.g. a 3-way-merge strategy patch for custom resources.
func kubernetesNativeScheme() *runtime.Scheme {
	k8sNativeSchemeOnce.Do(func() {
		k8sNativeScheme = runtime.NewScheme()
		scheme.AddToScheme(k8sNativeScheme)
	})
	return k8sNativeScheme
}
