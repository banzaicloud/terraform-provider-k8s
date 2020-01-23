package k8s

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/banzaicloud/k8s-objectmatcher/patch"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func resourceK8sManifest() *schema.Resource {
	return &schema.Resource{
		Create: resourceK8sManifestCreate,
		Read:   resourceK8sManifestRead,
		Update: resourceK8sManifestUpdate,
		Delete: resourceK8sManifestDelete,

		Schema: map[string]*schema.Schema{
			"namespace": {
				Type:      schema.TypeString,
				Optional:  true,
				Sensitive: false,
				ForceNew:  true,
			},
			"content": {
				Type:      schema.TypeString,
				Required:  true,
				Sensitive: false,
			},
		},
	}
}

func resourceK8sManifestCreate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*ProviderConfig).RuntimeClient

	namespace := d.Get("namespace").(string)
	content := d.Get("content").(string)

	decoder := yaml.NewYAMLOrJSONDecoder(strings.NewReader(content), 4096)

	var object *unstructured.Unstructured

	// TODO: add support for a list of objects?
	err := decoder.Decode(&object)
	if err != nil && err != io.EOF {
		return fmt.Errorf("Failed to unmarshal manifest: %s", err)
	}

	objectNamespace := object.GetNamespace()

	if namespace == "" && objectNamespace == "" {
		return fmt.Errorf("Missing namespace: either add namespace to the object or the resource config")
	} else if objectNamespace == "" {
		// TODO: which namespace should have a higher precedence?
		object.SetNamespace(namespace)
	}

	if err := patch.DefaultAnnotator.SetLastAppliedAnnotation(object); err != nil {
		return fmt.Errorf("Failed to apply annotation to object: %s", err)
	}

	log.Printf("[INFO] Creating new manifest: %#v", object)
	err = client.Create(context.Background(), object)
	if err != nil {
		return err
	}

	d.SetId(buildId(object))

	return resourceK8sManifestRead(d, meta)
}

func resourceK8sManifestRead(d *schema.ResourceData, meta interface{}) error {
	namespace, gv, kind, name, err := idParts(d.Id())
	if err != nil {
		return err
	}

	groupVersion, err := k8sschema.ParseGroupVersion(gv)
	if err != nil {
		log.Printf("[DEBUG] Invalid group version in resource ID: %#v", err)
		return err
	}

	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(groupVersion.WithKind(kind))
	object.SetNamespace(namespace)
	object.SetName(name)

	objectKey, err := client.ObjectKeyFromObject(object)
	if err != nil {
		log.Printf("[DEBUG] Received error: %#v", err)
		return err
	}

	client := meta.(*ProviderConfig).RuntimeClient

	log.Printf("[INFO] Reading object %s", name)
	err = client.Get(context.Background(), objectKey, object)
	if err != nil {
		log.Printf("[DEBUG] Received error: %#v", err)
		return err
	}
	log.Printf("[INFO] Received object: %#v", object)

	// TODO: save metadata in terraform state

	return nil
}

func resourceK8sManifestUpdate(d *schema.ResourceData, meta interface{}) error {
	namespace, gv, kind, name, err := idParts(d.Id())
	if err != nil {
		return err
	}

	groupVersion, err := k8sschema.ParseGroupVersion(gv)
	if err != nil {
		log.Printf("[DEBUG] Invalid group version in resource ID: %#v", err)
		return err
	}

	currentObject := &unstructured.Unstructured{}
	currentObject.SetGroupVersionKind(groupVersion.WithKind(kind))
	currentObject.SetNamespace(namespace)
	currentObject.SetName(name)

	objectKey, err := client.ObjectKeyFromObject(currentObject)
	if err != nil {
		log.Printf("[DEBUG] Received error: %#v", err)
		return err
	}

	client := meta.(*ProviderConfig).RuntimeClient

	log.Printf("[INFO] Reading object %s", name)
	err = client.Get(context.Background(), objectKey, currentObject)
	if err != nil {
		log.Printf("[DEBUG] Received error: %#v", err)
		return err
	}
	log.Printf("[INFO] Received object: %#v", currentObject)
}

func resourceK8sManifestDelete(d *schema.ResourceData, meta interface{}) error {

}
