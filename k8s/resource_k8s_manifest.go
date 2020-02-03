package k8s

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/mitchellh/mapstructure"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
		object.SetNamespace("default")
	} else if objectNamespace == "" {
		// TODO: which namespace should have a higher precedence?
		object.SetNamespace(namespace)
	}

	client := meta.(*ProviderConfig).RuntimeClient

	log.Printf("[INFO] Creating new manifest: %#v", object)
	err = client.Create(context.Background(), object)
	if err != nil {
		return err
	}

	err = waitForReadyStatus(d, client, object)
	if err != nil {
		return err
	}

	d.SetId(buildId(object))

	return resourceK8sManifestRead(d, meta)
}

func waitForReadyStatus(d *schema.ResourceData, c client.Client, object *unstructured.Unstructured) error {
	objectKey, err := client.ObjectKeyFromObject(object)
	if err != nil {
		log.Printf("[DEBUG] Received error: %#v", err)
		return err
	}

	createStateConf := &resource.StateChangeConf{
		Pending: []string{
			"pending",
		},
		Target: []string{
			"ready",
		},
		Refresh: func() (interface{}, string, error) {
			err = c.Get(context.Background(), objectKey, object)
			if err != nil {
				log.Printf("[DEBUG] Received error: %#v", err)
				return nil, "error", err
			}

			log.Printf("[DEBUG] Received object: %#v", object)

			if s, ok := object.Object["status"]; ok {
				log.Printf("[DEBUG] Object has status: %#v", s)

				if len(s.(map[string]interface{})) == 0 {
					return object, "pending", nil
				}

				var status status
				err = mapstructure.Decode(s, &status)
				if err != nil {
					log.Printf("[DEBUG] Received error on decode: %#v", err)
					return nil, "error", err
				}

				if status.ReadyReplicas != nil && *status.ReadyReplicas > 0 {
					return object, "ready", nil
				}

				if status.Phase != nil && *status.Phase == "Active" {
					return object, "ready", nil
				}

				return object, "pending", nil
			}

			return object, "ready", nil
		},
		Timeout:                   d.Timeout(schema.TimeoutCreate),
		Delay:                     5 * time.Second,
		MinTimeout:                5 * time.Second,
		ContinuousTargetOccurence: 1,
	}

	_, err = createStateConf.WaitForState()
	if err != nil {
		return fmt.Errorf("Error waiting for resource (%s) to be created: %s", d.Id(), err)
	}

	return nil
}

type status struct {
	ReadyReplicas *int
	Phase         *string
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
	namespace, _, _, name, err := idParts(d.Id())
	if err != nil {
		return err
	}

	content := d.Get("content").(string)

	decoder := yaml.NewYAMLOrJSONDecoder(strings.NewReader(content), 4096)

	var object *unstructured.Unstructured

	// TODO: add support for a list of objects?
	err = decoder.Decode(&object)
	if err != nil && err != io.EOF {
		return fmt.Errorf("Failed to unmarshal manifest: %s", err)
	}

	objectNamespace := object.GetNamespace()

	if namespace == "" && objectNamespace == "" {
		object.SetNamespace("default")
	} else if objectNamespace == "" {
		// TODO: which namespace should have a higher precedence?
		object.SetNamespace(namespace)
	}

	client := meta.(*ProviderConfig).RuntimeClient

	log.Printf("[INFO] Updating object %s", name)
	err = client.Update(context.Background(), object)
	if err != nil {
		log.Printf("[DEBUG] Received error: %#v", err)
		return err
	}
	log.Printf("[INFO] Updated object: %#v", object)

	return waitForReadyStatus(d, client, object)
}

func resourceK8sManifestDelete(d *schema.ResourceData, meta interface{}) error {
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

	log.Printf("[INFO] Deleting object %s", name)
	err = client.Delete(context.Background(), currentObject)
	if err != nil {
		log.Printf("[DEBUG] Received error: %#v", err)
		return err
	}

	createStateConf := &resource.StateChangeConf{
		Pending: []string{
			"deleting",
		},
		Target: []string{
			"deleted",
		},
		Refresh: func() (interface{}, string, error) {
			err := client.Get(context.Background(), objectKey, currentObject)
			if err != nil {
				log.Printf("[INFO] error when deleting object %s: %+v", name, err)
				if apierrors.IsNotFound(err) {
					return currentObject, "deleted", nil
				}
				return nil, "error", err

			}
			return currentObject, "deleting", nil
		},
		Timeout:                   d.Timeout(schema.TimeoutDelete),
		Delay:                     5 * time.Second,
		MinTimeout:                5 * time.Second,
		ContinuousTargetOccurence: 1,
	}

	_, err = createStateConf.WaitForState()
	if err != nil {
		return fmt.Errorf("Error waiting for resource (%s) to be deleted: %s", d.Id(), err)
	}

	log.Printf("[INFO] Deleted object: %#v", currentObject)

	return nil
}
