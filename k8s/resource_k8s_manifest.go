package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/itchyny/gojq"
	"log"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/helper/validation"
	"github.com/mitchellh/mapstructure"
	goyaml "gopkg.in/yaml.v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/kubectl/pkg/polymorphichelpers"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func resourceK8sManifest() *schema.Resource {
	return &schema.Resource{
		Create: resourceK8sManifestCreate,
		Read:   resourceK8sManifestRead,
		Update: resourceK8sManifestUpdate,
		Delete: resourceK8sManifestDelete,
		Importer: &schema.ResourceImporter{
			State: resourceK8sManifestImport,
		},
		Schema: map[string]*schema.Schema{
			"namespace": {
				Type:      schema.TypeString,
				Optional:  true,
				Sensitive: false,
				ForceNew:  true,
			},
			"content": {
				Type:         schema.TypeString,
				Required:     true,
				Sensitive:    false,
				ValidateFunc: validation.StringIsNotEmpty,
			},
			"override_content": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"ignore_fields": {
				Type:        schema.TypeList,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Description: "List of jq style path you don't want to update.",
				Optional:    true,
			},
			"delete_cascade": {
				Type:      schema.TypeBool,
				Optional:  true,
				Sensitive: false,
			},
		},
		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(5 * time.Minute),
			Update: schema.DefaultTimeout(5 * time.Minute),
			Delete: schema.DefaultTimeout(5 * time.Minute),
		},
	}
}

func resourceK8sManifestCreate(d *schema.ResourceData, config interface{}) error {

	namespace := d.Get("namespace").(string)
	content := d.Get("content").(string)

	object, err := contentToObject(content)
	if err != nil {
		return err
	}

	objectNamespace := object.GetNamespace()

	if namespace == "" && objectNamespace == "" {
		object.SetNamespace("default")
	} else if objectNamespace == "" {
		// TODO: which namespace should have a higher precedence?
		object.SetNamespace(namespace)
	}

	client := config.(*ProviderConfig).RuntimeClient

	log.Printf("[INFO] Creating new manifest: %#v", object)
	err = client.Create(context.Background(), object)
	if err != nil {
		return err
	}

	// this must stand before the wait to avoid losing state on error
	d.SetId(buildId(object))

	err = waitForReadyStatus(d, client, object, d.Timeout(schema.TimeoutCreate))
	if err != nil {
		return err
	}

	return resourceK8sManifestRead(d, config)
}

func waitForReadyStatus(d *schema.ResourceData, c client.Client, object *unstructured.Unstructured, timeout time.Duration) error {
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

				if statusViewer, err := polymorphichelpers.StatusViewerFor(object.GetObjectKind().GroupVersionKind().GroupKind()); err == nil {
					_, ready, err := statusViewer.Status(object, 0)
					if err != nil {
						return nil, "error", err
					}
					if ready {
						return object, "ready", nil
					}
					return object, "pending", nil
				}
				log.Printf("[DEBUG] Object has no rollout status viewer")

				var status status
				err = mapstructure.Decode(s, &status)
				if err != nil {
					log.Printf("[DEBUG] Received error on decode: %#v", err)
					return nil, "error", err
				}

				if status.ReadyReplicas != nil {
					if *status.ReadyReplicas > 0 {
						return object, "ready", nil
					}

					return object, "pending", nil
				}

				if status.Phase != nil {
					if *status.Phase == "Active" || *status.Phase == "Bound" || *status.Phase == "Running" || *status.Phase == "Ready" || *status.Phase == "Online" || *status.Phase == "Healthy" {
						return object, "ready", nil
					}

					return object, "pending", nil
				}

				if status.LoadBalancer != nil {
					// LoadBalancer status may be for an Ingress or a Service having type=LoadBalancer
					checkLoadBalancer := true
					if object.GetAPIVersion() == "v1" && object.GetKind() == "Service" {
						specInterface, ok := object.Object["spec"]
						if !ok {
							log.Printf("[DEBUG] Received error on decode: %#v", err)
							return nil, "error", err
						}
						spec, ok := specInterface.(map[string]interface{})
						if !ok {
							log.Printf("[DEBUG] Received error on decode: %#v", err)
							return nil, "error", err
						}
						serviceType, ok := spec["type"]
						if !ok {
							log.Printf("[DEBUG] Received error on decode: %#v", err)
							return nil, "error", err
						}
						checkLoadBalancer = serviceType == "LoadBalancer"
					}
					if checkLoadBalancer {
						if len(*status.LoadBalancer) > 0 {
							return object, "ready", nil
						}
						return object, "pending", nil
					}
				}
			}

			return object, "ready", nil
		},
		Timeout:                   timeout,
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
	LoadBalancer  *map[string]interface{}
}

func resourceK8sManifestRead(d *schema.ResourceData, config interface{}) error {
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

	client := config.(*ProviderConfig).RuntimeClient

	log.Printf("[INFO] Reading object %s", name)
	err = client.Get(context.Background(), objectKey, object)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Printf("[INFO] Object missing: %#v", object)
			d.SetId("")
			return nil
		}
		if meta.IsNoMatchError(err) {
			log.Printf("[INFO] Object kind missing: %#v", object)
			d.SetId("")
			return nil
		}

		log.Printf("[DEBUG] Received error: %#v", err)
		return err
	}
	log.Printf("[INFO] Received object: %#v", object)

	ignoreFields, hasIgnoreFields := d.GetOk("ignore_fields")
	if hasIgnoreFields {
		content := d.Get("content").(string)
		contentModified, err := excludeIgnoreFields(ignoreFields, content)
		if err != nil {
			return err
		}

		contentYaml, err := json2Yaml(contentModified)
		if err != nil {
			return err
		}

		d.Set("override_content", contentYaml)
	}

	// TODO: save metadata in terraform state

	return nil
}

func resourceK8sManifestUpdate(d *schema.ResourceData, config interface{}) error {
	var originalData string
	var newData string

	namespace, _, _, _, err := idParts(d.Id())
	if err != nil {
		return err
	}

	if d.HasChanges("content", "ignore_fields") {
		//originalDataRaw, newDataRaw := d.GetChange("content")
		newDataRaw := d.Get("content")
		originalDataRaw := d.Get("override_content")

		ignoreFields, hasIgnoreFields := d.GetOk("ignore_fields")
		if hasIgnoreFields {
			originalData, err = excludeIgnoreFields(ignoreFields, originalDataRaw.(string))
			if err != nil {
				return err
			}

			newData, err = excludeIgnoreFields(ignoreFields, newDataRaw.(string))
			if err != nil {
				return err
			}
		}

		log.Printf("[DEBUG] Original vs modified: %s %s", originalData, newData)
		modified, err := contentToObject(newData)
		if err != nil {
			return err
		}

		original, err := contentToObject(originalData)
		if err != nil {
			return err
		}

		objectNamespace := modified.GetNamespace()

		if namespace == "" && objectNamespace == "" {
			modified.SetNamespace("default")
		} else if objectNamespace == "" {
			// TODO: which namespace should have a higher precedence?
			modified.SetNamespace(namespace)
		}

		objectKey, err := client.ObjectKeyFromObject(modified)
		if err != nil {
			log.Printf("[DEBUG] Received error: %#v", err)
			return err
		}

		current := modified.DeepCopy()

		client := config.(*ProviderConfig).RuntimeClient

		err = client.Get(context.Background(), objectKey, current)
		if err != nil {
			log.Printf("[DEBUG] Received error: %#v", err)
			return err
		}

		modified.SetResourceVersion(current.DeepCopy().GetResourceVersion())

		current.SetResourceVersion("")
		original.SetResourceVersion("")

		if err := patch(config.(*ProviderConfig).RuntimeClient, modified, original, current); err != nil {
			log.Printf("[DEBUG] Received error: %#v", err)
			return err
		}
		log.Printf("[INFO] Updated object: %#v", modified)

		err = waitForReadyStatus(d, client, modified, d.Timeout(schema.TimeoutCreate))
		if err != nil {
			return err
		}
	}

	return resourceK8sManifestRead(d, config)
}

func resourceK8sManifestDelete(d *schema.ResourceData, config interface{}) error {
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

	deleteCascade := d.Get("delete_cascade").(bool)
	deleteOptions := []client.DeleteOption{}
	if deleteCascade {
		deleteOptions = append(deleteOptions, client.PropagationPolicy(metav1.DeletePropagationForeground))
	}

	client := config.(*ProviderConfig).RuntimeClient

	log.Printf("[INFO] Deleting object %s", name)
	err = client.Delete(context.Background(), currentObject, deleteOptions...)
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

func resourceK8sManifestImport(d *schema.ResourceData, config interface{}) ([]*schema.ResourceData, error) {

	namespace, gv, kind, name, err := idParts(d.Id())
	if err != nil {
		return nil, err
	}

	groupVersion, err := k8sschema.ParseGroupVersion(gv)
	if err != nil {
		log.Printf("[DEBUG] Invalid group version in resource ID: %#v", err)
		return nil, err
	}

	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(groupVersion.WithKind(kind))
	object.SetNamespace(namespace)
	object.SetName(name)

	objectKey, err := client.ObjectKeyFromObject(object)
	if err != nil {
		log.Printf("[DEBUG] Received error: %#v", err)
		return nil, err
	}

	client := config.(*ProviderConfig).RuntimeClient

	err = client.Get(context.Background(), objectKey, object)
	if err != nil {
		log.Printf("[DEBUG] Received error: %#v", err)
		return nil, err
	}

	resource := schema.ResourceData{}
	resource.SetId(d.Id())

	return []*schema.ResourceData{&resource}, nil
}

func contentToObject(content string) (*unstructured.Unstructured, error) {
	decoder := yaml.NewYAMLOrJSONDecoder(strings.NewReader(content), 4096)

	var object *unstructured.Unstructured

	for {
		err := decoder.Decode(&object)
		if err != nil {
			return nil, fmt.Errorf("Failed to unmarshal manifest: %s", err)
		}

		if object != nil {
			return object, nil
		}
	}
}

func excludeIgnoreFields(ignoreFieldsRaw interface{}, content string) (string, error) {
	var contentModified []byte
	var ignoreFields []string

	for _, j := range ignoreFieldsRaw.([]interface{}) {
		ignoreFields = append(ignoreFields, j.(string))
	}

	for _, i := range ignoreFields {
		query, err := gojq.Parse(fmt.Sprintf("del(%s)", i))
		if err != nil {
			log.Printf("[DEBUG] Received error: %#v", err)
			return "", err
		}

		if len(contentModified) > 0 {
			d, err := yaml2GoData(string(contentModified))
			if err != nil {
				log.Printf("[DEBUG] Received error: %#v", err)
				return "", err
			}

			v, _ := query.Run(d).Next()
			if err, ok := v.(error); ok {
				log.Printf("[DEBUG] Received error: %#v", err)
				return "", err
			}

			contentModified, err = gojq.Marshal(v)

		} else {
			d, err := yaml2GoData(content)
			if err != nil {
				log.Printf("[DEBUG] Received error: %#v", err)
				return "", err
			}

			v, _ := query.Run(d).Next()
			if err, ok := v.(error); ok {
				log.Printf("[DEBUG] !!!Received error: %#v", err)
				return "", err
			}

			contentModified, err = gojq.Marshal(v)
		}

		if err != nil {
			log.Printf("[DEBUG] Received error from jq: %#v", err)
		}
	}
	return string(contentModified), nil
}

func yaml2GoData(i string) (map[string]interface{}, error) {
	var body map[string]interface{}
	decoder := yaml.NewYAMLOrJSONDecoder(strings.NewReader(i), 4096)
	err := decoder.Decode(&body)

	return body, err
}

func json2Yaml(i string) (string, error) {
	var body interface{}
	err := json.Unmarshal([]byte(i), &body)
	if err != nil {
		return "", err
	}

	data, err := goyaml.Marshal(body)

	return string(data), err
}
