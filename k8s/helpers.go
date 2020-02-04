package k8s

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const idSeparator = "::"

func idParts(id string) (string, string, string, string, error) {
	parts := strings.Split(id, idSeparator)
	if len(parts) != 4 {
		err := fmt.Errorf("unexpected ID format (%q), expected %q.", id, "namespace::groupVersion::kind::name")
		return "", "", "", "", err
	}

	return parts[0], parts[1], parts[2], parts[3], nil
}

func buildId(object *unstructured.Unstructured) string {
	return strings.Join(
		[]string{
			object.GetNamespace(),
			object.GroupVersionKind().GroupVersion().String(),
			object.GroupVersionKind().Kind,
			object.GetName(),
		},
		idSeparator,
	)
}

func expandStringSlice(s []interface{}) []string {
	result := make([]string, len(s), len(s))
	for k, v := range s {
		// Handle the Terraform parser bug which turns empty strings in lists to nil.
		if v == nil {
			result[k] = ""
		} else {
			result[k] = v.(string)
		}
	}
	return result
}
