# k8s_manifest Resource

The `k8s_manifest` resource applies any kind of Kubernetes resources against the Kubernetes API server and waits until it gets provisioned (not supported for CRDs).

## Example Usage

```hcl
provider "k8s" {
  config_context = "prod-cluster"
}

data "template_file" "nginx-deployment" {
  template = "${file("manifests/nginx-deployment.yaml")}"

  vars {
    replicas = "${var.replicas}"
  }
}

resource "k8s_manifest" "nginx-deployment" {
  content = "${data.template_file.nginx-deployment.rendered}"
}

# creating a second resource in the nginx namespace
resource "k8s_manifest" "nginx-deployment" {
  content   = "${data.template_file.nginx-deployment.rendered}"
  namespace = "nginx"
}
```

**NOTE**: If the YAML formatted `content` contains multiple documents (separated by `---`) only the first non-empty document is going to be parsed. This is because Terraform is mostly designed to represent a single resource on the provider side with a Terraform resource:

> resource types correspond to an infrastructure object type that is managed via a remote network API
> -- <cite>[Terraform Documentation](https://www.terraform.io/docs/configuration/resources.html)</cite>

You can workaround this easily with the following snippet (however we still suggest to use separate resources):

```hcl
locals {
  resources = split("\n---\n", data.template_file.ngnix.rendered)
}

resource "k8s_manifest" "nginx-deployment" {
  count = length(local.resources)

  content = local.resources[count.index]
}
```

## Argument Reference

The following arguments are supported:

* `content` - (Required) Content defines the specification of manifest in YAML (or JSON format).
* `namespace` - (Optional) Namespace defines the namespace of the resource to be created if not defined in the `content`.
* `ignore_field` - (Optional) List of jq style path you don't want to update in your manifest.


## Import

A resource can be imported using the namespace, groupVersion, kind, and name, e.g.

```
$ terraform import k8s_manifest.example namespace::groupVersion::kind::name
```
