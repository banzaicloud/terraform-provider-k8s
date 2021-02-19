# Kubernetes Terraform Provider

The k8s Terraform provider enables Terraform to deploy Kubernetes resources. Unlike the [official Kubernetes provider][kubernetes-provider] it handles raw manifests, leveraging [controller-runtime](https://github.com/kubernetes-sigs/controller-runtime) and the [Unstructured API](https://pkg.go.dev/github.com/kubernetes/apimachinery/pkg/apis/meta/v1/unstructured?tab=doc) directly to allow developers to work with any Kubernetes resource natively.

This project is a hard fork of [ericchiang/terraform-provider-k8s](https://github.com/ericchiang/terraform-provider-k8s).

## Installation

### The Go Get way

Use `go get` to install the provider:

```
go get -u github.com/banzaicloud/terraform-provider-k8s
```

Register the plugin in `~/.terraformrc` (see [Documentation](https://www.terraform.io/docs/commands/cli-config.html) for Windows users): 

```hcl
providers {
  k8s = "/$GOPATH/bin/terraform-provider-k8s"
}
```

### The Terraform Plugin way (enable versioning)

Download a release from the [Release page](https://github.com/banzaicloud/terraform-provider-k8s/releases) and make sure the name matches the following convention:

| OS      | Version | Name                              |
| ------- | ------- | --------------------------------- |
| LINUX   | 0.4.0   | terraform-provider-k8s_v0.4.0     |
|         | 0.3.0   | terraform-provider-k8s_v0.3.0     |
| Windows | 0.4.0   | terraform-provider-k8s_v0.4.0.exe |
|         | 0.3.0   | terraform-provider-k8s_v0.3.0.exe |

Install the plugin using [Terraform Third-party Plugin Documentation](https://www.terraform.io/docs/configuration/providers.html#third-party-plugins):

| Operating system  | User plugins directory        |
| ----------------- | ----------------------------- |
| Windows           | %APPDATA%\terraform.d\plugins |
| All other systems | ~/.terraform.d/plugins        |

## Usage

The provider uses your default Kubernetes configuration by default, but it takes some optional configuration parameters, see the [Configuration](#configuration) section (these parameters are the same as for the [Kubernetes provider](https://www.terraform.io/docs/providers/kubernetes/index.html#authentication)).

```hcl
terraform {
  required_providers {
    k8s = {
      version = ">= 0.8.0"
      source  = "banzaicloud/k8s"
    }
  }
}

provider "k8s" {
  config_context = "prod-cluster"
}
```

The `k8s` Terraform provider introduces a single Terraform resource, a `k8s_manifest`. The resource contains a `content` field, which contains a raw manifest in JSON or YAML format.

```hcl
variable "replicas" {
  type    = "string"
  default = 3
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

In this case `manifests/nginx-deployment.yaml` is a templated deployment manifest.

```yaml
apiVersion: apps/v1beta2
kind: Deployment
metadata:
  name: nginx-deployment
  labels:
    app: nginx
spec:
  replicas: ${replicas}
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      containers:
      - name: nginx
        image: nginx:1.7.9
        ports:
        - containerPort: 80
```

The Kubernetes resources can then be managed through Terraform.

```terminal
$ terraform apply
# ...
Apply complete! Resources: 1 added, 1 changed, 0 destroyed.
$ kubectl get deployments
NAME               DESIRED   CURRENT   UP-TO-DATE   AVAILABLE   AGE
nginx-deployment   3         3         3            3           1m
$ terraform apply -var 'replicas=5'
# ...
Apply complete! Resources: 0 added, 1 changed, 0 destroyed.
$ kubectl get deployments
NAME               DESIRED   CURRENT   UP-TO-DATE   AVAILABLE   AGE
nginx-deployment   5         5         5            3           3m
$ terraform destroy -force
# ...
Destroy complete! Resources: 2 destroyed.
$ kubectl get deployments
No resources found.
```

**NOTE**: If the YAML formatted `content` contains multiple documents (separated by `---`) only the first non-empty document is going to be parsed. This is because Terraform is mostly designed to represent a single resource on the provider side with a Terraform resource:

> resource types correspond to an infrastructure object type that is managed via a remote network API
> -- <cite>[Terraform Documentation](https://www.terraform.io/docs/configuration/resources.html)</cite>

You can workaround this easily with the following snippet (however we still suggest to use separate resources):

```hcl
locals {
  resources = split("\n---\n", data.template_file.nginx.rendered)
}

resource "k8s_manifest" "nginx-deployment" {
  count = length(local.resources)

  content = local.resources[count.index]
}
```

## Helm workflow

#### Requirements 

- Helm 2 or Helm 3

Get a versioned chart into your source code and render it

##### Helm 2

``` shell
helm fetch stable/nginx-ingress --version 1.24.4 --untardir charts --untar
helm template --namespace nginx-ingress .\charts\nginx-ingress --output-dir manifests/
```

##### Helm 3

``` shell
helm pull stable/nginx-ingress --version 1.24.4 --untardir charts --untar
helm template --namespace nginx-ingress nginx-ingress .\charts\nginx-ingress --output-dir manifests/
```

Apply the `main.tf` with the k8s provider

```hcl2
# terraform 0.12.x
locals {
  nginx-ingress_files   = fileset(path.module, "manifests/nginx-ingress/templates/*.yaml")
}

data "local_file" "nginx-ingress_files_content" {
  for_each = local.nginx-ingress_files
  filename = each.value
}

resource "k8s_manifest" "nginx-ingress" {
  for_each = data.local_file.nginx-ingress_files_content
  content  = each.value.content
  namespace = "nginx"
}
```

## Configuration

There are generally two ways to configure the Kubernetes provider.

### File config

The provider always first tries to load **a config file** from a given
(or default) location. Depending on whether you have current context set
this _may_ require `config_context_auth_info` and/or `config_context_cluster`
and/or `config_context`.

#### Setting default config context

Here's an example for how to set default context and avoid all provider configuration:

```
kubectl config set-context default-system \
  --cluster=chosen-cluster \
  --user=chosen-user

kubectl config use-context default-system
```

Read [more about `kubectl` in the official docs](https://kubernetes.io/docs/user-guide/kubectl-overview/).

### In-cluster service account token

If no other configuration is specified, and when it detects it is running in a kubernetes pod,
the provider will try to use the service account token from the `/var/run/secrets/kubernetes.io/serviceaccount/token` path.
Detection of in-cluster execution is based on the sole availability both of the `KUBERNETES_SERVICE_HOST` and `KUBERNETES_SERVICE_PORT` environment variables,
with non empty values.

```hcl
provider "k8s" {
  load_config_file = "false"
}
```

If you have any other static configuration setting specified in a config file or static configuration, in-cluster service account token will not be tried.

### Statically defined credentials

An other way is **statically** define TLS certificate credentials:

```hcl
provider "k8s" {
  load_config_file = "false"

  host = "https://104.196.242.174"

  client_certificate     = "${file("~/.kube/client-cert.pem")}"
  client_key             = "${file("~/.kube/client-key.pem")}"
  cluster_ca_certificate = "${file("~/.kube/cluster-ca-cert.pem")}"
}
```

or username and password (HTTP Basic Authorization):

```hcl
provider "k8s" {
  load_config_file = "false"

  host = "https://104.196.242.174"

  username = "username"
  password = "password"
}
```


If you have **both** valid configuration in a config file and static configuration, the static one is used as override.
i.e. any static field will override its counterpart loaded from the config.

## Argument Reference

The following arguments are supported:

* `host` - (Optional) The hostname (in form of URI) of Kubernetes master. Can be sourced from `KUBE_HOST`.
* `username` - (Optional) The username to use for HTTP basic authentication when accessing the Kubernetes master endpoint. Can be sourced from `KUBE_USER`.
* `password` - (Optional) The password to use for HTTP basic authentication when accessing the Kubernetes master endpoint. Can be sourced from `KUBE_PASSWORD`.
* `insecure` - (Optional) Whether server should be accessed without verifying the TLS certificate. Can be sourced from `KUBE_INSECURE`. Defaults to `false`.
* `client_certificate` - (Optional) PEM-encoded client certificate for TLS authentication. Can be sourced from `KUBE_CLIENT_CERT_DATA`.
* `client_key` - (Optional) PEM-encoded client certificate key for TLS authentication. Can be sourced from `KUBE_CLIENT_KEY_DATA`.
* `cluster_ca_certificate` - (Optional) PEM-encoded root certificates bundle for TLS authentication. Can be sourced from `KUBE_CLUSTER_CA_CERT_DATA`.
* `config_path` - (Optional) Path to the kube config file. Can be sourced from `KUBE_CONFIG` or `KUBECONFIG`. Defaults to `~/.kube/config`.
* `config_context` - (Optional) Context to choose from the config file. Can be sourced from `KUBE_CTX`.
* `config_context_auth_info` - (Optional) Authentication info context of the kube config (name of the kubeconfig user, `--user` flag in `kubectl`). Can be sourced from `KUBE_CTX_AUTH_INFO`.
* `config_context_cluster` - (Optional) Cluster context of the kube config (name of the kubeconfig cluster, `--cluster` flag in `kubectl`). Can be sourced from `KUBE_CTX_CLUSTER`.
* `token` - (Optional) Token of your service account.  Can be sourced from `KUBE_TOKEN`.
* `load_config_file` - (Optional) By default the local config (~/.kube/config) is loaded when you use this provider. This option at false disables this behaviour which is desired when statically specifying the configuration or relying on in-cluster config. Can be sourced from `KUBE_LOAD_CONFIG_FILE`.
* `exec` - (Optional) Configuration block to use an [exec-based credential plugin] (https://kubernetes.io/docs/reference/access-authn-authz/authentication/#client-go-credential-plugins), e.g. call an external command to receive user credentials.
  * `api_version` - (Required) API version to use when decoding the ExecCredentials resource, e.g. `client.authentication.k8s.io/v1beta1`.
  * `command` - (Required) Command to execute.
  * `args` - (Optional) List of arguments to pass when executing the plugin.
  * `env` - (Optional) Map of environment variables to set when executing the plugin.

## Release

```bash
gpg --fingerprint $MY_EMAIL
export GPG_FINGERPRINT="THEF FING ERPR INTO OFTH  EPUB LICK EYOF YOU!"
goreleaser release --rm-dist -p 2
```

[kubernetes-provider]: https://www.terraform.io/docs/providers/kubernetes/index.html
