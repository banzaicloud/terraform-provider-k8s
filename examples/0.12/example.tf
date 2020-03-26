data "template_file" "my-configmap" {
  template = file("${path.module}/../manifests/my-configmap.yaml")

  vars = {
    greeting = var.greeting
  }
}

resource "k8s_manifest" "my-configmap" {
  content = data.template_file.my-configmap.rendered
}

data "template_file" "nginx-deployment" {
  template = file("${path.module}/../manifests/nginx-deployment.yaml")

  vars = {
    replicas = var.replicas
  }
}

resource "k8s_manifest" "nginx-deployment" {
  content = data.template_file.nginx-deployment.rendered
}

data "template_file" "nginx-namespace" {
  template = file("${path.module}/../manifests/nginx-namespace.yaml")
}

resource "k8s_manifest" "nginx-namespace" {
  content   = data.template_file.nginx-namespace.rendered
  namespace = "kube-system"
}

resource "k8s_manifest" "nginx-deployment-with-namespace" {
  content   = data.template_file.nginx-deployment.rendered
  namespace = "nginx"
}

data "template_file" "nginx-service" {
  template = file("${path.module}/../manifests/nginx-service.yaml")
}

resource "k8s_manifest" "nginx-service" {
  content   = data.template_file.nginx-service.rendered
  namespace = "nginx"
}

data "template_file" "nginx-ingress" {
  template = file("${path.module}/../manifests/nginx-ingress.yaml")
}

resource "k8s_manifest" "nginx-ingress" {
  content   = data.template_file.nginx-ingress.rendered
  namespace = "nginx"
}

data "template_file" "nginx-pvc" {
  template = file("${path.module}/../manifests/nginx-pvc.yaml")
}

resource "k8s_manifest" "nginx-pvc" {
  content   = data.template_file.nginx-pvc.rendered
  namespace = "nginx"
}

resource "k8s_manifest" "crontab-crd" {
    content   = file("${path.module}/../manifests/crontab-crd.yaml")
}

resource "k8s_manifest" "crontab-resource" {
    content   = file("${path.module}/../manifests/crontab-resource.yaml")
    namespace = "nginx"
    depends_on = [k8s_manifest.crontab-crd]
}
