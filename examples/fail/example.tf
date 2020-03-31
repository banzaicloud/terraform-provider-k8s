
data "template_file" "nginx-deployment" {
  template = file("${path.module}/../manifests/nginx-deployment-fail.yaml")

  vars = {
    replicas = var.replicas
  }
}

resource "k8s_manifest" "nginx-deployment" {
    content = data.template_file.nginx-deployment.rendered

    timeouts {
        create = "20s"
    }
}
