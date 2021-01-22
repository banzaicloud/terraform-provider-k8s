module github.com/banzaicloud/terraform-provider-k8s

go 1.13

require (
	github.com/evanphx/json-patch v4.5.0+incompatible
	github.com/hashicorp/terraform-plugin-sdk v1.15.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mitchellh/mapstructure v1.3.3
	github.com/pkg/errors v0.8.1
	go.uber.org/zap v1.15.0 // indirect
	k8s.io/apimachinery v0.18.6
	k8s.io/client-go v0.18.6
	k8s.io/kubectl v0.18.6
	sigs.k8s.io/controller-runtime v0.6.2
)
