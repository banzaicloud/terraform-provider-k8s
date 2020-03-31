module github.com/banzaicloud/terraform-provider-k8s

go 1.13

require (
	github.com/hashicorp/terraform-plugin-sdk v1.7.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mitchellh/mapstructure v1.1.2
	go.uber.org/zap v1.10.0 // indirect
	google.golang.org/grpc v1.23.1 // indirect
	k8s.io/apimachinery v0.17.2
	k8s.io/client-go v0.17.2
	k8s.io/kubectl v0.17.2
	sigs.k8s.io/controller-runtime v0.4.0
)
