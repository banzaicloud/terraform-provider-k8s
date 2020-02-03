package main

import (
	"github.com/banzaicloud/terraform-provider-k8s/k8s"
	"github.com/hashicorp/terraform-plugin-sdk/plugin"
	"github.com/hashicorp/terraform-plugin-sdk/terraform"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{
		ProviderFunc: func() terraform.ResourceProvider {
			return k8s.Provider()
		},
	})
}
