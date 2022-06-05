// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package k8se

import (
	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/config"
	"github.com/dapr/kit/logger"
)

const (
	dnsSuffixKey     = "dnsSuffix"
	dnsPortKey       = "dnsPort"
	DefaultDnsSuffix = "dapr"
	DefaultDnsPort   = "80"
)

type resolver struct {
	logger    logger.Logger
	dnsSuffix string
	dnsPort   string
}

// NewResolver creates Kubernetes name resolver.
func NewResolver(logger logger.Logger) nameresolution.Resolver {
	return &resolver{
		logger:    logger,
		dnsSuffix: DefaultDnsSuffix,
		dnsPort:   DefaultDnsPort,
	}
}

// Init initializes Kubernetes name resolver.
func (k *resolver) Init(metadata nameresolution.Metadata) error {
	configInterface, err := config.Normalize(metadata.Configuration)
	if err != nil {
		return err
	}
	if config, ok := configInterface.(map[string]string); ok {
		dnsSuffix := config[dnsSuffixKey]
		if dnsSuffix != "" {
			k.dnsSuffix = dnsSuffix
		}
		dnsPort := config[dnsPortKey]
		if dnsPort != "" {
			k.dnsPort = dnsPort
		}

	}

	return nil
}

// ResolveID resolves name to address in Kubernetes.
func (k *resolver) ResolveID(req nameresolution.ResolveRequest) (string, error) {
	// K8se requires this formatting for Dapr services
	return req.ID + "." + k.dnsSuffix + ":" + k.dnsPort, nil
}
