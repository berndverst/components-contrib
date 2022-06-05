// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package k8se

import (
	"testing"

	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

func TestResolve(t *testing.T) {
	resolver := NewResolver(logger.NewLogger("test"))
	request := nameresolution.ResolveRequest{ID: "myid", Namespace: "abc", Port: 1234}

	const expect = "myid.dapr:80"
	target, err := resolver.ResolveID(request)

	assert.NoError(t, err)
	assert.Equal(t, expect, target)
}

func TestResolveWithCustomClusterDomain(t *testing.T) {
	resolver := NewResolver(logger.NewLogger("test"))
	_ = resolver.Init(nameresolution.Metadata{
		Configuration: map[string]string{
			"dnsSuffix": "k8se-envoy-dapr",
			"dnsPort":   "80",
		},
	})
	request := nameresolution.ResolveRequest{ID: "myid", Namespace: "abc", Port: 1234}

	const expect = "myid.k8se-envoy-dapr:80"
	target, err := resolver.ResolveID(request)

	assert.NoError(t, err)
	assert.Equal(t, expect, target)
}
