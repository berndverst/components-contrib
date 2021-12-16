// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package azureblobstoragebinding_test

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/azure/blobstorage"
	"github.com/dapr/components-contrib/secretstores"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	daprsdk "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
)

const (
	sidecarName = "blobstorage-sidecar"
)

func TestBlobStorage(t *testing.T) {
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGRPCPort := ports[0]
	currentHTTPPort := ports[1]

	log := logger.NewLogger("dapr.components")

	testCreateBlobFromFile := func(ctx flow.Context) error {
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		dataBytes, err := os.ReadFile("dapr.svg")

		assert.NoError(t, err)

		invokeCreateRequest := &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "create",
			Data:      dataBytes,
			Metadata:  nil,
		}

		out, invokeCreateErr := client.InvokeBinding(ctx, invokeCreateRequest)
		assert.NoError(t, invokeCreateErr)
		fmt.Println(out.Metadata["blobName"])

		return nil
	}

	testCreateGetListDelete := func(ctx flow.Context) error {
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		input := "some example content"
		dataBytes := []byte(input)
		h := md5.New()
		h.Write(dataBytes)
		md5HashBase64 := base64.StdEncoding.EncodeToString(h.Sum(nil))

		invokeCreateMetadata := map[string]string{
			"blobName":           "filename.txt",
			"contentType":        "text/plain",
			"contentMD5":         md5HashBase64,
			"contentEncoding":    "UTF-8",
			"contentLanguage":    "en-us",
			"contentDisposition": "attachment",
			"cacheControl":       "no-cache",
			"custom":             "hello-world",
		}

		invokeCreateRequest := &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "create",
			Data:      dataBytes,
			Metadata:  invokeCreateMetadata,
		}

		_, invokeCreateErr := client.InvokeBinding(ctx, invokeCreateRequest)

		assert.NoError(t, invokeCreateErr)

		invokeGetMetadata := map[string]string{
			"blobName":        "filename.txt",
			"includeMetadata": "true",
		}

		invokeGetRequest := &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "get",
			Data:      nil,
			Metadata:  invokeGetMetadata,
		}

		out, invokeGetErr := client.InvokeBinding(ctx, invokeGetRequest)
		assert.NoError(t, invokeGetErr)
		assert.Equal(t, string(out.Data), input)
		assert.Contains(t, out.Metadata, "custom")
		assert.Equal(t, out.Metadata["custom"], "hello-world")

		invokeListRequest := &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "list",
			Data:      nil,
			Metadata:  nil,
		}

		out, invokeErr := client.InvokeBinding(ctx, invokeListRequest)
		assert.NoError(t, invokeErr)
		var output []map[string]interface{}
		err := json.Unmarshal(out.Data, &output)
		assert.NoError(t, err)

		found := false
		for _, item := range output {
			if item["Name"] == "filename.txt" {
				found = true
				properties := item["Properties"].(map[string]interface{})
				assert.Equal(t, properties["ContentMD5"], invokeCreateMetadata["contentMD5"])
				assert.Equal(t, properties["ContentType"], invokeCreateMetadata["contentType"])
				assert.Equal(t, properties["CacheControl"], invokeCreateMetadata["cacheControl"])
				assert.Equal(t, properties["ContentDisposition"], invokeCreateMetadata["contentDisposition"])
				assert.Equal(t, properties["ContentEncoding"], invokeCreateMetadata["contentEncoding"])
				assert.Equal(t, properties["ContentLanguage"], invokeCreateMetadata["contentLanguage"])
				break
			}
		}
		assert.True(t, found)

		return nil
	}

	invokeListContents := func(ctx flow.Context) error {
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		requestOptions := make(map[string]interface{})
		optionsBytes, marshalErr := json.Marshal(requestOptions)
		if marshalErr != nil {
			return marshalErr
		}

		invokeRequest := &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "list",
			Data:      optionsBytes,
			Metadata:  nil,
		}

		out, invokeErr := client.InvokeBinding(ctx, invokeRequest)
		assert.NoError(t, invokeErr)
		fmt.Println(string(out.Data))

		return invokeErr
	}

	flow.New(t, "blobstorage binding authentication using service principal").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/serviceprincipal"),
			embedded.WithDaprGRPCPort(currentGRPCPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
			),
			runtime.WithOutputBindings(
				bindings_loader.NewOutput("azure.blobstorage", func() bindings.OutputBinding {
					return blobstorage.NewAzureBlobStorage(log)
				}),
			))).
		Step("Create blob", testCreateGetListDelete).
		Run()

	ports, err = dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGRPCPort = ports[0]
	currentHTTPPort = ports[1]

	flow.New(t, "blobstorage binding authentication using access key").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/accesskey"),
			embedded.WithDaprGRPCPort(currentGRPCPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
			),
			runtime.WithOutputBindings(
				bindings_loader.NewOutput("azure.blobstorage", func() bindings.OutputBinding {
					return blobstorage.NewAzureBlobStorage(log)
				}),
			))).
		Step("Create blob", testCreateGetListDelete).
		Step("Create blob from file", testCreateBlobFromFile).
		Step("List contents", invokeListContents).
		Run()
}
