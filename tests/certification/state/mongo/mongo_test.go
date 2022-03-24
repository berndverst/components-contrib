/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mongo_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	// State.

	"github.com/dapr/components-contrib/state"
	state_mongodb "github.com/dapr/components-contrib/state/mongodb"
	state_loader "github.com/dapr/dapr/pkg/components/state"

	// Dapr runtime and Go-SDK
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"

	// Certification testing runnables
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"

	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
)

const (
	sidecarNamePrefix       = "mongo-sidecar-"
	dockerComposeYAML       = "docker-compose.yml"
	stateStoreName          = "dapr-state-store"
	certificationTestPrefix = "stable-certification-"
)

func TestMongo(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort := ports[0]
	// currentHTTPPort := ports[1]

	basicTest := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		// save state, default options: strong, last-write
		err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"key1", []byte("certificationdata"))
		assert.NoError(t, err)

		// get state
		item, err := client.GetState(ctx, stateStoreName, certificationTestPrefix+"key1")
		assert.NoError(t, err)
		assert.Equal(t, "certificationdata", string(item.Value))

		// delete state
		err = client.DeleteState(ctx, stateStoreName, certificationTestPrefix+"key1")
		assert.NoError(t, err)

		return nil
	}

	flow.New(t, "Mongo certification using Mongo Docker").
		// Run SQL Server using Docker Compose.
		Step(dockercompose.Run("mongo", dockerComposeYAML)).

		// Run the Dapr sidecar with the SQL Server component.
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(3500),
			embedded.WithComponentsPath("components"),
			runtime.WithStates(
				state_loader.New("mongo", func() state.Store {
					return state_mongodb.NewMongoDB(log)
				}),
			))).
		Step("Run basic test", basicTest).
		Step("Stopping SQL Server Docker container", dockercompose.Stop("sqlserver", dockerComposeYAML)).
		Run()
}
