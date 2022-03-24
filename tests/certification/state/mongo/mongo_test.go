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

package mongocertification_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	// State.

	"github.com/dapr/components-contrib/state"
	state_mongodb "github.com/dapr/components-contrib/state/mongodb"
	state_loader "github.com/dapr/dapr/pkg/components/state"

	// Dapr runtime and Go-SDK
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/kit/logger"

	// Certification testing runnables
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"

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
	ports, err := dapr_testing.GetFreePorts(3)
	assert.NoError(t, err)

	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	startTestApp := func(ctx flow.Context) error {
		go func() {
			main()
		}()
		return nil
	}

	reminderTest := func(ctx flow.Context) error {
		// make http post request to the test app

		values := map[string]string{"data": "reminderdata", "dueTime": "1s", "period": "1s"}
		json_data, err := json.Marshal(values)
		if err != nil {
			log.Fatal(err)
		}

		resp, err := http.Post("http://localhost:3000/test/testactorfeatures/10/reminders/myReminder", "application/json",
			bytes.NewBuffer(json_data))
		if err != nil {
			log.Fatal(err)
		}

		if resp.StatusCode > 299 {
			log.Fatal("Reminder test failed")
		}

		time.Sleep(time.Second * 5)

		return nil
	}

	flow.New(t, "Mongo certification using Mongo Docker").
		// Run Mongo and Placement using Docker Compose.
		// Step(dockercompose.Run("mongo", dockerComposeYAML)).
		Step("Run Actors App", startTestApp).

		// Run the Dapr sidecar with the Mongo component.
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault",
			embedded.WithPlacementAddresses([]string{"localhost:50005"}),
			embedded.WithAppProtocol(runtime.HTTPProtocol, 3000),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(3500),
			embedded.WithComponentsPath("components"),
			runtime.WithStates(
				state_loader.New("mongodb", func() state.Store {
					return state_mongodb.NewMongoDB(log)
				}),
			))).
		Step("Create Reminders", reminderTest).
		// Step("stopping the sidecar", sidecar.Stop(sidecarNamePrefix+"dockerDefault")).
		Run()

	flow.New(t, "Mongo certification using Mongo Docker again").
		Step(sidecar.Run(sidecarNamePrefix+"dockerDefault2",
			embedded.WithPlacementAddresses([]string{"localhost:50005"}),
			embedded.WithAppProtocol(runtime.HTTPProtocol, 3000),
			embedded.WithDaprGRPCPort(ports[2]),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			embedded.WithComponentsPath("components"),
			runtime.WithStates(
				state_loader.New("mongodb", func() state.Store {
					return state_mongodb.NewMongoDB(log)
				}),
			))).
		Step("wait", func(ctx flow.Context) error {
			time.Sleep(time.Second * 5)
			return nil
		}).
		// Step("Stopping Mongo Container", dockercompose.Stop("mongo", dockerComposeYAML)).
		Run()
}
