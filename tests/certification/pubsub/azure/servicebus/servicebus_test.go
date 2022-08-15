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

package servicebus_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	// Pub-Sub.
	"github.com/dapr/components-contrib/pubsub"
	pubsub_servicebus "github.com/dapr/components-contrib/pubsub/azure/servicebus"
	"github.com/dapr/components-contrib/secretstores"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"

	// Dapr runtime and Go-SDK
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"

	// Certification testing runnables
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/simulate"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"
)

const (
	sidecarName1 = "dapr-1"
	sidecarName2 = "dapr-2"

	appID1 = "app-1"
	appID2 = "app-2"

	numMessages      = 10
	appPort          = 8000
	portOffset       = 2
	messageKey       = "partitionKey"
	pubsubName       = "messagebus"
	topicActiveName  = "certification-pubsub-topic-active"
	topicPassiveName = "certification-pubsub-topic-passive"
	topicToBeCreated = "certification-topic-per-test-run"
	topicDefaultName = "certification-topic-default"
	partition0       = "partition-0"
	partition1       = "partition-1"
)

func TestServicebus(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	component := pubsub_loader.New("azure.servicebus", func() pubsub.PubSub {
		return pubsub_servicebus.NewAzureServiceBus(log)
	})

	secretStoreComponent := secretstores_loader.New("local.env", func() secretstores.SecretStore {
		return secretstore_env.NewEnvSecretStore(log)
	})

	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	// subscriber of the given topic
	subscriberApplication := func(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, 100)
			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      topicName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					if err := sim(); err != nil {
						return true, err
					}

					// Track/Observe the data of the event.
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
			}

			// add the messages as expectations to the watchers
			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, topicName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, topicName, message)
				}
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}

			return nil
		}
	}

	flow.New(t, "servicebus certification basic test").

		// Test : single publisher, multiple subscriber with their own consumerID
		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).

		// Run the Dapr sidecar with the eventhubs component 1, with permission at namespace level
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer_one"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			runtime.WithSecretStores(secretStoreComponent),
			runtime.WithPubSubs(component))).

		// Run subscriberApplication app2
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID2, topicActiveName, consumerGroup2))).

		// Run the Dapr sidecar with the component 2.
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/consumer_two"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			runtime.WithSecretStores(secretStoreComponent),
			runtime.WithPubSubs(component))).
		Step("publish messages to topic1", publishMessages(nil, sidecarName1, topicActiveName, consumerGroup1, consumerGroup2)).
		Step("publish messages to unUsedTopic", publishMessages(nil, sidecarName1, topicPassiveName)).
		Step("verify if app1 has recevied messages published to active topic", assertMessages(10*time.Second, consumerGroup1)).
		Step("verify if app2 has recevied messages published to passive topic", assertMessages(10*time.Second, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

func TestServicebusMultipleSubsSameConsumerIDs(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	component := pubsub_loader.New("azure.servicebus", func() pubsub.PubSub {
		return pubsub_servicebus.NewAzureServiceBus(log)
	})

	secretStoreComponent := secretstores_loader.New("local.env", func() secretstores.SecretStore {
		return secretstore_env.NewEnvSecretStore(log)
	})

	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	metadata1 := map[string]string{
		messageKey: partition1,
	}

	// subscriber of the given topic
	subscriberApplication := func(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, 100)
			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      topicName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					if err := sim(); err != nil {
						return true, err
					}

					// Track/Observe the data of the event.
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
			}

			// add the messages as expectations to the watchers
			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, topicName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, topicName, message)
				}
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}

			return nil
		}
	}

	flow.New(t, "servicebus certification - single publisher and multiple subscribers with same consumer IDs").

		// Test : single publisher, multiple subscriber with their own consumerID
		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).

		// Run the Dapr sidecar with the eventhubs component 1, with permission at namespace level
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer_one"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			runtime.WithSecretStores(secretStoreComponent),
			runtime.WithPubSubs(component))).

		// Run subscriberApplication app2
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID2, topicActiveName, consumerGroup2))).

		// Run the Dapr sidecar with the component 2.
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/consumer_two"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			runtime.WithSecretStores(secretStoreComponent),
			runtime.WithPubSubs(component))).
		Step("publish messages to topic1", publishMessages(metadata, sidecarName1, topicActiveName, consumerGroup2)).
		Step("publish messages to topic1", publishMessages(metadata1, sidecarName2, topicActiveName, consumerGroup2)).
		Step("verify if app1, app2 together have recevied messages published to topic1", assertMessages(10*time.Second, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

func TestServicebusMultipleSubsDifferentConsumerIDs(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	component := pubsub_loader.New("azure.servicebus", func() pubsub.PubSub {
		return pubsub_servicebus.NewAzureServiceBus(log)
	})

	secretStoreComponent := secretstores_loader.New("local.env", func() secretstores.SecretStore {
		return secretstore_env.NewEnvSecretStore(log)
	})

	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	// subscriber of the given topic
	subscriberApplication := func(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, 100)
			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      topicName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					if err := sim(); err != nil {
						return true, err
					}

					// Track/Observe the data of the event.
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
			}

			// add the messages as expectations to the watchers
			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, topicName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, topicName, message)
				}
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}

			return nil
		}
	}

	flow.New(t, "servicebus certification - single publisher and multiple subscribers with different consumer IDs").

		// Test : single publisher, multiple subscriber with their own consumerID
		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).

		// Run the Dapr sidecar with the eventhubs component 1, with permission at namespace level
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer_one"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			runtime.WithSecretStores(secretStoreComponent),
			runtime.WithPubSubs(component))).

		// Run subscriberApplication app2
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID2, topicActiveName, consumerGroup2))).

		// Run the Dapr sidecar with the component 2.
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/consumer_two"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			runtime.WithSecretStores(secretStoreComponent),
			runtime.WithPubSubs(component))).
		Step("publish messages to topic1", publishMessages(metadata, sidecarName1, topicActiveName, consumerGroup1)).
		Step("verify if app1, app2 together have recevied messages published to topic1", assertMessages(10*time.Second, consumerGroup1)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

func TestServicebusMultiplePubSubsDifferentConsumerIDs(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	component := pubsub_loader.New("azure.servicebus", func() pubsub.PubSub {
		return pubsub_servicebus.NewAzureServiceBus(log)
	})

	secretStoreComponent := secretstores_loader.New("local.env", func() secretstores.SecretStore {
		return secretstore_env.NewEnvSecretStore(log)
	})

	consumerGroup1 := watcher.NewUnordered()
	consumerGroup2 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	metadata1 := map[string]string{
		messageKey: partition1,
	}

	// subscriber of the given topic
	subscriberApplication := func(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, 100)
			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      topicName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					if err := sim(); err != nil {
						return true, err
					}

					// Track/Observe the data of the event.
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
			}

			// add the messages as expectations to the watchers
			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, topicName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, topicName, message)
				}
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}

			return nil
		}
	}

	flow.New(t, "servicebus certification - multiple publishers and multiple subscribers with different consumer IDs").

		// Test : single publisher, multiple subscriber with their own consumerID
		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).

		// Run the Dapr sidecar with the eventhubs component 1, with permission at namespace level
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer_one"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			runtime.WithSecretStores(secretStoreComponent),
			runtime.WithPubSubs(component))).

		// Run subscriberApplication app2
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			subscriberApplication(appID2, topicActiveName, consumerGroup2))).

		// Run the Dapr sidecar with the component 2.
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/consumer_two"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			runtime.WithSecretStores(secretStoreComponent),
			runtime.WithPubSubs(component))).
		Step("publish messages to topic1", publishMessages(metadata, sidecarName1, topicActiveName, consumerGroup1)).
		Step("publish messages to topic1", publishMessages(metadata1, sidecarName2, topicActiveName, consumerGroup2)).
		Step("verify if app1, app2 together have recevied messages published to topic1", assertMessages(10*time.Second, consumerGroup1)).
		Step("verify if app1, app2 together have recevied messages published to topic1", assertMessages(10*time.Second, consumerGroup2)).
		Step("reset", flow.Reset(consumerGroup1, consumerGroup2)).
		Run()
}

func TestServicebusNonexistingTopic(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	component := pubsub_loader.New("azure.servicebus", func() pubsub.PubSub {
		return pubsub_servicebus.NewAzureServiceBus(log)
	})

	secretStoreComponent := secretstores_loader.New("local.env", func() secretstores.SecretStore {
		return secretstore_env.NewEnvSecretStore(log)
	})

	consumerGroup1 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	// subscriber of the given topic
	subscriberApplication := func(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, 100)
			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      topicName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					if err := sim(); err != nil {
						return true, err
					}

					// Track/Observe the data of the event.
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
			}

			// add the messages as expectations to the watchers
			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, topicName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, topicName, message)
				}
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}

			return nil
		}
	}

	flow.New(t, "servicebus certification - non-existing topic").

		// Test : Entitymanagement , Test partition key, in order processing with single publisher/subscriber
		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset*3),
			subscriberApplication(appID1, topicToBeCreated, consumerGroup1))).

		// Run the Dapr sidecar with the component entitymanagement
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer_one"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset*3),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset*3),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset*3),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset*3),
			runtime.WithSecretStores(secretStoreComponent),
			runtime.WithPubSubs(component))).
		Step(fmt.Sprintf("publish messages to topicToBeCreated: %s", topicToBeCreated), publishMessages(metadata, sidecarName1, topicToBeCreated, consumerGroup1)).
		// Step("interrupt network", network.InterruptNetwork(time.Minute, []string{}, []string{}, "5671", "5672")).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("verify if app1 has recevied messages published to newly created topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

func TestServicebusNetworkInterruption(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	component := pubsub_loader.New("azure.servicebus", func() pubsub.PubSub {
		return pubsub_servicebus.NewAzureServiceBus(log)
	})

	secretStoreComponent := secretstores_loader.New("local.env", func() secretstores.SecretStore {
		return secretstore_env.NewEnvSecretStore(log)
	})

	consumerGroup1 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	// subscriber of the given topic
	subscriberApplication := func(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, 100)
			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      topicName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					if err := sim(); err != nil {
						return true, err
					}

					// Track/Observe the data of the event.
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
			}

			// add the messages as expectations to the watchers
			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, topicName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, topicName, message)
				}
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}

			return nil
		}
	}

	flow.New(t, "servicebus certification - network interruption").

		// Test : Entitymanagement , Test partition key, in order processing with single publisher/subscriber
		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset*3),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).

		// Run the Dapr sidecar with the component entitymanagement
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/consumer_one"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset*3),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset*3),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset*3),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset*3),
			runtime.WithSecretStores(secretStoreComponent),
			runtime.WithPubSubs(component))).
		Step(fmt.Sprintf("publish messages to topicToBeCreated: %s", topicActiveName), publishMessages(metadata, sidecarName1, topicActiveName, consumerGroup1)).
		Step("interrupt network", network.InterruptNetwork(time.Minute, []string{}, []string{}, "5671", "5672")).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("verify if app1 has recevied messages published to newly created topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

func TestServicebusEntityManagement(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	consumerGroup1 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	publishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
			}

			// add the messages as expectations to the watchers
			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, topicName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, topicName, message)
				}
				require.Error(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	testDefault := func(ctx flow.Context) error {
		ports, _ := dapr_testing.GetFreePorts(6)
		grpcPortS1 := ports[0]
		httpPortS1 := ports[1]
		profilePortS1 := ports[2]
		sidecar := sidecar.New(sidecarName1, embedded.WithComponentsPath("./components/entity_mgnt"),
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(grpcPortS1),
			embedded.WithDaprHTTPPort(httpPortS1),
			embedded.WithProfilePort(profilePortS1),
			runtime.WithPubSubs(
				pubsub_loader.New("azure.servicebus", func() pubsub.PubSub {
					return pubsub_servicebus.NewAzureServiceBus(log)
				}),
			),
			runtime.WithSecretStores(secretstores_loader.New("local.env", func() secretstores.SecretStore {
				return secretstore_env.NewEnvSecretStore(log)
			})))
		sidecar.Start(ctx)
		return nil
	}

	flow.New(t, "servicebus certification - entity management enabled").

		// Test : disableEntityManagement optional attribute , Test processing with all optional attributes and single publisher/subscriber
		// Run subscriberApplication app1
		Step("verify if app1 has recevied messages published to newly created topic", testDefault).
		Step(fmt.Sprintf("publish messages to topicDefault: %s", topicDefaultName), publishMessages(metadata, sidecarName1, topicDefaultName, consumerGroup1)).
		Run()
}

func TestServicebusDefaultTtl(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	component := pubsub_loader.New("azure.servicebus", func() pubsub.PubSub {
		return pubsub_servicebus.NewAzureServiceBus(log)
	})

	secretStoreComponent := secretstores_loader.New("local.env", func() secretstores.SecretStore {
		return secretstore_env.NewEnvSecretStore(log)
	})

	consumerGroup1 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	// subscriber of the given topic
	subscriberApplication := func(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, 100)
			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      topicName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					if err := sim(); err != nil {
						return true, err
					}

					// Track/Observe the data of the event.
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	testTtlPublishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
			}

			// add the messages as expectations to the watchers
			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, topicName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, topicName, message)
				}
				require.NoError(ctx, err, "error publishing message")
			}
			// Wait for double the TTL after sending the last message.
			time.Sleep(time.Second * 20)
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}

			return nil
		}
	}

	flow.New(t, "servicebus certification").

		// Test : defaultMessageTimeToLiveInSec optional attribute, in order processing with single publisher/subscriber
		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset*5),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).

		// Run the Dapr sidecar with the component entitymanagement
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/default_ttl"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset*5),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset*5),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset*5),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset*5),
			runtime.WithSecretStores(secretStoreComponent),
			runtime.WithPubSubs(component))).
		Step(fmt.Sprintf("publish messages to topicToBeCreated: %s", topicActiveName), testTtlPublishMessages(metadata, sidecarName1, topicActiveName, consumerGroup1)).
		Step("verify if app6 has recevied messages published to newly created topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}

func TestServicebusAuthentication(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	component := pubsub_loader.New("azure.servicebus", func() pubsub.PubSub {
		return pubsub_servicebus.NewAzureServiceBus(log)
	})

	secretStoreComponent := secretstores_loader.New("local.env", func() secretstores.SecretStore {
		return secretstore_env.NewEnvSecretStore(log)
	})

	consumerGroup1 := watcher.NewUnordered()

	// Set the partition key on all messages so they are written to the same partition. This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: partition0,
	}

	// subscriber of the given topic
	subscriberApplication := func(appID string, topicName string, messagesWatcher *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, 100)
			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: pubsubName,
					Topic:      topicName,
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					if err := sim(); err != nil {
						return true, err
					}

					// Track/Observe the data of the event.
					messagesWatcher.Observe(e.Data)
					ctx.Logf("Message Received appID: %s,pubsub: %s, topic: %s, id: %s, data: %s", appID, e.PubsubName, e.Topic, e.ID, e.Data)
					return false, nil
				}),
			)
		}
	}

	publishMessages := func(metadata map[string]string, sidecarName string, topicName string, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// prepare the messages
			messages := make([]string, numMessages)
			for i := range messages {
				messages[i] = fmt.Sprintf("partitionKey: %s, message for topic: %s, index: %03d, uniqueId: %s", metadata[messageKey], topicName, i, uuid.New().String())
			}

			// add the messages as expectations to the watchers
			for _, messageWatcher := range messageWatchers {
				messageWatcher.ExpectStrings(messages...)
			}

			// get the sidecar (dapr) client
			client := sidecar.GetClient(ctx, sidecarName)

			// publish messages
			ctx.Logf("Publishing messages. sidecarName: %s, topicName: %s", sidecarName, topicName)

			var publishOptions dapr.PublishEventOption

			if metadata != nil {
				publishOptions = dapr.PublishEventWithMetadata(metadata)
			}

			for _, message := range messages {
				ctx.Logf("Publishing: %q", message)
				var err error

				if publishOptions != nil {
					err = client.PublishEvent(ctx, pubsubName, topicName, message, publishOptions)
				} else {
					err = client.PublishEvent(ctx, pubsubName, topicName, message)
				}
				require.NoError(ctx, err, "error publishing message")
			}
			return nil
		}
	}

	assertMessages := func(timeout time.Duration, messageWatchers ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// assert for messages
			for _, m := range messageWatchers {
				m.Assert(ctx, 25*timeout)
			}

			return nil
		}
	}

	flow.New(t, "servicebus certification - authentication").

		// Test : Entitymanagement , Test partition key, in order processing with single publisher/subscriber
		// Run subscriberApplication app1
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort+portOffset*3),
			subscriberApplication(appID1, topicActiveName, consumerGroup1))).

		// Run the Dapr sidecar with the component entitymanagement
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/authentication"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset*3),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset*3),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset*3),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset*3),
			runtime.WithSecretStores(secretStoreComponent),
			runtime.WithPubSubs(component))).
		Step(fmt.Sprintf("publish messages to topicToBeCreated: %s", topicActiveName), publishMessages(metadata, sidecarName1, topicActiveName, consumerGroup1)).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("verify if app1 has recevied messages published to newly created topic", assertMessages(10*time.Second, consumerGroup1)).
		Run()
}
