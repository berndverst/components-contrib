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

package rabbitmq

import (
	"fmt"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"

	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

func getFakeProperties() map[string]string {
	props := map[string]string{}
	props[metadataConnectionStringKey] = "fakeconnectionstring"
	props[metadataProtocolKey] = "fakeprotocol"
	props[metadataHostnameKey] = "fakehostname"
	props[metadataUsernameKey] = "fakeusername"
	props[metadataPasswordKey] = "fakepassword"
	props[metadataConsumerIDKey] = "fakeConsumerID"

	return props
}

func TestCreateMetadata(t *testing.T) {
	log := logger.NewLogger("test")

	booleanFlagTests := []struct {
		in       string
		expected bool
	}{
		{"true", true},
		{"TRUE", true},
		{"false", false},
		{"FALSE", false},
	}

	t.Run("metadata is correct", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}

		// act
		m, err := createMetadata(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[metadataConnectionStringKey], m.ConnectionString)
		assert.Equal(t, fakeProperties[metadataProtocolKey], m.Protocol)
		assert.Equal(t, fakeProperties[metadataHostnameKey], m.Hostname)
		assert.Equal(t, fakeProperties[metadataUsernameKey], m.Username)
		assert.Equal(t, fakeProperties[metadataPasswordKey], m.Password)
		assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.ConsumerID)
		assert.Equal(t, false, m.AutoAck)
		assert.Equal(t, false, m.RequeueInFailure)
		assert.Equal(t, true, m.DeleteWhenUnused)
		assert.Equal(t, false, m.EnableDeadLetter)
		assert.Equal(t, false, m.PublisherConfirm)
		assert.Equal(t, uint8(0), m.DeliveryMode)
		assert.Equal(t, uint8(0), m.PrefetchCount)
		assert.Equal(t, int64(0), m.MaxLen)
		assert.Equal(t, int64(0), m.MaxLenBytes)
		assert.Equal(t, fanoutExchangeKind, m.ExchangeKind)
	})

	invalidDeliveryModes := []string{"3", "10", "-1"}

	for _, deliveryMode := range invalidDeliveryModes {
		t.Run(fmt.Sprintf("deliveryMode value=%s", deliveryMode), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Base: mdata.Base{Properties: fakeProperties},
			}
			fakeMetaData.Properties[metadataDeliveryModeKey] = deliveryMode

			// act
			m, err := createMetadata(fakeMetaData, log)

			// assert
			assert.EqualError(t, err, "rabbitmq pub/sub error: invalid RabbitMQ delivery mode, accepted values are between 0 and 2")
			assert.Equal(t, fakeProperties[metadataHostnameKey], m.Hostname)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.ConsumerID)
			assert.Equal(t, uint8(0), m.DeliveryMode)
		})
	}

	t.Run("deliveryMode is set", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		fakeMetaData.Properties[metadataDeliveryModeKey] = "2"

		// act
		m, err := createMetadata(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[metadataHostnameKey], m.Hostname)
		assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.ConsumerID)
		assert.Equal(t, uint8(2), m.DeliveryMode)
	})

	t.Run("invalid concurrency", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		fakeMetaData.Properties[pubsub.ConcurrencyKey] = "a"

		// act
		_, err := createMetadata(fakeMetaData, log)

		// assert
		assert.Error(t, err)
	})

	t.Run("prefetchCount is set", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		fakeMetaData.Properties[metadataPrefetchCountKey] = "1"

		// act
		m, err := createMetadata(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[metadataHostnameKey], m.Hostname)
		assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.ConsumerID)
		assert.Equal(t, uint8(1), m.PrefetchCount)
	})

	t.Run("maxLen and maxLenBytes is set", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		fakeMetaData.Properties[metadataMaxLenKey] = "1"
		fakeMetaData.Properties[metadataMaxLenBytesKey] = "2000000"

		// act
		m, err := createMetadata(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[metadataHostnameKey], m.Hostname)
		assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.ConsumerID)
		assert.Equal(t, int64(1), m.MaxLen)
		assert.Equal(t, int64(2000000), m.MaxLenBytes)
	})

	for _, tt := range booleanFlagTests {
		t.Run(fmt.Sprintf("autoAck value=%s", tt.in), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Base: mdata.Base{Properties: fakeProperties},
			}
			fakeMetaData.Properties[metadataAutoAckKey] = tt.in

			// act
			m, err := createMetadata(fakeMetaData, log)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, fakeProperties[metadataHostnameKey], m.Hostname)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.ConsumerID)
			assert.Equal(t, tt.expected, m.AutoAck)
		})
	}

	for _, tt := range booleanFlagTests {
		t.Run(fmt.Sprintf("requeueInFailure value=%s", tt.in), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Base: mdata.Base{Properties: fakeProperties},
			}
			fakeMetaData.Properties[metadataRequeueInFailureKey] = tt.in

			// act
			m, err := createMetadata(fakeMetaData, log)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, fakeProperties[metadataHostnameKey], m.Hostname)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.ConsumerID)
			assert.Equal(t, tt.expected, m.RequeueInFailure)
		})
	}

	for _, tt := range booleanFlagTests {
		t.Run(fmt.Sprintf("deleteWhenUnused value=%s", tt.in), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Base: mdata.Base{Properties: fakeProperties},
			}
			fakeMetaData.Properties[metadataDeleteWhenUnusedKey] = tt.in

			// act
			m, err := createMetadata(fakeMetaData, log)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, fakeProperties[metadataHostnameKey], m.Hostname)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.ConsumerID)
			assert.Equal(t, tt.expected, m.DeleteWhenUnused)
		})
	}

	for _, tt := range booleanFlagTests {
		t.Run(fmt.Sprintf("durable value=%s", tt.in), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Base: mdata.Base{Properties: fakeProperties},
			}
			fakeMetaData.Properties[metadataDurableKey] = tt.in

			// act
			m, err := createMetadata(fakeMetaData, log)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, fakeProperties[metadataHostnameKey], m.Hostname)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.ConsumerID)
			assert.Equal(t, tt.expected, m.Durable)
		})
	}

	for _, tt := range booleanFlagTests {
		t.Run(fmt.Sprintf("publisherConfirm value=%s", tt.in), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Base: mdata.Base{Properties: fakeProperties},
			}
			fakeMetaData.Properties[metadataPublisherConfirmKey] = tt.in

			// act
			m, err := createMetadata(fakeMetaData, log)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, fakeProperties[metadataHostnameKey], m.Hostname)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.ConsumerID)
			assert.Equal(t, tt.expected, m.PublisherConfirm)
		})
	}

	for _, tt := range booleanFlagTests {
		t.Run(fmt.Sprintf("enableDeadLetter value=%s", tt.in), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Base: mdata.Base{Properties: fakeProperties},
			}
			fakeMetaData.Properties[metadataEnableDeadLetterKey] = tt.in

			// act
			m, err := createMetadata(fakeMetaData, log)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, fakeProperties[metadataHostnameKey], m.Hostname)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.ConsumerID)
			assert.Equal(t, tt.expected, m.EnableDeadLetter)
		})
	}
	validExchangeKind := []string{amqp.ExchangeDirect, amqp.ExchangeTopic, amqp.ExchangeFanout, amqp.ExchangeHeaders}

	for _, exchangeKind := range validExchangeKind {
		t.Run(fmt.Sprintf("exchangeKind value=%s", exchangeKind), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Base: mdata.Base{Properties: fakeProperties},
			}
			fakeMetaData.Properties[metadataExchangeKindKey] = exchangeKind

			// act
			m, err := createMetadata(fakeMetaData, log)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, fakeProperties[metadataHostnameKey], m.Hostname)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.ConsumerID)
			assert.Equal(t, exchangeKind, m.ExchangeKind)
		})
	}

	t.Run("exchangeKind is invalid", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		fakeMetaData.Properties[metadataExchangeKindKey] = "invalid"

		// act
		_, err := createMetadata(fakeMetaData, log)

		// assert
		assert.Error(t, err)
	})
}

func TestConnectionURI(t *testing.T) {
	log := logger.NewLogger("test")

	testCases := []struct {
		args           map[string]string
		expectedOutput string
	}{
		// connection string
		{
			args:           map[string]string{"connectionString": "amqp://fakeuser:fakepassword@fakehostname-connectionstring"},
			expectedOutput: "amqp://fakeuser:fakepassword@fakehostname-connectionstring",
		},

		// individual arguments
		{
			args:           map[string]string{},
			expectedOutput: "amqp://localhost",
		},
		{
			args:           map[string]string{"hostname": "localhost"},
			expectedOutput: "amqp://localhost",
		},
		{
			args:           map[string]string{"hostname": "fake-hostname", "password": "testpassword"},
			expectedOutput: "amqp://fake-hostname",
		},
		{
			args:           map[string]string{"hostname": "localhost", "username": "testusername"},
			expectedOutput: "amqp://testusername@localhost",
		},
		{
			args:           map[string]string{"hostname": "localhost", "username": "testusername", "password": "testpassword"},
			expectedOutput: "amqp://testusername:testpassword@localhost",
		},
		{
			args:           map[string]string{"protocol": "amqps", "hostname": "localhost", "username": "testusername", "password": "testpassword"},
			expectedOutput: "amqps://testusername:testpassword@localhost",
		},

		// legacy argument
		{
			args:           map[string]string{"host": "amqp://fake-hostname"},
			expectedOutput: "amqp://fake-hostname",
		},
	}

	var metadata pubsub.Metadata

	for _, testCase := range testCases {
		metadata = pubsub.Metadata{
			Base: mdata.Base{Properties: testCase.args},
		}

		m, err := createMetadata(metadata, log)

		assert.NoError(t, err)
		assert.Equal(t, testCase.expectedOutput, m.connectionURI())
	}
}
