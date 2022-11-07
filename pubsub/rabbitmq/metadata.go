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
	"net/url"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type metadata struct {
	ConsumerID       string
	ConnectionString string
	Protocol         string
	Hostname         string
	Username         string
	Password         string
	Durable          bool
	EnableDeadLetter bool
	DeleteWhenUnused bool
	AutoAck          bool
	RequeueInFailure bool
	DeliveryMode     uint8 // Transient (0 or 1) or Persistent (2)
	PrefetchCount    uint8 // Prefetch deactivated if 0
	ReconnectWait    time.Duration
	MaxLen           int64
	MaxLenBytes      int64
	ExchangeKind     string
	PublisherConfirm bool
	Concurrency      pubsub.ConcurrencyMode
}

const (
	metadataConsumerIDKey = "consumerID"

	metadataConnectionStringKey = "connectionString"
	metadataHostKey             = "host"

	metadataProtocolKey = "protocol"
	metadataHostnameKey = "hostname"
	metadataUsernameKey = "username"
	metadataPasswordKey = "password"

	metadataDurableKey              = "durable"
	metadataEnableDeadLetterKey     = "enableDeadLetter"
	metadataDeleteWhenUnusedKey     = "deletedWhenUnused"
	metadataAutoAckKey              = "autoAck"
	metadataRequeueInFailureKey     = "requeueInFailure"
	metadataDeliveryModeKey         = "deliveryMode"
	metadataPrefetchCountKey        = "prefetchCount"
	metadataReconnectWaitSecondsKey = "reconnectWaitSeconds"
	metadataMaxLenKey               = "maxLen"
	metadataMaxLenBytesKey          = "maxLenBytes"
	metadataExchangeKindKey         = "exchangeKind"
	metadataPublisherConfirmKey     = "publisherConfirm"

	defaultReconnectWaitSeconds = 3
)

// createMetadata creates a new instance from the pubsub metadata.
func createMetadata(pubSubMetadata pubsub.Metadata, log logger.Logger) (*metadata, error) {
	result := metadata{
		Protocol:         "amqp",
		Hostname:         "localhost",
		Durable:          true,
		DeleteWhenUnused: true,
		AutoAck:          false,
		ReconnectWait:    time.Duration(defaultReconnectWaitSeconds) * time.Second,
		ExchangeKind:     fanoutExchangeKind,
		PublisherConfirm: false,
	}

	if val, found := pubSubMetadata.Properties[metadataConnectionStringKey]; found && val != "" {
		result.ConnectionString = val
	} else if val, found := pubSubMetadata.Properties[metadataHostKey]; found && val != "" {
		result.ConnectionString = val
		log.Warn("[DEPRECATION NOTICE] The 'host' argument is deprecated. Use 'connectionString' or individual connection arguments instead: https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-rabbitmq/")
	}

	if val, found := pubSubMetadata.Properties[metadataProtocolKey]; found && val != "" {
		result.Protocol = val
	}

	if val, found := pubSubMetadata.Properties[metadataHostnameKey]; found && val != "" {
		result.Hostname = val
	}

	if val, found := pubSubMetadata.Properties[metadataUsernameKey]; found && val != "" {
		result.Username = val
	}

	if val, found := pubSubMetadata.Properties[metadataPasswordKey]; found && val != "" {
		result.Password = val
	}

	if val, found := pubSubMetadata.Properties[metadataConsumerIDKey]; found && val != "" {
		result.ConsumerID = val
	}

	if val, found := pubSubMetadata.Properties[metadataDeliveryModeKey]; found && val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			if intVal < 0 || intVal > 2 {
				return &result, fmt.Errorf("%s invalid RabbitMQ delivery mode, accepted values are between 0 and 2", errorMessagePrefix)
			}
			result.DeliveryMode = uint8(intVal)
		}
	}

	if val, found := pubSubMetadata.Properties[metadataDurableKey]; found && val != "" {
		if boolVal, err := strconv.ParseBool(val); err == nil {
			result.Durable = boolVal
		}
	}

	if val, found := pubSubMetadata.Properties[metadataEnableDeadLetterKey]; found && val != "" {
		if boolVal, err := strconv.ParseBool(val); err == nil {
			result.EnableDeadLetter = boolVal
		}
	}

	if val, found := pubSubMetadata.Properties[metadataDeleteWhenUnusedKey]; found && val != "" {
		if boolVal, err := strconv.ParseBool(val); err == nil {
			result.DeleteWhenUnused = boolVal
		}
	}

	if val, found := pubSubMetadata.Properties[metadataAutoAckKey]; found && val != "" {
		if boolVal, err := strconv.ParseBool(val); err == nil {
			result.AutoAck = boolVal
		}
	}

	if val, found := pubSubMetadata.Properties[metadataRequeueInFailureKey]; found && val != "" {
		if boolVal, err := strconv.ParseBool(val); err == nil {
			result.RequeueInFailure = boolVal
		}
	}

	if val, found := pubSubMetadata.Properties[metadataReconnectWaitSecondsKey]; found && val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			result.ReconnectWait = time.Duration(intVal) * time.Second
		}
	}

	if val, found := pubSubMetadata.Properties[metadataPrefetchCountKey]; found && val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			result.PrefetchCount = uint8(intVal)
		}
	}

	if val, found := pubSubMetadata.Properties[metadataMaxLenKey]; found && val != "" {
		if intVal, err := strconv.ParseInt(val, 10, 64); err == nil {
			result.MaxLen = intVal
		}
	}

	if val, found := pubSubMetadata.Properties[metadataMaxLenBytesKey]; found && val != "" {
		if intVal, err := strconv.ParseInt(val, 10, 64); err == nil {
			result.MaxLenBytes = intVal
		}
	}

	if val, found := pubSubMetadata.Properties[metadataExchangeKindKey]; found && val != "" {
		if exchangeKindValid(val) {
			result.ExchangeKind = val
		} else {
			return &result, fmt.Errorf("%s invalid RabbitMQ exchange kind %s", errorMessagePrefix, val)
		}
	}

	if val, found := pubSubMetadata.Properties[metadataPublisherConfirmKey]; found && val != "" {
		if boolVal, err := strconv.ParseBool(val); err == nil {
			result.PublisherConfirm = boolVal
		}
	}

	c, err := pubsub.Concurrency(pubSubMetadata.Properties)
	if err != nil {
		return &result, err
	}
	result.Concurrency = c

	return &result, nil
}

func (m *metadata) formatQueueDeclareArgs(origin amqp.Table) amqp.Table {
	if origin == nil {
		origin = amqp.Table{}
	}
	if m.MaxLen > 0 {
		origin[argMaxLength] = m.MaxLen
	}
	if m.MaxLenBytes > 0 {
		origin[argMaxLengthBytes] = m.MaxLenBytes
	}

	return origin
}

func exchangeKindValid(kind string) bool {
	return kind == amqp.ExchangeFanout || kind == amqp.ExchangeTopic || kind == amqp.ExchangeDirect || kind == amqp.ExchangeHeaders
}

func (m *metadata) connectionURI() string {
	if m.ConnectionString != "" {
		return m.ConnectionString
	}

	u := url.URL{
		Scheme: m.Protocol,
		Host:   m.Hostname,
	}

	if m.Username != "" && m.Password != "" {
		u.User = url.UserPassword(m.Username, m.Password)
	} else if m.Username != "" {
		u.User = url.User(m.Username)
	}

	return u.String()
}
