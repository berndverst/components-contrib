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

/*
Package natsstreaming implements NATS Streaming pubsub component
*/
package natsstreaming

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"time"

	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
	"github.com/nats-io/stan.go/pb"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

// compulsory options.
const (
	natsURL                = "natsURL"
	natsStreamingClusterID = "natsStreamingClusterID"
)

// subscription options (optional).
const (
	durableSubscriptionName = "durableSubscriptionName"
	startAtSequence         = "startAtSequence"
	startWithLastReceived   = "startWithLastReceived"
	deliverAll              = "deliverAll"
	deliverNew              = "deliverNew"
	startAtTimeDelta        = "startAtTimeDelta"
	startAtTime             = "startAtTime"
	startAtTimeFormat       = "startAtTimeFormat"
	ackWaitTime             = "ackWaitTime"
	maxInFlight             = "maxInFlight"
)

// valid values for subscription options.
const (
	subscriptionTypeQueueGroup = "queue"
	subscriptionTypeTopic      = "topic"
	startWithLastReceivedTrue  = "true"
	deliverAllTrue             = "true"
	deliverNewTrue             = "true"
)

const (
	consumerID       = "consumerID" // passed in by Dapr runtime
	subscriptionType = "subscriptionType"
)

type natsStreamingPubSub struct {
	metadata         natsMetadata
	natStreamingConn stan.Conn

	logger logger.Logger

	backOffConfig retry.Config
}

// NewNATSStreamingPubSub returns a new NATS Streaming pub-sub implementation.
func NewNATSStreamingPubSub(logger logger.Logger) pubsub.PubSub {
	return &natsStreamingPubSub{logger: logger}
}

func parseNATSStreamingMetadata(meta pubsub.Metadata) (natsMetadata, error) {
	m := natsMetadata{}
	metadata.DecodeMetadata(meta.Properties, &m)

	if m.NatsURL == "" {
		return m, errors.New("nats-streaming error: missing nats URL")
	}
	if m.NatsStreamingClusterID == "" {
		return m, errors.New("nats-streaming error: missing nats streaming cluster ID")
	}

	if m.SubscriptionType != subscriptionTypeTopic && m.SubscriptionType != subscriptionTypeQueueGroup {
		return m, errors.New("nats-streaming error: invalid value for subscriptionType is topic or queue")
	}

	if m.NatsQueueGroupName == "" && m.ConsumerID != "" {
		m.NatsQueueGroupName = m.ConsumerID
	} else {
		if m.NatsQueueGroupName == "" {
			return m, errors.New("nats-streaming error: missing queue group name")
		}
	}

	if m.MaxInFlight != 0 && m.MaxInFlight < 1 {
		return m, errors.New("nats-streaming error: maxInFlight should be equal to or more than 1")
	}

	//nolint:nestif
	// subscription options - only one can be used
	if val, ok := meta.Properties[startAtSequence]; ok && val != "" {
		// nats streaming accepts a uint64 as sequence
		seq, err := strconv.ParseUint(meta.Properties[startAtSequence], 10, 64)
		if err != nil {
			return m, fmt.Errorf("nats-streaming error %s ", err)
		}
		if seq < 1 {
			return m, errors.New("nats-streaming error: startAtSequence should be equal to or more than 1")
		}
		m.StartAtSequence = seq
	} else if val, ok := meta.Properties[startWithLastReceived]; ok {
		// only valid value is true
		if val == startWithLastReceivedTrue {
			m.StartWithLastReceived = val
		} else {
			return m, errors.New("nats-streaming error: valid value for startWithLastReceived is true")
		}
	} else if val, ok := meta.Properties[deliverAll]; ok {
		// only valid value is true
		if val == deliverAllTrue {
			m.DeliverAll = val
		} else {
			return m, errors.New("nats-streaming error: valid value for deliverAll is true")
		}
	} else if val, ok := meta.Properties[deliverNew]; ok {
		// only valid value is true
		if val == deliverNewTrue {
			m.DeliverNew = val
		} else {
			return m, errors.New("nats-streaming error: valid value for deliverNew is true")
		}
	} else if val, ok := meta.Properties[startAtTimeDelta]; ok && val != "" {
		dur, err := time.ParseDuration(meta.Properties[startAtTimeDelta])
		if err != nil {
			return m, fmt.Errorf("nats-streaming error %s ", err)
		}
		m.StartAtTimeDelta = dur
	} else if val, ok := meta.Properties[startAtTime]; ok && val != "" {
		m.StartAtTime = val
		if val, ok := meta.Properties[startAtTimeFormat]; ok && val != "" {
			m.StartAtTimeFormat = val
		} else {
			return m, errors.New("nats-streaming error: missing value for startAtTimeFormat")
		}
	}

	c, err := pubsub.Concurrency(meta.Properties)
	if err != nil {
		return m, fmt.Errorf("nats-streaming error: can't parse %s: %s", pubsub.ConcurrencyKey, err)
	}

	m.ConcurrencyMode = c
	return m, nil
}

func (n *natsStreamingPubSub) Init(metadata pubsub.Metadata) error {
	m, err := parseNATSStreamingMetadata(metadata)
	if err != nil {
		return err
	}
	n.metadata = m
	clientID := genRandomString(20)
	opts := []nats.Option{nats.Name(clientID)}
	natsConn, err := nats.Connect(m.NatsURL, opts...)
	if err != nil {
		return fmt.Errorf("nats-streaming: error connecting to nats server at %s: %s", m.NatsURL, err)
	}
	natStreamingConn, err := stan.Connect(m.NatsStreamingClusterID, clientID, stan.NatsConn(natsConn))
	if err != nil {
		return fmt.Errorf("nats-streaming: error connecting to nats streaming server %s: %s", m.NatsStreamingClusterID, err)
	}
	n.logger.Debugf("connected to natsstreaming at %s", m.NatsURL)

	// Default retry configuration is used if no
	// backOff properties are set.
	if err := retry.DecodeConfigWithPrefix(
		&n.backOffConfig,
		metadata.Properties,
		"backOff"); err != nil {
		return err
	}

	n.natStreamingConn = natStreamingConn

	return nil
}

func (n *natsStreamingPubSub) Publish(req *pubsub.PublishRequest) error {
	err := n.natStreamingConn.Publish(req.Topic, req.Data)
	if err != nil {
		return fmt.Errorf("nats-streaming: error from publish: %s", err)
	}

	return nil
}

func (n *natsStreamingPubSub) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	natStreamingsubscriptionOptions, err := n.subscriptionOptions()
	if err != nil {
		return fmt.Errorf("nats-streaming: error getting subscription options %s", err)
	}

	natsMsgHandler := func(natsMsg *stan.Msg) {
		msg := pubsub.NewMessage{
			Topic: req.Topic,
			Data:  natsMsg.Data,
		}

		n.logger.Debugf("Processing NATS Streaming message %s/%d", natsMsg.Subject, natsMsg.Sequence)

		f := func() {
			herr := handler(ctx, &msg)
			if herr == nil {
				natsMsg.Ack()
			}
		}

		switch n.metadata.ConcurrencyMode {
		case pubsub.Single:
			f()
		case pubsub.Parallel:
			go f()
		}
	}

	var subscription stan.Subscription
	if n.metadata.SubscriptionType == subscriptionTypeTopic {
		subscription, err = n.natStreamingConn.Subscribe(req.Topic, natsMsgHandler, natStreamingsubscriptionOptions...)
	} else if n.metadata.SubscriptionType == subscriptionTypeQueueGroup {
		subscription, err = n.natStreamingConn.QueueSubscribe(req.Topic, n.metadata.NatsQueueGroupName, natsMsgHandler, natStreamingsubscriptionOptions...)
	}

	if err != nil {
		return fmt.Errorf("nats-streaming: subscribe error %s", err)
	}

	go func() {
		<-ctx.Done()
		err := subscription.Unsubscribe()
		if err != nil {
			n.logger.Warnf("nats-streaming: error while unsubscribing from topic %s: %v", req.Topic, err)
		}
	}()

	if n.metadata.SubscriptionType == subscriptionTypeTopic {
		n.logger.Debugf("nats-streaming: subscribed to subject %s", req.Topic)
	} else if n.metadata.SubscriptionType == subscriptionTypeQueueGroup {
		n.logger.Debugf("nats-streaming: subscribed to subject %s with queue group %s", req.Topic, n.metadata.NatsQueueGroupName)
	}

	return nil
}

func (n *natsStreamingPubSub) subscriptionOptions() ([]stan.SubscriptionOption, error) {
	var options []stan.SubscriptionOption

	if n.metadata.DurableSubscriptionName != "" {
		options = append(options, stan.DurableName(n.metadata.DurableSubscriptionName))
	}

	switch {
	case n.metadata.DeliverNew == deliverNewTrue:
		options = append(options, stan.StartAt(pb.StartPosition_NewOnly)) //nolint:nosnakecase
	case n.metadata.StartAtSequence >= 1: // messages index start from 1, this is a valid check
		options = append(options, stan.StartAtSequence(n.metadata.StartAtSequence))
	case n.metadata.StartWithLastReceived == startWithLastReceivedTrue:
		options = append(options, stan.StartWithLastReceived())
	case n.metadata.DeliverAll == deliverAllTrue:
		options = append(options, stan.DeliverAllAvailable())
	case n.metadata.StartAtTimeDelta > (1 * time.Nanosecond): // as long as its a valid time.Duration
		options = append(options, stan.StartAtTimeDelta(n.metadata.StartAtTimeDelta))
	case n.metadata.StartAtTime != "":
		if n.metadata.StartAtTimeFormat != "" {
			startTime, err := time.Parse(n.metadata.StartAtTimeFormat, n.metadata.StartAtTime)
			if err != nil {
				return nil, err
			}
			options = append(options, stan.StartAtTime(startTime))
		}
	}

	// default is auto ACK. switching to manual ACK since processing errors need to be handled
	options = append(options, stan.SetManualAckMode())

	// check if set the ack options.
	if n.metadata.AckWaitTime > (1 * time.Nanosecond) {
		options = append(options, stan.AckWait(n.metadata.AckWaitTime))
	}
	if n.metadata.MaxInFlight >= 1 {
		options = append(options, stan.MaxInflight(int(n.metadata.MaxInFlight)))
	}

	return options, nil
}

const inputs = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"

// generates a random string of length 20.
func genRandomString(n int) string {
	b := make([]byte, n)
	s := rand.NewSource(int64(time.Now().Nanosecond()))
	for i := range b {
		b[i] = inputs[s.Int63()%int64(len(inputs))]
	}
	clientID := string(b)

	return clientID
}

func (n *natsStreamingPubSub) Close() error {
	return n.natStreamingConn.Close()
}

func (n *natsStreamingPubSub) Features() []pubsub.Feature {
	return nil
}

func (n *natsStreamingPubSub) GetComponentMetadata() map[string]string {
	metadataStruct := natsMetadata{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo)
	return metadataInfo
}
