package pubsub

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/semanggilab/webcore-go/app/config"
	"github.com/semanggilab/webcore-go/app/helper"
	"github.com/semanggilab/webcore-go/app/loader"
	"github.com/semanggilab/webcore-go/app/logger"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// PubSubMessage represents a PubSub message
type PubSubMessage struct {
	ID          string
	Data        []byte
	PublishTime time.Time
	Attributes  map[string]string
}

func (p *PubSubMessage) GetID() string {
	return p.ID
}

func (p *PubSubMessage) GetData() []byte {
	return p.Data
}

func (p *PubSubMessage) GetPublishTime() time.Time {
	return p.PublishTime
}

func (p *PubSubMessage) GetAttributes() map[string]string {
	return p.Attributes
}

// PubSub represents shared Google PubSub connection
type PubSub struct {
	Client    *pubsub.Client
	Config    config.PubSubConfig
	Receivers []loader.PubSubReceiver
}

// NewPubSub creates a new PubSub connection
func NewPubSub(ctx context.Context, config config.PubSubConfig) (*PubSub, error) {
	var client *pubsub.Client
	var err error

	if config.ProjectID == "" {
		return nil, fmt.Errorf("PubSub config project_id cannot be empty")
	}

	// Configure PubSub client options
	opts := []option.ClientOption{}

	// if config.EmulatorHost != "" {
	// 	opts = append(opts, option.WithEndpoint(config.EmulatorHost), option.WithoutAuthentication())
	// }

	if config.CredentialsPath != "" {
		opts = append(opts, option.WithCredentialsFile(config.CredentialsPath))
	}

	client, err = pubsub.NewClient(ctx, config.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create PubSub client: %v", err)
	}

	return &PubSub{
		Client:    client,
		Config:    config,
		Receivers: []loader.PubSubReceiver{},
	}, nil
}

func (ps *PubSub) Install(args ...any) error {
	// Tidak melakukan apa-apa
	return nil
}

func (ps *PubSub) Connect() error {
	// Tidak melakukan apa-apa proses konek hanya dilakukan saat di mode consumer pull message atau publish message di mode producer
	return nil
}

// Close closes the PubSub connection
func (ps *PubSub) Disconnect() error {
	if ps.Client != nil {
		return ps.Client.Close()
	}
	return nil
}

func (ps *PubSub) Uninstall() error {
	// Tidak melakukan apa-apa
	return nil
}

func (ps *PubSub) Publish(ctx context.Context, message any, attributes map[string]string) (string, error) {
	var str string
	var ok bool
	var err error

	str, ok = message.(string)
	if !ok {
		str, err = helper.ToJSON(message)
		if err != nil {
			return "", err
		}
	}

	return ps.PublishMessage(ctx, []byte(str), attributes)
}

func (ps *PubSub) RegisterReceiver(receiver loader.PubSubReceiver) {
	ps.Receivers = append(ps.Receivers, receiver)
}

func (ps *PubSub) StartReceiving(ctx context.Context) {
	if len(ps.Receivers) == 0 {
		logger.Error("PubSub has no Receiver to process incomming message")
		return
	}

	go func() {
		sub := ps.Client.Subscriber(ps.Config.Subscription)
		err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			msg.Ack()
			m := &PubSubMessage{
				ID:          msg.ID,
				Data:        msg.Data,
				PublishTime: msg.PublishTime,
				Attributes:  msg.Attributes,
			}
			for _, c := range ps.Receivers {
				go c.Consume(ctx, []loader.IPubSubMessage{m})
			}
		})

		if err != nil {
			logger.Error("Error receiving messages", "error", err)
			return
		}
	}()
}

// EnsureTopicExists checks if the topic exists
func (ps *PubSub) EnsureTopicExists(ctx context.Context) bool {
	return ps.GetTopicInfo(ctx) != nil
}

// EnsureSubscriptionExists checks if the subscription exists
func (ps *PubSub) EnsureSubscriptionExists(ctx context.Context) bool {
	return ps.GetSubscriptionInfo(ctx) != nil
}

// GetTopicInfo returns information about the topic
func (ps *PubSub) GetTopicInfo(ctx context.Context) *pubsubpb.Topic {
	req := &pubsubpb.ListTopicsRequest{
		Project: fmt.Sprintf("projects/%s", ps.Config.ProjectID),
	}
	topicName := ps.Config.Topic
	// logger.Debug("Cek Exists", "topic", topicName)
	it := ps.Client.TopicAdminClient.ListTopics(ctx, req)
	for {
		topic, err := it.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			continue
		}

		// logger.Debug(" - Topic", "name", topic.Name)
		if topic.Name == topicName {
			return topic
		}
	}

	return nil
}

// GetSubscriptionInfo returns information about the subscription
func (ps *PubSub) GetSubscriptionInfo(ctx context.Context) *pubsubpb.Subscription {
	exists := ps.ListSubscriptions(ctx)

	subName := ps.Config.Subscription
	// logger.Debug("Cek Exists", "subscription", subName)
	for _, sub := range exists {
		if sub != nil {
			// logger.Debug(" - Subscription", "name", sub.Name)
			if sub.Name == subName {
				return sub
			}
		}
	}

	return nil
}

// ListSubscriptions lists all subscriptions for this topic
func (ps *PubSub) ListSubscriptions(ctx context.Context) []*pubsubpb.Subscription {
	var subs []*pubsubpb.Subscription
	req := &pubsubpb.ListSubscriptionsRequest{
		Project: fmt.Sprintf("projects/%s", ps.Config.ProjectID),
	}
	it := ps.Client.SubscriptionAdminClient.ListSubscriptions(ctx, req)
	for {
		s, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			continue
		}
		subs = append(subs, s)
	}
	return subs
}

// PublishMessage publishes a message to the topic
func (ps *PubSub) PublishMessage(ctx context.Context, data []byte, attributes map[string]string) (string, error) {
	publisher := ps.Client.Publisher(ps.Config.Topic)
	result := publisher.Publish(ctx, &pubsub.Message{
		Data:       data,
		Attributes: attributes,
	})

	// Block until the result is returned and a server-generated
	// ID is returned for the published message.
	msgID, err := result.Get(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to publish message: %v", err)
	}

	logger.Debug("PubSub Publish: message", "msgID", msgID)
	return msgID, nil
}

// PublishMessages publishes multiple messages to the topic
func (ps *PubSub) PublishMessages(ctx context.Context, messages [][]byte, attributes map[string]string) ([]string, error) {
	results := []string{}

	var err error
	var msgID string
	i := 0
	for _, msg := range messages {
		msgID, err = ps.PublishMessage(ctx, msg, attributes)
		if msgID != "" {
			results[i] = msgID
			i++
		}
	}

	return results, err
}

// PullMessages pulls messages from the subscription
func (ps *PubSub) PullMessages(ctx context.Context, maxMessages int) ([]*pubsub.Message, error) {
	subscriber := ps.Client.Subscriber(ps.Config.Subscription)
	subscriber.ReceiveSettings.MaxOutstandingMessages = maxMessages

	messages := make([]*pubsub.Message, 0)

	err := subscriber.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		messages = append(messages, msg)
		msg.Ack() // Acknowledge the message
	})

	if err != nil {
		return nil, fmt.Errorf("failed to pull messages: %v", err)
	}

	return messages, nil
}
