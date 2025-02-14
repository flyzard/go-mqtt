// Package mqtt Implemets the MQTT communication for the vault.
package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/flyzard/go-logging"
	"github.com/google/uuid"
)

// HandlerFunc is a function that handles MQTT messages.
type HandlerFunc func(payload []byte) error

// BuildCustomTopic is a function that builds a custom topic.
type BuildCustomTopic func(subTopic string) string

// Client wraps the Paho MQTT Client for specialized MQTT operations.
type Client struct {
	client       mqtt.Client
	logger       *logging.Logger
	handlers     map[string]HandlerFunc
	topicBuilder BuildCustomTopic
}

func defaultTopicBuilder(topic string) string {
	return topic
}

// NewMQTTClient initializes a new MQTTClient using the Paho MQTT client.
func NewMQTTClient(logger *logging.Logger, broker, clientID, username, password string, buildPublisTopicFunc BuildCustomTopic) (*Client, error) {
	opts := configureMQTTOptions(broker, clientID, username, password)
	client := mqtt.NewClient(opts)

	// Use default topic builder if none provided
	if buildPublisTopicFunc == nil {
		buildPublisTopicFunc = defaultTopicBuilder
	}

	return &Client{
		client:       client,
		logger:       logger,
		handlers:     make(map[string]HandlerFunc),
		topicBuilder: buildPublisTopicFunc,
	}, nil
}

// configureMQTTOptions configures the MQTT client options.
func configureMQTTOptions(broker, clientID, username, password string) *mqtt.ClientOptions {

	// generate unique string
	uuidStr := uuid.New().String()
	clientID = fmt.Sprintf("%s-%s", clientID, uuidStr)

	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID(clientID).
		SetUsername(username).
		SetPassword(password).
		SetCleanSession(false).
		SetKeepAlive(60 * time.Second).
		SetConnectTimeout(30 * time.Second)

	lwtTopic := fmt.Sprintf("/lwt/topic/%s", clientID)
	lwtMessage := fmt.Sprintf("%s disconnected unexpectedly", clientID)
	lwtQos := byte(1)
	lwtRetained := false

	opts.SetWill(lwtTopic, lwtMessage, lwtQos, lwtRetained)

	return opts
}

// Connect establishes a connection to the MQTT broker.
func (mc *Client) Connect(ctx context.Context) error {
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		token := mc.client.Connect()
		if token.Wait() && token.Error() != nil {
			mc.logger.Error("MQTT connection failed: %v", token.Error())
		} else {
			mc.logger.Info("MQTT connected successfully.")
			return nil
		}

		mc.logger.Info("Retrying MQTT connection in %v...", backoff)
		select {
		case <-time.After(backoff):
			// Increase backoff, capping at maxBackoff.
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		case <-ctx.Done():
			mc.logger.Info("MQTT connection retry canceled due to context cancellation.")
			return ctx.Err()
		}
	}
}

// Publish publishes a message to the MQTT broker.
func (mc *Client) Publish(topic string, msg any, qos byte, retained bool) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	fullTopic := mc.topicBuilder(topic)

	token := mc.client.Publish(fullTopic, qos, retained, payload)
	if token.WaitTimeout(2*time.Second) && token.Error() != nil {
		return fmt.Errorf("failed to publish message on topic %s: %w", fullTopic, token.Error())
	}

	mc.logger.Info("Message on topic %s published", fullTopic)
	return nil
}

// buildTopic builds the full topic for the given sub-topic.
func (mc *Client) buildTopic(subTopic string, vaultID int) string {
	return fmt.Sprintf("vault/%d/%s", vaultID, subTopic)
}

// Subscribe subscribes to a topic with a given message handler.
func (mc *Client) Subscribe(topic string, callback HandlerFunc) error {
	token := mc.client.Subscribe(topic, 1, func(_ mqtt.Client, msg mqtt.Message) {
		mc.logger.Info("Message received on topic %s", msg.Topic())
		if err := callback(msg.Payload()); err != nil {
			mc.logger.Error("Failed to handle mqtt message: %v", err)
		}
	})
	if token.WaitTimeout(5*time.Second) && token.Error() != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %w", topic, token.Error())
	}
	mc.logger.Info("Subscribed to topic %s", topic)
	return nil
}

// RegisterHandler registers a handler for a specific topic.
func (mc *Client) RegisterHandler(topic string, handler HandlerFunc) {
	mc.handlers[topic] = handler
}

// StartSubscribing subscribes the MQTTClient to the topics and starts the handlers.
func (mc *Client) StartSubscribing() error {
	for topic, handlerFunc := range mc.handlers {
		if err := mc.Subscribe(topic, handlerFunc); err != nil {
			return fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
		}
	}

	return nil
}

// Disconnect stops the MQTTClient.
func (mc *Client) Disconnect() {
	mc.logger.Info("Disconnecting MQTTClient")
	mc.client.Disconnect(250) // Argument is the disconnection quiesce time in milliseconds
}

// MonitorMQTTConnection monitors the MQTT connection and attempts reconnection if lost.
func (mc *Client) MonitorMQTTConnection(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// If the connection is lost, attempt reconnection.
			if !mc.client.IsConnected() {
				mc.logger.Info("MQTT connection lost. Attempting to reconnect...")
				if err := mc.Connect(ctx); err != nil {
					mc.logger.Error("MQTT reconnection failed: %v", err)
				} else {
					mc.logger.Info("MQTT reconnected successfully.")
				}
			}
			return
		case <-ctx.Done():
			mc.logger.Info("MQTT connection monitor shutting down.")
			return
		}
	}
}
