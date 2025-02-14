// Package gomqtt provides a flexible MQTT client with configurable options.
package gomqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

// Logger defines the interface for logging within the package.
type Logger interface {
	Info(format string, args ...interface{})
	Error(format string, args ...interface{})
}

// noopLogger is a logger that does nothing.
type noopLogger struct{}

func (n noopLogger) Info(_ string, _ ...interface{})  {}
func (n noopLogger) Error(_ string, _ ...interface{}) {}

// HandlerFunc handles incoming MQTT messages.
type HandlerFunc func(payload []byte) error

// BuildCustomTopic constructs a topic string from a base.
type BuildCustomTopic func(baseTopic string) string

// Client manages the MQTT connection and operations.
type Client struct {
	mqttClient   mqtt.Client
	logger       Logger
	handlers     map[string]HandlerFunc
	topicBuilder BuildCustomTopic
}

// Option configures the MQTT client.
type Option func(*options)

type options struct {
	clientID       string
	username       string
	password       string
	logger         Logger
	topicBuilder   BuildCustomTopic
	cleanSession   bool
	keepAlive      time.Duration
	connectTimeout time.Duration
	lwtTopic       string
	lwtMessage     string
	lwtQos         byte
	lwtRetained    bool
	appendUUID     bool
}

// WithClientID sets the client ID.
func WithClientID(id string) Option {
	return func(o *options) {
		o.clientID = id
	}
}

// WithCredentials sets the username and password.
func WithCredentials(username, password string) Option {
	return func(o *options) {
		o.username = username
		o.password = password
	}
}

// WithLogger sets a custom logger.
func WithLogger(logger Logger) Option {
	return func(o *options) {
		o.logger = logger
	}
}

// WithTopicBuilder sets a custom topic builder.
func WithTopicBuilder(builder BuildCustomTopic) Option {
	return func(o *options) {
		o.topicBuilder = builder
	}
}

// WithWill configures the Last Will and Testament.
func WithWill(topic, message string, qos byte, retained bool) Option {
	return func(o *options) {
		o.lwtTopic = topic
		o.lwtMessage = message
		o.lwtQos = qos
		o.lwtRetained = retained
	}
}

// WithUniqueClientID appends a UUID to the client ID.
func WithUniqueClientID() Option {
	return func(o *options) {
		o.appendUUID = true
	}
}

// NewClient creates a new MQTT client with the given broker and options.
func NewClient(broker string, opts ...Option) (*Client, error) {
	cfg := &options{
		cleanSession:   false,
		keepAlive:      60 * time.Second,
		connectTimeout: 30 * time.Second,
		logger:         noopLogger{},
		topicBuilder:   func(t string) string { return t },
	}

	for _, opt := range opts {
		opt(cfg)
	}

	clientID := cfg.clientID
	if cfg.appendUUID {
		clientID = fmt.Sprintf("%s-%s", clientID, uuid.New().String())
	}

	if cfg.lwtTopic == "" {
		cfg.lwtTopic = fmt.Sprintf("/lwt/%s", clientID)
		cfg.lwtMessage = fmt.Sprintf("%s disconnected", clientID)
	}

	mqttOpts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID(clientID).
		SetUsername(cfg.username).
		SetPassword(cfg.password).
		SetCleanSession(cfg.cleanSession).
		SetKeepAlive(cfg.keepAlive).
		SetConnectTimeout(cfg.connectTimeout).
		SetWill(cfg.lwtTopic, cfg.lwtMessage, cfg.lwtQos, cfg.lwtRetained)

	client := mqtt.NewClient(mqttOpts)
	return &Client{
		mqttClient:   client,
		logger:       cfg.logger,
		handlers:     make(map[string]HandlerFunc),
		topicBuilder: cfg.topicBuilder,
	}, nil
}

// Connect establishes the MQTT connection with exponential backoff.
func (c *Client) Connect(ctx context.Context) error {
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		token := c.mqttClient.Connect()
		if token.Wait() && token.Error() != nil {
			c.logger.Error("Connection failed: %v", token.Error())
		} else {
			c.logger.Info("Connected successfully.")
			return nil
		}

		select {
		case <-time.After(backoff):
			backoff = min(backoff*2, maxBackoff)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Publish sends a message to the MQTT broker.
func (c *Client) Publish(topic string, msg interface{}, qos byte, retained bool) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	fullTopic := c.topicBuilder(topic)
	token := c.mqttClient.Publish(fullTopic, qos, retained, payload)
	if token.WaitTimeout(2*time.Second) && token.Error() != nil {
		return fmt.Errorf("publish failed: %w", token.Error())
	}
	c.logger.Info("Published to %s", fullTopic)
	return nil
}

// Subscribe registers a handler for a topic.
func (c *Client) Subscribe(topic string, handler HandlerFunc) error {
	token := c.mqttClient.Subscribe(topic, 1, func(_ mqtt.Client, msg mqtt.Message) {
		c.logger.Info("Received message on %s", msg.Topic())
		if err := handler(msg.Payload()); err != nil {
			c.logger.Error("Handler error: %v", err)
		}
	})
	if token.WaitTimeout(5*time.Second) && token.Error() != nil {
		return fmt.Errorf("subscribe failed: %w", token.Error())
	}
	c.logger.Info("Subscribed to %s", topic)
	return nil
}

// MonitorConnection checks the connection status and reconnects if necessary.
func (c *Client) MonitorConnection(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !c.mqttClient.IsConnected() {
				c.logger.Info("Reconnecting...")
				if err := c.Connect(ctx); err != nil {
					c.logger.Error("Reconnect failed: %v", err)
				}
			}
		case <-ctx.Done():
			c.logger.Info("Connection monitor stopped.")
			return
		}
	}
}

// Disconnect closes the MQTT connection.
func (c *Client) Disconnect() {
	c.mqttClient.Disconnect(250)
	c.logger.Info("Disconnected.")
}
