# Go MQTT Client

A flexible MQTT client wrapper for Go with support for:

- Automatic reconnection with exponential backoff
- JSON message publishing
- Custom topic building
- Configurable logging
- Last Will and Testament (LWT)
- Connection monitoring

## Installation

```sh
go get github.com/flyzard/go-mqtt
```

## Quick Start

```
client, err := gomqtt.NewClient("tcp://localhost:1883",
    gomqtt.WithClientID("my-client"),
    gomqtt.WithUniqueClientID(),
)
if err != nil {
    log.Fatal(err)
}

ctx := context.Background()
if err := client.Connect(ctx); err != nil {
    log.Fatal(err)
}
defer client.Disconnect()

// Publish a message
err = client.Publish("test/topic", "Hello MQTT!", 1, false)

// Subscribe to topics
err = client.Subscribe("test/topic", func(payload []byte) error {
    fmt.Printf("Received: %s\n", payload)
    return nil
})
```

## License
MIT