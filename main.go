package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/go-redis/redis"
	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

func assertEqual(a interface{}, b interface{}, message string) {
	if a == b {
		return
	}
	if len(message) == 0 {
		message = fmt.Sprintf("%v != %v", a, b)
	}
	log.Fatal("not equal when checking status code")
}

// Define Topic Prefix
const TopicPrefix = "cdc/*/sqlserver"

func getEnv(key, def string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return def
}

func MessageHandlerEuro(message message.InboundMessage) {
	var messageBody string

	if payload, ok := message.GetPayloadAsString(); ok {
		messageBody = payload
	} else if payload, ok := message.GetPayloadAsBytes(); ok {
		messageBody = string(payload)
	}

	fmt.Println(messageBody)

}

type Author struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func addCacheRedis(host string, password string) {

	client := redis.NewClient(&redis.Options{
		Addr:     host,
		Password: password,
		DB:       0,
	})

	defer client.Close()

	json, err := json.Marshal(Author{Name: "Elliot", Age: 25})
	if err != nil {
		fmt.Println(err)
	}

	err = client.Set("author1", json, 0).Err()
	if err != nil {
		fmt.Println(err)
	}

}

func main() {

	// Configuration parameters
	brokerConfig := config.ServicePropertyMap{
		config.TransportLayerPropertyHost:                getEnv("TransportLayerPropertyHost", "tcps://"),
		config.ServicePropertyVPNName:                    getEnv("ServicePropertyVPNName", "brokerName"),
		config.AuthenticationPropertySchemeBasicUserName: getEnv("AuthenticationPropertySchemeBasicUserName", "solace-cloud-client"),
		config.AuthenticationPropertySchemeBasicPassword: getEnv("AuthenticationPropertySchemeBasicPassword", "password"),
	}
	messagingService, err := messaging.NewMessagingServiceBuilder().FromConfigurationProvider(brokerConfig).WithTransportSecurityStrategy(config.NewTransportSecurityStrategy().WithoutCertificateValidation()).
		Build()

	if err != nil {
		panic(err)
	}

	// Connect to the messaging serice
	if err := messagingService.Connect(); err != nil {
		panic(err)
	}

	fmt.Println("Connected to the broker? ", messagingService.IsConnected())

	//  Build a Direct Message Receiver
	directReceiver, err := messagingService.CreateDirectMessageReceiverBuilder().
		WithSubscriptions(resource.TopicSubscriptionOf(TopicPrefix)).
		Build()

	if err != nil {
		panic(err)
	}

	// Start Direct Message Receiver
	if err := directReceiver.Start(); err != nil {
		panic(err)
	}

	fmt.Println("Direct Receiver running? ", directReceiver.IsRunning())

	for 1 != 0 {

		if regErr := directReceiver.ReceiveAsync(MessageHandlerEuro); regErr != nil {
			panic(regErr)
		}

		addCacheRedis(getEnv("RedisHost", "tcps://"), getEnv("RedisPassword", "password"))

	}

}
