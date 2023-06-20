package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

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
const TopicPrefix = "cdc/*/postgresql"

func getEnv(key, def string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return def
}

type Blogs struct {
	BlogID      int       `json:"blog_id"`
	Title       string    `json:"title"`
	Content     string    `json:"content"`
	Author      string    `json:"author"`
	CreatedOn   time.Time `json:"created_on"`
	LastUpdated time.Time `json:"last_updated"`
}

func MessageHandlerEuro(message message.InboundMessage) {

	payload, ok := message.GetPayloadAsBytes()
	if ok != true {
		fmt.Println("Some issue happens")
	}

	var result map[string]interface{}
	fmt.Println(string(payload))

	err := json.Unmarshal(payload, &result)
	if err != nil {
		// print out if error is not nil
		fmt.Println(err)
	}

	changeKind := (result["change"]).([]interface{})[0].(map[string]interface{})["kind"].(string)

	if changeKind != "delete" {

		data := (result["change"]).([]interface{})[0].(map[string]interface{})["columnvalues"]
		fmt.Println(data)

		blogId := data.([]interface{})[0]
		blogTitle := data.([]interface{})[1]
		blogContent := data.([]interface{})[2]
		blogAuthor := data.([]interface{})[3]
		blogCreatedOn := data.([]interface{})[4]
		blogLastUpdated := data.([]interface{})[5]
		fmt.Println(blogId)

		blog := Blogs{}
		blog.BlogID = int(blogId.(float64))
		blog.Title = blogTitle.(string)
		blog.Content = blogContent.(string)
		blog.Author = blogAuthor.(string)
		createdOn, _ := blogCreatedOn.(string)
		lastUpdated, _ := blogLastUpdated.(string)
		createdOnTime, err := time.Parse("2006-01-02 15:04:05.999999999", createdOn)
		if err != nil {
			fmt.Println(err)
			return
		}
		blog.CreatedOn = createdOnTime
		lastUpdatedTime, err := time.Parse("2006-01-02 15:04:05.999999999", lastUpdated)
		if err != nil {
			fmt.Println(err)
			return
		}
		blog.LastUpdated = lastUpdatedTime

		blogJson, err := json.Marshal(blog)
		if err != nil {
			fmt.Println(err)
			return
		}

		updateCacheRedis(getEnv("RedisHost", "tcps://"), getEnv("RedisPassword", "password"), changeKind, strconv.Itoa(int(blogId.(float64))), blogJson)

	} else if changeKind == "delete" {

		blogId := result["change"].([]interface{})[0].(map[string]interface{})["oldkeys"].(map[string]interface{})["keyvalues"].([]interface{})[0]

		deleteCacheRedis(getEnv("RedisHost", "tcps://"), getEnv("RedisPassword", "password"), strconv.Itoa(int(blogId.(float64))))
	}

}

type Author struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func updateCacheRedis(host string, password string, changeKind string, key string, json []byte) {

	client := redis.NewClient(&redis.Options{
		Addr:     host,
		Password: password,
		DB:       0,
	})

	defer client.Close()

	if changeKind == "delete" {
		err := client.Del("blogId-" + key).Err()
		if err != nil {
			fmt.Println(err)
		}
	} else if changeKind == "update" {
		err := client.Del("blogId-" + key).Err()
		if err != nil {
			fmt.Println(err)
		}
		err = client.Set("blogId-"+key, json, 0).Err()
		if err != nil {
			fmt.Println(err)
		}
	} else if changeKind == "insert" {

		err := client.Set("blogId-"+key, json, 0).Err()
		if err != nil {
			fmt.Println(err)
		}
	}

}

func deleteCacheRedis(host string, password string, key string) {

	client := redis.NewClient(&redis.Options{
		Addr:     host,
		Password: password,
		DB:       0,
	})

	defer client.Close()

	err := client.Del("blogId-" + key).Err()
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

	}

}
