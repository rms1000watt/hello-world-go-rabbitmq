package main

import (
	"fmt"
	"log"
	"time"

	"strings"

	"github.com/streadway/amqp"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Println("Failed connecting to RabbitMQ:", err)
		return
	}
	defer conn.Close()

	msgs, workChannel, err := setupWorkQueue(conn)
	if err != nil {
		log.Println("Failed connecting to work channel or queue:", err)
		return
	}
	defer workChannel.Close()

	responseChannel, err := setupResponseExchange(conn)
	if err != nil {
		log.Println("Failed connecting to response exchange:", err)
		return
	}
	defer responseChannel.Close()

	forever := make(chan bool)
	responses := make(chan string)

	go func() {
		for d := range msgs {
			log.Println("Received a message: ", string(d.Body))

			log.Println("Working...")
			time.Sleep(1 * time.Second)
			log.Println("Working...")
			time.Sleep(1 * time.Second)

			log.Println("Done")
			d.Ack(false)

			responses <- string(d.Body) + " world!!"
		}
	}()

	go func() {
		for response := range responses {

			splitStr := strings.Split(response, "::")
			workID := splitStr[0]
			payload := splitStr[1]

			msg := amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(payload),
			}

			err := responseChannel.Publish(
				"response-exchange", // exchange
				workID,              // routing key
				false,               // mandatory
				false,               // immediate
				msg,
			)
			if err != nil {
				log.Println("Failed to publish to exchange:", err)
				continue
			}

			log.Println("sent:", response)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func setupWorkQueue(conn *amqp.Connection) (msgs <-chan amqp.Delivery, workChannel *amqp.Channel, err error) {
	workChannel, err = conn.Channel()
	if err != nil {
		log.Println("Failed to open work channel:", err)
		return
	}

	workQueue, err := workChannel.QueueDeclare(
		"work-queue-1", // name
		true,           // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		log.Println("Failed to declare work queue:", err)
		return
	}

	err = workChannel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		log.Println("Failed to set QoS:", err)
		return
	}

	msgs, err = workChannel.Consume(
		workQueue.Name, // queue
		"",             // consumer
		false,          // auto-ack
		false,          // exclusive
		false,          // no-local
		false,          // no-wait
		nil,            // args
	)
	if err != nil {
		log.Println("Failed to register a consumer:", err)
		return
	}

	return
}

func setupResponseExchange(conn *amqp.Connection) (responseChannel *amqp.Channel, err error) {
	responseChannel, err = conn.Channel()
	if err != nil {
		log.Println("Failed to open a channel")
		return
	}

	err = responseChannel.ExchangeDeclare(
		"response-exchange", // name
		"topic",             // type
		true,                // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
	if err != nil {
		fmt.Println("Failed to declare an exchange")
		return
	}

	return
}
