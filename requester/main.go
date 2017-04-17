package main

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Println("Failed connecting to RabbitMQ:", err)
		return
	}
	defer conn.Close()

	// Connect to worker queue. Push work down here
	workChannel, workQueue, err := setupWorkQueue(conn)
	if err != nil {
		log.Println("Failed setting up work channel or work queue:", err)
		return
	}
	defer workChannel.Close()

	workID := "81929123"

	// Connect to response exchange. Wait for response on topic here
	response, responseChannel, err := setupResponseExchange(conn, workID)
	if err != nil {
		log.Println("Failed setting up response exchange:", err)
		return
	}
	defer responseChannel.Close()

	// Send work to worker
	body := workID + "::hello"
	msg := amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(body),
	}

	if err := workChannel.Publish("", workQueue.Name, false, false, msg); err != nil {
		log.Println("Failed submitting work:", err)
		return
	}

	log.Printf(" [x] Sent %s", body)
	log.Printf(" [*] Waiting for response. To exit press CTRL+C")

	// Wait for worker to respond
	select {
	case res := <-response:
		log.Println("Response Body:", string(res.Body))
	case <-time.After(time.Second * 3):
		fmt.Println("timeout after 3 seconds")
	}
}

func setupWorkQueue(conn *amqp.Connection) (workChannel *amqp.Channel, workQueue amqp.Queue, err error) {
	// Send work through a channel to the worker
	workChannel, err = conn.Channel()
	if err != nil {
		log.Println("Failed opening queue channel:", err)
	}

	// This may be optional if its already declared. Not sure
	workQueue, err = workChannel.QueueDeclare(
		"work-queue-1", // name
		true,           // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		return
	}

	return
}

func setupResponseExchange(conn *amqp.Connection, workID string) (response <-chan amqp.Delivery, responseChannel *amqp.Channel, err error) {
	responseChannel, err = conn.Channel()
	if err != nil {
		log.Println("Failed setting up response channel")
		return
	}

	// This may be optional if its already declared. Not sure
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
		log.Println("Failed setting up response exchange")
		return
	}

	responseQueue, err := responseChannel.QueueDeclare(
		"queue-name-1", // name
		false,          // durable
		false,          // delete when unused
		true,           // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		log.Println("Failed declaring response queue")
		return
	}

	err = responseChannel.QueueBind(
		responseQueue.Name,  // queue name
		workID,              // routing key
		"response-exchange", // exchange
		false,               // noWait
		nil,                 // args
	)
	if err != nil {
		log.Println("Failed binding to response queue")
		return
	}

	// Wait for the worker to send response back through a specific exchange (even before work is sent)
	response, err = responseChannel.Consume(
		responseQueue.Name, // queue
		"",                 // consumer
		true,               // auto ack
		false,              // exclusive
		false,              // no local
		false,              // no wait
		nil,                // args
	)
	if err != nil {
		log.Println("Failed connecting to consumer:", err)
		return
	}

	return
}
