## Hello World Go Rabbitmq

This is an example project using Golang and RabbitMQ.

There are 3 servers:

1. The initial work producer (who then immediately listens to an exchange topic)
2. Rabbitmq
3. The work consumer (who then immediately publishes to an exchange topic)

### Installation

```
go get -u -v github.com/rms1000watt/hello-world-go-rabbitmq
go get -u -v github.com/streadway/amqp
brew install rabbitmq
```

### Usage

In one window

```
rabbitmq-server
```

In another window

```
go run worker/main.go
```

In another window

```
go run requester/main.go
```

You can visit the Admin GUI at http://127.0.0.1:15672/ to see whats going on.

Username: guest
Password: guest
