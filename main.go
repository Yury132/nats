package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {

	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, _ := nats.Connect(url)
	defer nc.Drain()

	js, _ := jetstream.New(nc)

	cfg := jetstream.StreamConfig{
		Name:      "EVENTS",
		Retention: jetstream.WorkQueuePolicy,
		Subjects:  []string{"events.>"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, _ := js.CreateStream(ctx, cfg)
	fmt.Println("created the stream")
	printStreamState(ctx, stream)

	js.Publish(ctx, "events.us.page_loaded", []byte("q1"))
	js.Publish(ctx, "events.eu.mouse_clicked", []byte("q2"))
	js.Publish(ctx, "events.us.input_focused", []byte("q3"))
	fmt.Println("published 3 messages")

	fmt.Println("# Stream info without any consumers")
	printStreamState(ctx, stream)

	cons, _ := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name: "processor-1",
	})

	msgs, _ := cons.Fetch(2)
	for msg := range msgs.Messages() {
		msg.DoubleAck(ctx)
		// Нужно печатать полученные данные
		fmt.Println(string(msg.Data()))
	}

	fmt.Println("\n# Stream info with one consumer")
	printStreamState(ctx, stream)

	// Удаляем первого получателя
	stream.DeleteConsumer(ctx, "processor-1")

	fmt.Println("\n# Create non-overlapping consumers")
	cons1, _ := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:          "processor-us",
		FilterSubject: "events.us.>",
	})
	cons2, _ := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:          "processor-eu",
		FilterSubject: "events.eu.>",
	})

	js.Publish(ctx, "events.eu.mouse_clicked", []byte("n1"))
	js.Publish(ctx, "events.us.page_loaded", []byte("n2"))
	js.Publish(ctx, "events.us.input_focused", []byte("n3"))
	js.Publish(ctx, "events.eu.page_loaded", []byte("n4"))
	fmt.Println("published 4 messages")

	msgs, _ = cons1.Fetch(2)
	for msg := range msgs.Messages() {
		fmt.Printf("us Первый получил: %s\n", msg.Subject())
		fmt.Println("\n", string(msg.Data()))
		//msg.Ack()
		msg.DoubleAck(ctx)
	}
	// Для непрерывного получения сообщений
	//cons1.Consume()
	// Читает сообщение за сообщением
	//cons1.Next()

	msgs, _ = cons2.Fetch(2)
	for msg := range msgs.Messages() {
		fmt.Printf("eu Второй получил: %s\n", msg.Subject())
		fmt.Println("\n", string(msg.Data()))
		//msg.Ack()
		msg.DoubleAck(ctx)
	}
	printStreamState(ctx, stream)
}

func printStreamState(ctx context.Context, stream jetstream.Stream) {
	info, _ := stream.Info(ctx)
	b, _ := json.MarshalIndent(info.State, "", " ")
	fmt.Println(string(b))
}
