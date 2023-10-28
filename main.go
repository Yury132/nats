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

	// Адрес сервера nats
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	// Подключаемся к серверу
	nc, _ := nats.Connect(url)
	defer nc.Drain()

	js, _ := jetstream.New(nc)

	cfg := jetstream.StreamConfig{
		Name: "EVENTS",
		// Очередь
		Retention: jetstream.WorkQueuePolicy,
		Subjects:  []string{"events.>"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Создаем поток
	stream, _ := js.CreateStream(ctx, cfg)
	fmt.Println("Создали поток")

	fmt.Println("Начальное состояние потока:")
	// Смотрим что сейчас в потоке
	printStreamState(ctx, stream)

	fmt.Println("Отправили 3 сообщения")
	// Отправляем данные
	js.Publish(ctx, "events.us.page_loaded", []byte("важная информация 1"))
	js.Publish(ctx, "events.eu.mouse_clicked", []byte("важная информация 2"))
	js.Publish(ctx, "events.us.input_focused", []byte("важная информация 3"))

	fmt.Println("Поток до чтения данных получателем:")
	printStreamState(ctx, stream)

	// Создаем получателя
	cons, _ := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name: "processor-1",
	})

	// Получатель читает только 3 сообщения
	msgs, _ := cons.Fetch(3)
	for msg := range msgs.Messages() {
		msg.DoubleAck(ctx)
		// Печатаем полученные данные
		fmt.Println("Получатель прочитал сообщение - ", string(msg.Data()))
	}

	fmt.Println("Поток после чтения данных получателем:")
	printStreamState(ctx, stream)

	fmt.Println("Удалили получателя")
	// Удаляем получателя
	stream.DeleteConsumer(ctx, "processor-1")

	fmt.Println("Создаем 2 новых получателей")
	// Новые получатели
	cons1, _ := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:          "processor-us",
		FilterSubject: "events.us.>",
	})
	cons2, _ := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:          "processor-eu",
		FilterSubject: "events.eu.>",
	})

	fmt.Println("Отправили еще 4 новых сообщения")
	// Публикуем данные
	js.Publish(ctx, "events.eu.mouse_clicked", []byte("новая информация 1"))
	js.Publish(ctx, "events.us.page_loaded", []byte("новая информация 2"))
	js.Publish(ctx, "events.us.input_focused", []byte("новая информация 3"))
	js.Publish(ctx, "events.eu.page_loaded", []byte("новая информация 4"))

	fmt.Println("Состояние потока:")
	printStreamState(ctx, stream)

	// Первый получатель читает только 2 собщения
	msgs, _ = cons1.Fetch(2)
	for msg := range msgs.Messages() {
		fmt.Printf("us Первый получил: %s\n", msg.Subject())
		fmt.Println(string(msg.Data()))
		//msg.Ack()
		// Сообщаем серверу о прочтении данного сообщения
		msg.DoubleAck(ctx)
	}

	// Для непрерывного получения сообщений
	//cons1.Consume()
	// Читает сообщение за сообщением
	//cons1.Next()

	// Второй получатель читает только 2 собщения
	msgs, _ = cons2.Fetch(2)
	for msg := range msgs.Messages() {
		fmt.Printf("eu Второй получил: %s\n", msg.Subject())
		fmt.Println(string(msg.Data()))
		//msg.Ack()
		// Сообщаем серверу о прочтении данного сообщения
		msg.DoubleAck(ctx)
	}
	fmt.Println("Конечное состояние потока:")
	printStreamState(ctx, stream)
}

// Отображаем текущее состояние потока
func printStreamState(ctx context.Context, stream jetstream.Stream) {
	info, _ := stream.Info(ctx)
	b, _ := json.MarshalIndent(info.State, "", " ")
	fmt.Println(string(b))
}
