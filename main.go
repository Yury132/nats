package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Воркер
func worker(id int, jobs <-chan string, wg *sync.WaitGroup) {
	// Ожидаем получения данных для работы
	// Если данных нет в канале - блокировка
	for j := range jobs {
		fmt.Println("worker", id, "начал создание миниатюры по пути: ", j)
		// Создаем миниатюру, сохраняем данные в бд и прочее
		time.Sleep(time.Second * 1)
		fmt.Println("worker", id, "создал миниатюру по пути: ", j)
		wg.Done()
	}
}

func main() {

	// Каналы для воркера
	jobs := make(chan string, 100)

	var wg sync.WaitGroup

	// Сразу запускаем воркеров в горутинах
	// Они будут ожидать получения данных для работы
	for w := 1; w <= 2; w++ {
		go worker(w, jobs, &wg)
	}

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

	// Смотрим что сейчас в потоке
	//fmt.Println("Начальное состояние потока:")

	//printStreamState(ctx, stream)

	// Отправляем данные
	fmt.Println("Отправили 6 сообщений")

	js.Publish(ctx, "events.us.page_loaded", []byte("images/picture-001"))
	js.Publish(ctx, "events.eu.mouse_clicked", []byte("images/picture-002"))
	js.Publish(ctx, "events.us.input_focused", []byte("images/picture-003"))
	js.Publish(ctx, "events.us.page_loaded", []byte("images/picture-004"))
	js.Publish(ctx, "events.eu.mouse_clicked", []byte("images/picture-005"))
	js.Publish(ctx, "events.us.input_focused", []byte("images/picture-006"))

	//fmt.Println("Поток до чтения данных получателем:")
	//printStreamState(ctx, stream)

	// Создаем получателя
	cons, _ := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name: "processor-1",
	})

	// Костыль
	wg.Add(1)

	// Нужно запустить получателя в бесконечном цикле для чтения входящих сообшений
	go func() {
		// Бесконечный цикл на получение сообщений
		for {
			// После таймаута Next обрывает соединение с сервером nats
			msg, err := cons.Next()
			if err != nil {
				fmt.Println(err)
			}
			msg.DoubleAck(ctx)
			// Заполняем канал данными
			// Воркеры начнут работать
			jobs <- string(msg.Data())
			wg.Add(1)
			// Печатаем полученные данные
			fmt.Println("Получатель получил сообщение - ", string(msg.Data()))
		}
	}()

	// Это работает, но это НЕ бесконечный цикл
	// // Получатель читает определеное количество сообщений
	// msgs, _ := cons.Fetch(6)
	// for msg := range msgs.Messages() {
	// 	msg.DoubleAck(ctx)
	// 	// Заполняем канал данными
	// 	// Воркеры начнут работать
	// 	jobs <- string(msg.Data())
	// 	wg.Add(1)
	// 	// Печатаем полученные данные
	// 	fmt.Println("Получатель получил сообщение - ", string(msg.Data()))
	// }

	//fmt.Println("Поток после чтения данных получателем:")
	//printStreamState(ctx, stream)

	// Ждем завершения работы воркерами
	wg.Wait()

	//r
	//r
	//коммент все что ниже для теста воркер пула
	// fmt.Println("Удалили получателя")
	// // Удаляем получателя
	// stream.DeleteConsumer(ctx, "processor-1")

	// fmt.Println("Создаем 2 новых получателей")
	// // Новые получатели
	// cons1, _ := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
	// 	Name:          "processor-us",
	// 	FilterSubject: "events.us.>",
	// })
	// cons2, _ := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
	// 	Name:          "processor-eu",
	// 	FilterSubject: "events.eu.>",
	// })

	// fmt.Println("Отправили еще 4 новых сообщения")
	// // Публикуем данные
	// js.Publish(ctx, "events.eu.mouse_clicked", []byte("новая информация 1"))
	// js.Publish(ctx, "events.us.page_loaded", []byte("новая информация 2"))
	// js.Publish(ctx, "events.us.input_focused", []byte("новая информация 3"))
	// js.Publish(ctx, "events.eu.page_loaded", []byte("новая информация 4"))

	// fmt.Println("Состояние потока:")
	// printStreamState(ctx, stream)

	// // Первый получатель читает только 2 собщения
	// msgs, _ = cons1.Fetch(2)
	// for msg := range msgs.Messages() {
	// 	fmt.Printf("us Первый получил: %s\n", msg.Subject())
	// 	fmt.Println(string(msg.Data()))
	// 	//msg.Ack()
	// 	// Сообщаем серверу о прочтении данного сообщения
	// 	msg.DoubleAck(ctx)
	// }

	// // Для непрерывного получения сообщений
	// //cons1.Consume()
	// // Читает сообщение за сообщением
	// //cons1.Next()

	// // Второй получатель читает только 2 собщения
	// msgs, _ = cons2.Fetch(2)
	// for msg := range msgs.Messages() {
	// 	fmt.Printf("eu Второй получил: %s\n", msg.Subject())
	// 	fmt.Println(string(msg.Data()))
	// 	//msg.Ack()
	// 	// Сообщаем серверу о прочтении данного сообщения
	// 	msg.DoubleAck(ctx)
	// }
	// fmt.Println("Конечное состояние потока:")
	// printStreamState(ctx, stream)
}

// Отображаем текущее состояние потока
// func printStreamState(ctx context.Context, stream jetstream.Stream) {
// 	info, _ := stream.Info(ctx)
// 	b, _ := json.MarshalIndent(info.State, "", " ")
// 	fmt.Println(string(b))
// }
