package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Воркер
func worker(id int, jobs <-chan string) {
	// Ожидаем получения данных для работы
	// Если данных нет в канале - блокировка
	for j := range jobs {
		fmt.Println("worker", id, "начал создание миниатюры по пути: ", j)
		// Создаем миниатюру, сохраняем данные в бд и прочее
		time.Sleep(time.Second * 1)
		fmt.Println("worker", id, "создал миниатюру по пути: ", j)
	}
}

func main() {

	// Каналы для воркера
	jobs := make(chan string, 100)

	// Сразу запускаем воркеров в горутинах
	// Они будут ожидать получения данных для работы
	for w := 1; w <= 3; w++ {
		go worker(w, jobs)
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
	js.Publish(ctx, "events.us.page_loaded-2", []byte("images/picture-004"))
	js.Publish(ctx, "events.eu.mouse_clicked-2", []byte("images/picture-005"))
	js.Publish(ctx, "events.us.input_focused-2", []byte("images/picture-006"))

	//fmt.Println("Поток до чтения данных получателем:")
	//printStreamState(ctx, stream)

	// Создаем получателя
	cons, _ := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name: "processor-1",
	})

	// В горутине получатель беспрерывно ждет входящих сообщений
	// При получении сообщений, передает пути к изображениям (задачи) воркерам
	go func() {
		cons.Consume(func(msg jetstream.Msg) {
			// Печатаем полученные данные
			fmt.Println("Получатель получил сообщение - ", string(msg.Data()))
			// Заполняем канал данными
			// Воркеры начнут работать
			jobs <- string(msg.Data())
			// Подтверждаем получение сообщения
			msg.DoubleAck(ctx)
			//msg.Ack()
		})
	}()

	// Завершение программы по Ctrl+C
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT)
	<-shutdown

	//fmt.Println("Поток после чтения данных получателем:")
	//printStreamState(ctx, stream)
}

// Отображаем текущее состояние потока
// func printStreamState(ctx context.Context, stream jetstream.Stream) {
// 	info, _ := stream.Info(ctx)
// 	b, _ := json.MarshalIndent(info.State, "", " ")
// 	fmt.Println(string(b))
// }
