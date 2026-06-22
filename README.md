# ndm_test

Тестовое задание: HTTP брокер очередей на Go.

## Запуск

```bash
go run . 8080
```

## Проверка

```bash
go test ./...
```

## API

```text
PUT /queue?v=message
GET /queue
GET /queue?timeout=N
```

Сообщения выдаются по FIFO. Ожидающие получатели обслуживаются в порядке поступления запросов.
