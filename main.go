package main

import (
	"container/list"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Broker struct {
	mu     sync.Mutex
	queues map[string]*queue
}

type queue struct {
	messages list.List
	waiters  list.List
}

type waiter struct {
	ch   chan string
	elem *list.Element
}

func NewBroker() *Broker {
	return &Broker{queues: make(map[string]*queue)}
}

func (b *Broker) Push(name, message string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	q := b.queueLocked(name)
	if elem := q.waiters.Front(); elem != nil {
		w := elem.Value.(*waiter)
		q.waiters.Remove(elem)
		w.elem = nil
		w.ch <- message
		b.cleanupLocked(name, q)
		return
	}

	q.messages.PushBack(message)
}

func (b *Broker) Pop(ctx context.Context, name string, wait time.Duration) (string, bool) {
	b.mu.Lock()
	q := b.queues[name]
	if q != nil {
		elem := q.messages.Front()
		if elem != nil {
			message := elem.Value.(string)
			q.messages.Remove(elem)
			b.cleanupLocked(name, q)
			b.mu.Unlock()
			return message, true
		}
	}

	if wait <= 0 {
		b.mu.Unlock()
		return "", false
	}

	if q == nil {
		q = b.queueLocked(name)
	}
	w := &waiter{ch: make(chan string, 1)}
	w.elem = q.waiters.PushBack(w)
	b.mu.Unlock()

	select {
	case message := <-w.ch:
		return message, true
	case <-ctx.Done():
		return b.cancelWait(name, q, w)
	}
}

func (b *Broker) queueLocked(name string) *queue {
	q := b.queues[name]
	if q == nil {
		q = &queue{}
		b.queues[name] = q
	}
	return q
}

func (b *Broker) cancelWait(name string, q *queue, w *waiter) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if w.elem == nil {
		return <-w.ch, true
	}

	q.waiters.Remove(w.elem)
	w.elem = nil
	b.cleanupLocked(name, q)
	return "", false
}

func (b *Broker) cleanupLocked(name string, q *queue) {
	if q.messages.Len() == 0 && q.waiters.Len() == 0 {
		delete(b.queues, name)
	}
}

func NewHandler(broker *Broker) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queueName := strings.TrimPrefix(r.URL.Path, "/")
		if queueName == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		switch r.Method {
		case http.MethodPut:
			put(w, r, broker, queueName)
		case http.MethodGet:
			get(w, r, broker, queueName)
		default:
			w.Header().Set("Allow", "GET, PUT")
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
}

func put(w http.ResponseWriter, r *http.Request, broker *Broker, queueName string) {
	values, ok := r.URL.Query()["v"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	message := ""
	if len(values) > 0 {
		message = values[0]
	}
	broker.Push(queueName, message)
	w.WriteHeader(http.StatusOK)
}

func get(w http.ResponseWriter, r *http.Request, broker *Broker, queueName string) {
	timeout, ok := timeoutFromQuery(r)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	cancel := func() {}
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	message, found := broker.Pop(ctx, queueName, timeout)
	if !found {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if _, err := io.WriteString(w, message); err != nil {
		return
	}
}

func timeoutFromQuery(r *http.Request) (time.Duration, bool) {
	values, exists := r.URL.Query()["timeout"]
	if !exists {
		return 0, true
	}

	seconds, err := strconv.ParseInt(values[0], 10, 64)
	const maxTimeoutSeconds = int64(1<<63-1) / int64(time.Second)
	if err != nil || seconds < 0 || seconds > maxTimeoutSeconds {
		return 0, false
	}

	return time.Duration(seconds) * time.Second, true
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: queuebroker <port>")
		os.Exit(1)
	}

	if err := http.ListenAndServe(":"+os.Args[1], NewHandler(NewBroker())); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
