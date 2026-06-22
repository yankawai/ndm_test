package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPPutAndGetFIFO(t *testing.T) {
	handler := NewHandler(NewBroker())

	assertRequest(t, handler, http.MethodPut, "/pet?v=cat", http.StatusOK, "")
	assertRequest(t, handler, http.MethodPut, "/pet?v=dog", http.StatusOK, "")
	assertRequest(t, handler, http.MethodPut, "/role?v=manager", http.StatusOK, "")
	assertRequest(t, handler, http.MethodPut, "/role?v=executive", http.StatusOK, "")

	assertRequest(t, handler, http.MethodGet, "/pet", http.StatusOK, "cat")
	assertRequest(t, handler, http.MethodGet, "/pet", http.StatusOK, "dog")
	assertRequest(t, handler, http.MethodGet, "/pet", http.StatusNotFound, "")
	assertRequest(t, handler, http.MethodGet, "/role", http.StatusOK, "manager")
	assertRequest(t, handler, http.MethodGet, "/role", http.StatusOK, "executive")
	assertRequest(t, handler, http.MethodGet, "/role", http.StatusNotFound, "")
}

func TestHTTPValidation(t *testing.T) {
	handler := NewHandler(NewBroker())

	assertRequest(t, handler, http.MethodPut, "/pet", http.StatusBadRequest, "")
	assertRequest(t, handler, http.MethodPut, "/pet?v=", http.StatusOK, "")
	assertRequest(t, handler, http.MethodGet, "/pet", http.StatusOK, "")
	assertRequest(t, handler, http.MethodGet, "/pet?timeout=-1", http.StatusBadRequest, "")
	assertRequest(t, handler, http.MethodGet, "/pet?timeout=x", http.StatusBadRequest, "")
}

func TestWaitingReceiversAreServedInRequestOrder(t *testing.T) {
	broker := NewBroker()

	first := make(chan string, 1)
	second := make(chan string, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	go popInto(ctx, broker, "pet", first)
	waitForWaiters(t, broker, "pet", 1)
	go popInto(ctx, broker, "pet", second)
	waitForWaiters(t, broker, "pet", 2)

	broker.Push("pet", "cat")
	broker.Push("pet", "dog")

	assertReceive(t, first, "cat")
	assertReceive(t, second, "dog")
}

func TestWaitingReceiverTimesOut(t *testing.T) {
	broker := NewBroker()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	if message, ok := broker.Pop(ctx, "pet", 10*time.Millisecond); ok || message != "" {
		t.Fatalf("expected timeout without message, got message=%q ok=%v", message, ok)
	}
}

func assertRequest(t *testing.T, handler http.Handler, method, target string, status int, body string) {
	t.Helper()

	request := httptest.NewRequest(method, target, nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	response := recorder.Result()
	defer response.Body.Close()

	if response.StatusCode != status {
		t.Fatalf("%s %s: expected status %d, got %d", method, target, status, response.StatusCode)
	}

	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("%s %s: read body: %v", method, target, err)
	}
	if string(data) != body {
		t.Fatalf("%s %s: expected body %q, got %q", method, target, body, string(data))
	}
}

func popInto(ctx context.Context, broker *Broker, queue string, out chan<- string) {
	message, ok := broker.Pop(ctx, queue, time.Second)
	if !ok {
		out <- ""
		return
	}
	out <- message
}

func waitForWaiters(t *testing.T, broker *Broker, queueName string, count int) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		broker.mu.Lock()
		queue := broker.queues[queueName]
		actual := 0
		if queue != nil {
			actual = queue.waiters.Len()
		}
		broker.mu.Unlock()

		if actual == count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("expected %d waiting receivers for queue %q", count, queueName)
}

func assertReceive(t *testing.T, ch <-chan string, expected string) {
	t.Helper()

	select {
	case actual := <-ch:
		if actual != expected {
			t.Fatalf("expected %q, got %q", expected, actual)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected to receive %q", expected)
	}
}
