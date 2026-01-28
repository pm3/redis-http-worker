package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

// Configuration
var (
	redisHostPort    = getEnv("REDIS_HOST_PORT", "redis:6379")
	redisPassword    = os.Getenv("REDIS_PASSWORD")
	group            = getEnv("GROUP", "workers")
	consumerPrefix   = getEnv("CONSUMER_PREFIX", "c")
	streamsString    = getEnv("STREAMS", "critical,fast,default,slow")
	defaultWorkerURI = getEnv("DEFAULT_WORKER_URI", "http://localhost:8000/worker/[EVENT_TYPE]")
	metricsPort      = getEnvInt("METRICS_PORT", 9000)
	connectTimeout   = getEnvInt("CONNECT_TIMEOUT", 2)
	readTimeout      = getEnvInt("READ_TIMEOUT", 60)
)

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

const tsLayout = "2006-01-02 15:04:05"
func printMsg(msg string, args ...any) {
	ts := time.Now().Format(tsLayout)
	body := fmt.Sprintf(msg, args...)
	fmt.Printf("%s: %s\n", ts, body)
}

func nowTs() int64 {
	return time.Now().Unix()
}

type FlexString string
func (s *FlexString) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)

	// null -> prázdny string (alebo si nastav nil cez *FlexString, ak chceš rozlišovať)
	if len(b) == 0 || bytes.Equal(b, []byte("null")) {
		*s = ""
		return nil
	}

	// keď príde normálny JSON string
	if b[0] == '"' {
		var v string
		if err := json.Unmarshal(b, &v); err != nil {
			return err
		}
		*s = FlexString(v)
		return nil
	}

	// keď príde číslo (alebo iný literal) -> ulož raw text
	*s = FlexString(string(b))
	return nil
}
// EventMeta represents the metadata of an event
type EventMeta struct {
	ID        FlexString `json:"id,omitempty"`
	Type      string `json:"type,omitempty"`
	Queue     string `json:"queue,omitempty"`
	URL       string `json:"url,omitempty"`
	CreatedAt string `json:"createdAt,omitempty"`
}

func ensureGroup(ctx context.Context, r *redis.Client, stream string) error {
	err := r.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func waitForRedis(ctx context.Context, r *redis.Client) {
	sleepTime := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:
			break
		}

		if err := r.Ping(ctx).Err(); err != nil {
			printMsg("Failed to connect to Redis: %v", err)
			time.Sleep(sleepTime)
			if sleepTime < 10*time.Second {
				sleepTime += time.Second
			}
			continue
		}
		return
	}
}

func scheduleRetry(ctx context.Context, r *redis.Client, stream string, meta *EventMeta, payload string, delay int) error {
	nextAttemptTs := nowTs() + int64(delay)

	// Update meta with next attempt time
	metaMap := map[string]interface{}{
		"id":            string(meta.ID),
		"type":          meta.Type,
		"queue":         meta.Queue,
		"url":           meta.URL,
		"createdAt":     meta.CreatedAt,
		"nextAttemptAt": nextAttemptTs,
	}
	metaJSON, _ := json.Marshal(metaMap)

	retryID, err := r.XAdd(ctx, &redis.XAddArgs{
		Stream: "events:retry",
		Values: map[string]interface{}{
			"meta":    string(metaJSON),
			"stream":  stream,
			"payload": payload,
		},
	}).Result()
	if err != nil {
		return err
	}

	return r.ZAdd(ctx, "events:retry:delays", redis.Z{
		Score:  float64(nextAttemptTs),
		Member: retryID,
	}).Err()
}

func checkReadyHost(ctx context.Context, client *http.Client, url string) bool {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(connectTimeout)*2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "OPTIONS", url, nil)
	if err != nil {
		printMsg("Check ready host: %s error=%v", url, err)
		return false
	}

	resp, err := client.Do(req)
	if err != nil {
		printMsg("Check ready host: %s error=%v", url, err)
		return false
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	
	printMsg("Check ready host: %s status=%d", url, resp.StatusCode)
	switch resp.StatusCode {
	case 200, 204, 301, 302, 303, 307, 308, 404, 405:
		return true
	default:
		return false
	}
}

func processMessage(
	ctx context.Context,
	r *redis.Client,
	client *http.Client,
	dlq *DlqManagerDb,
	stream, msgID string,
	fields map[string]interface{},
) string {
	now := time.Now()

	var meta EventMeta
	metaStr, _ := fields["meta"].(string)
	json.Unmarshal([]byte(metaStr), &meta)

	eventID := string(meta.ID)
	payload, _ := fields["payload"].(string)
	eventType := meta.Type
	if eventType == "" {
		eventType = "unknown"
	}

	// Track event age
	if meta.CreatedAt != "" {
		if createdAt, err := time.Parse(time.RFC3339, meta.CreatedAt); err == nil {
			ageSeconds := time.Since(createdAt).Seconds()
			eventAgeSeconds.WithLabelValues(stream, eventType).Observe(ageSeconds)
		}
	}

	// Determine URL
	url := meta.URL
	if url == "" {
		url = defaultWorkerURI
	}
	if strings.Contains(url, "[EVENT_TYPE]") {
		url = strings.ReplaceAll(url, "[EVENT_TYPE]", eventType)
	}

	printMsg("Processing [event=%s] type=%s url=%s", eventID, eventType, url)

	headers := map[string]string{
		"X-Event-Type": eventType,
		"X-Event-Queue": meta.Queue,
		"Content-Type": "text/plain",
	}
	if eventID != "" {
		headers["X-Event-Id"] = eventID
		url = strings.ReplaceAll(url, "[EVENT_ID]", eventID)
	}

	resp, err := callWorker(ctx, client, url, headers, payload)

	if err := r.XAck(ctx, stream, group, msgID).Err(); err != nil {
		printMsg("Failed to ACK message [event=%s] %s: %v", eventID, msgID, err)
	}
	if err := r.XDel(ctx, stream, msgID).Err(); err != nil {
		printMsg("Failed to delete message [event=%s] %s: %v", eventID, msgID, err)
	}

	if err != nil {
		eventsFailed.WithLabelValues(stream, eventType, "http_timeout_or_conn_error").Inc()
		// re-add message to end of stream
		if err := r.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{
				"meta":    metaStr,
				"payload": payload,
			},
		}).Err(); err != nil {
			printMsg("Failed to re-add message to stream [event=%s] %s: %v", eventID, stream, err)
		}
		return url
	}
	defer resp.Body.Close()

	duration := time.Since(now).Seconds()
	printMsg("Processed [event=%s] type=%s duration=%.3f status=%d", eventID, eventType, duration, resp.StatusCode)
	eventProcessingDuration.WithLabelValues(eventType).Observe(duration)
	errCountKey := fmt.Sprintf("err_count:%s:%s", stream, eventID)

	// Success
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		eventsProcessed.WithLabelValues(stream, eventType).Inc()
		dlq.CleanEvent(ctx, eventID)
		r.Delete(ctx, errCountKey).Err();
		io.Copy(io.Discard, resp.Body)
		return ""
	}

	// Log error response
	body, _ := io.ReadAll(resp.Body)
	bodyStr := strings.ReplaceAll(strings.ReplaceAll(string(body), "\n", "\\n"), "\r", "")
	printMsg("Error response [event=%s]: %d\\n%s", eventID, resp.StatusCode, bodyStr)
	eventsFailed.WithLabelValues(stream, eventType, fmt.Sprintf("http_%d", resp.StatusCode)).Inc()

	// User retryable with X-Retry-After header
	retryTime := resp.Header.Get("X-Retry-After")
	if resp.StatusCode == 400 && retryTime != "" {
		if retrySeconds, err := strconv.Atoi(retryTime); err == nil {
			errCount, _ := r.Incr(ctx, errCountKey).Int()
			if errCount >= 5 {
				printMsg("Max retries reached for event [event=%s] %s: %v", eventID, stream, err)
				err := sendToDlq(ctx, eventID, eventType, payload, fmt.Sprintf("http_%d", resp.StatusCode))
				if err != nil {
					printMsg("Failed to send event [event=%s] to DLQ: %v", eventID, err)
				}
				r.Delete(ctx, errCountKey).Err();
				printMsg("Sent to DLQ [event=%s] type=%s status=%d", eventID, eventType, resp.StatusCode)
				return ""
			}
			if err := scheduleRetry(ctx, r, stream, &meta, payload, retrySeconds, errCount); err != nil {
				printMsg("Failed to schedule retry for event [event=%s] %s: %v", eventID, stream, err)
			}
			printMsg("scheduled retry [event=%s] type=%s retry_time=%s err_count=%d", eventID, eventType, retryTime, errCount)
			return ""
		}
	}

	// Network retryable
	switch resp.StatusCode {
	case 502, 503, 504:
		// re-add message to end of stream
		if err := r.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{
				"meta":    metaStr,
				"payload": payload,
			},
		}).Err(); err != nil {
			printMsg("Failed to re-add message to stream %s: %v", stream, err)
		}
		return url
	}

	// Non-retryable - send to DLQ
	if err := dlq.SendToDlq(ctx, eventID, eventType, payload); err != nil {
		printMsg("Failed to send event [event=%s] to DLQ: %v", eventID, err)
	}
	r.Delete(ctx, errCountKey).Err();
	printMsg("Sent to DLQ [event=%s] type=%s status=%d", eventID, eventType, resp.StatusCode)
	return ""
}

func callWorker(ctx context.Context, client *http.Client, url string, events map[string]string, payload string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(payload))
	if err != nil {
		return nil, err
	}
	for k, v := range events {
		req.Header.Set(k, v)
	}
	return client.Do(req)
}

func consumeStream(ctx context.Context, stream string, r *redis.Client, client *http.Client, dlq *DlqManagerDb, wg *sync.WaitGroup) {
	defer wg.Done()

	if err := ensureGroup(ctx, r, stream); err != nil {
		printMsg("Failed to ensure group for stream %s: %v", stream, err)
		return
	}

	consumer := fmt.Sprintf("%s-%s-%d", consumerPrefix, stream, os.Getpid())
	var waitReadyHost string
	
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if waitReadyHost != "" {
			if checkReadyHost(ctx, client, waitReadyHost) {
				waitReadyHost = ""
			} else {
				time.Sleep(time.Second)
				continue
			}
		}

		streams, err := r.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, ">"},
			Count:    1,
			Block:    time.Second,
		}).Result()

		if err != nil {
			if err == redis.Nil {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			printMsg("Failed to read message from stream %s: %v", stream, err)
			waitForRedis(ctx, r)
			continue
		}

		for _, s := range streams {
			for _, msg := range s.Messages {
				waitReadyHost = processMessage(ctx, r, client, dlq, stream, msg.ID, msg.Values)
			}
		}
	}
}

func schedulerLoop(ctx context.Context, r *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		now := nowTs()
		dueIDs, err := r.ZRangeByScore(ctx, "events:retry:delays", &redis.ZRangeBy{
			Min:    "0",
			Max:    strconv.FormatInt(now, 10),
			Offset: 0,
			Count:  200,
		}).Result()

		if err != nil {
			if ctx.Err() != nil {
				return
			}
			printMsg("Scheduler loop failed: %v", err)
			waitForRedis(ctx, r)
			continue
		}

	for _, retryID := range dueIDs {
		msgs, err := r.XRange(ctx, "events:retry", retryID, retryID).Result()
		if err != nil {
			printMsg("Failed to get retry message %s: %v", retryID, err)
			if err := r.ZRem(ctx, "events:retry:delays", retryID).Err(); err != nil {
				printMsg("Failed to remove retry delay %s: %v", retryID, err)
			}
			continue
		}
		if len(msgs) == 0 {
			if err := r.ZRem(ctx, "events:retry:delays", retryID).Err(); err != nil {
				printMsg("Failed to remove retry delay %s: %v", retryID, err)
			}
			continue
		}

		msg := msgs[0]

		// parse meta
		metaStr, _ := msg.Values["meta"].(string)
		var meta map[string]interface{}
		json.Unmarshal([]byte(metaStr), &meta)

		stream, _ := msg.Values["stream"].(string)
		if stream == "" {
			stream = "events:default"
		}
		payload, _ := msg.Values["payload"].(string)

		if meta["id"] != nil {
			printMsg("Moved retry for event %v to %s", meta["id"], stream)
		}


		if err := r.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{
				"meta":    metaStr,
				"payload": payload,
			},
		}).Err(); err != nil {
			printMsg("Failed to add retry message to stream %s: %v", stream, err)
			continue
		}
		if err := r.ZRem(ctx, "events:retry:delays", retryID).Err(); err != nil {
			printMsg("Failed to remove retry delay %s: %v", retryID, err)
		}
		if err := r.XDel(ctx, "events:retry", retryID).Err(); err != nil {
			printMsg("Failed to delete retry message %s: %v", retryID, err)
		}
	}

		time.Sleep(time.Second)
	}
}

func main() {

	// Context with cancellation for graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	
	// Create Redis client
	r := redis.NewClient(&redis.Options{
		Addr:     redisHostPort,
		Password: redisPassword,
	})

	// Wait for Redis
	waitForRedis(ctx, r)

	// Parse streams
	streamNames := strings.Split(streamsString, ",")
	streams := make([]string, len(streamNames))
	for i, s := range streamNames {
		streams[i] = "events:" + strings.TrimSpace(s)
	}

	// Ensure groups exist
	for _, s := range streams {
		if err := ensureGroup(ctx, r, s); err != nil {
			printMsg("Failed to ensure group for %s: %v", s, err)
			return
		}
	}

	// Create HTTP client
	httpClient := &http.Client{
		Timeout: time.Duration(readTimeout) * time.Second,
		Transport: &http.Transport{
			ResponseHeaderTimeout: time.Duration(connectTimeout) * time.Second,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			IdleConnTimeout:       90 * time.Second,
		},
	}

	// Create DLQ manager
	dlq, err := CreateDlqManager()
	if err != nil {
		printMsg("Failed to create DLQ manager: %v", err)
		return
	}

	// Add reprocess-dlq endpoint
	http.HandleFunc("/reprocess-dlq", func(w http.ResponseWriter, req *http.Request) {
		query := req.URL.Query()
		exprDict := make(map[string]string)
		for k, v := range query {
			if len(v) > 0 {
				exprDict[k] = v[0]
			}
		}
		reprocessLog := dlq.ReprocessDlq(req.Context(), exprDict, "events:default")
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte(strings.Join(reprocessLog, "\n")))
	})

	// Start metrics server
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status": "ok"}`))
	})
	http.Handle("/metrics", promhttp.Handler())

	go func() {
		addr := fmt.Sprintf("0.0.0.0:%d", metricsPort)
		printMsg("Server running on :%d", metricsPort)
		if err := http.ListenAndServe(addr, nil); err != nil && err != http.ErrServerClosed {
			printMsg("Metrics server error: %v", err)
			return
		}
	}()

	// Start consumer tasks
	var wg sync.WaitGroup

	for _, s := range streams {
		wg.Add(1)
		go consumeStream(ctx, s, r, httpClient, dlq, &wg)
		if ctx.Err() != nil {
			return
		}
	}

	// Start scheduler task
	wg.Add(1)
	go schedulerLoop(ctx, r, &wg)
	if ctx.Err() != nil {
		return
	}
	// Wait for all goroutines to finish
	wg.Wait()
	printMsg("All workers stopped")
}


