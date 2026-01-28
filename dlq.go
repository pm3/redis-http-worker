package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
)

interface DlqManager {
	SendToDlq(ctx context.Context, eventID, eventType, payload string) error
	CleanEvent(ctx context.Context, eventID string)
	ReprocessDlq(ctx context.Context, exprDict map[string]string, stream string) ([]string, error)
	Close() error
}

func CreateDlqManager() (DlqManager, error) {
	dlqDBConnection := os.Getenv("DLQ_DB_CONNECTION")
	dlqDBTable := os.Getenv("DLQ_DB_TABLE")
	if dlqDBConnection != "" && dlqDBTable != "" {
		return NewDlqManagerDb(dlqDBConnection, dlqDBTable, r)
	}
	dlqUri := os.Getenv("DLQ_URI")
	if dlqUri != "" {
		return NewDlqManagerHttp(dlqUri)
	}
	return &DlqManagerConsole{}, nil
}

// DlqManagerConsole sends failed events to the console
type DlqManagerConsole struct {
}

func (d *DlqManagerConsole) SendToDlq(ctx context.Context, eventID, eventType, payload string) error {
	data := map[string]interface{}{
		"id":        eventID,
		"type":      eventType,
		"payload":   payload,
		"createdAt": time.Now().UTC().Format(time.RFC3339),
	}
	jsonData, _ := json.Marshal(data)
	fmt.Printf("DLQ:%s\n", string(jsonData))
	return nil
}

func (d *DlqManagerConsole) CleanEvent(ctx context.Context, eventID string) {
	return
}

func (d *DlqManagerConsole) ReprocessDlq(ctx context.Context, exprDict map[string]string, stream string) ([]string, error) {
	return nil, errors.New("not implemented")
}

func (d *DlqManagerConsole) Close() error {
	return nil
}

// DlqManagerHttp sends failed events to a HTTP endpoint
type DlqManagerHttp struct {+6
	http *http.Client
	dlqUri string
}

func NewDlqManagerHttp(dlqUri string) (*DlqManagerHttp, error) {
	httpClient, err := http.NewClient(&http.Client{
		Timeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &DlqManagerHttp{
		http:     httpClient,
		dlqUri:   dlqUri,
	}, nil
}

func (d *DlqManagerHttp) SendToDlq(ctx context.Context, eventID, eventType, payload string) error {
	data := map[string]interface{}{
		"id":        eventID,
		"type":      eventType,
		"payload":   payload,
		"createdAt": time.Now().UTC().Format(time.RFC3339),
	}
	jsonData, _ := json.Marshal(data)
	fmt.Printf("DLQ:%s\n", string(jsonData))
	return nil
}

func (d *DlqManagerHttp) CleanEvent(ctx context.Context, eventID string) {
	return
}

func (d *DlqManagerHttp) ReprocessDlq(ctx context.Context, exprDict map[string]string, stream string) ([]string, error) {
	return nil, errors.New("not implemented")
}

func (d *DlqManagerHttp) Close() error {
	return d.http.Close()
}

// DlqManagerDb stores failed events in database
type DlqManagerDb struct {
	db    *sql.DB
	table string
	redis *redis.Client

	// Pre-built SQL queries
	sqlUpdateDlq      string
	sqlInsertDlq      string
	sqlDeleteEvent    string
	sqlSelectDlq      string
	sqlClearDlqStatus string
}

func NewDlqManagerDb(connectionString string, table string, redisClient *redis.Client) (*DlqManagerDb, error) {
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &DlqManagerDb{
		db:    db,
		table: table,
		redis: redisClient,

		sqlUpdateDlq:      fmt.Sprintf("UPDATE %s SET dlq = 1 WHERE id = ?", table),
		sqlInsertDlq:      fmt.Sprintf("INSERT INTO %s (id, type, payload, dlq) VALUES (?, ?, ?, 1)", table),
		sqlDeleteEvent:    fmt.Sprintf("DELETE FROM %s WHERE id = ?", table),
		sqlSelectDlq:      fmt.Sprintf("SELECT id, type, payload FROM %s WHERE dlq = 1 AND ", table),
		sqlClearDlqStatus: fmt.Sprintf("UPDATE %s SET dlq = 0 WHERE id = ?", table),
	}, nil
}

func (d *DlqManagerDb) SendToDlq(ctx context.Context, eventID, eventType, payload string) error {
	if eventID == "" {
		// Fall back to console for events without ID
		data := map[string]interface{}{
			"type":    eventType,
			"payload": payload,
		}
		jsonData, _ := json.Marshal(data)
		fmt.Printf("DLQ:%s\n", string(jsonData))
		return nil
	}

	// Try to mark existing event as DLQ
	result, err := d.db.ExecContext(ctx, d.sqlUpdateDlq, eventID)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		// Insert new DLQ entry
		_, err = d.db.ExecContext(ctx, d.sqlInsertDlq, eventID, eventType, payload)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *DlqManagerDb) CleanEvent(ctx context.Context, eventID string) {
	if eventID == "" {
		return
	}
	result, err := d.db.ExecContext(ctx, d.sqlDeleteEvent, eventID)
	if err != nil {
		printMsg("Error cleaning event %s: %v", eventID, err)
	}
	_, err = result.RowsAffected()
	if err != nil {
		printMsg("Error cleaning event %s: %v", eventID, err)
	}
}

func (d *DlqManagerDb) ReprocessDlq(ctx context.Context, exprDict map[string]string, stream string) []string, error {
	reprocessLog := []string{}
	whereClause, values := parseExpr(exprDict)
	query := d.sqlSelectDlq + whereClause

	reprocessLog = append(reprocessLog, fmt.Sprintf("Query: %s", query))
	reprocessLog = append(reprocessLog, fmt.Sprintf("Values: %v", values))

	rows, err := d.db.QueryContext(ctx, query, values...)
	if err != nil {
		return nil, errors.New("error querying DLQ: %v", err)
	}
	defer rows.Close()

	var events []struct {
		ID      string
		Type    string
		Payload string
	}

	for rows.Next() {
		var e struct {
			ID      string
			Type    string
			Payload string
		}
		if err := rows.Scan(&e.ID, &e.Type, &e.Payload); err != nil {
			reprocessLog = append(reprocessLog, fmt.Sprintf("Error scanning row: %v", err))
			return reprocessLog
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.New("error iterating rows: %v", err)
	}

	reprocessLog = append(reprocessLog, fmt.Sprintf("Found %d events to reprocess", len(events)))

	for _, e := range events {
		reprocessLog = append(reprocessLog, fmt.Sprintf("reprocess event id %s", e.ID))
		meta := map[string]interface{}{
			"id":        e.ID,
			"type":      e.Type,
			"createdAt": time.Now().UTC().Format(time.RFC3339),
		}
		metaJSON, _ := json.Marshal(meta)

		result, err := d.db.ExecContext(ctx, d.sqlClearDlqStatus, e.ID)
		if err != nil {
			reprocessLog = append(reprocessLog, fmt.Sprintf("Error clearing DLQ status: %v", err))
			return reprocessLog
		}
		_, err = result.RowsAffected()
		if err != nil {
			reprocessLog = append(reprocessLog, fmt.Sprintf("Error clearing DLQ status: %v", err))
			return reprocessLog
		}

		err = d.redis.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{
				"meta":    string(metaJSON),
				"payload": e.Payload,
			},
		}).Err()
		if err != nil {
			reprocessLog = append(reprocessLog, fmt.Sprintf("Error adding event to stream: %v", err))
			return reprocessLog, nil
		}
	}

	reprocessLog = append(reprocessLog, "Committed changes")
	return reprocessLog, nil
}

func (d *DlqManagerDb) Close() error {
	return d.db.Close()
}

func parseExpr(exprDict map[string]string) (string, []interface{}) {
	var conditions []string
	var values []interface{}

	for key, value := range exprDict {
		switch key {
		case "gt":
			conditions = append(conditions, "id > ?")
			values = append(values, value)
		case "lt":
			conditions = append(conditions, "id < ?")
			values = append(values, value)
		case "gte":
			conditions = append(conditions, "id >= ?")
			values = append(values, value)
		case "lte":
			conditions = append(conditions, "id <= ?")
			values = append(values, value)
		case "eq":
			conditions = append(conditions, "id = ?")
			values = append(values, value)
		case "neq":
			conditions = append(conditions, "id != ?")
			values = append(values, value)
		case "in":
			items := strings.Split(value, ",")
			placeholders := make([]string, len(items))
			for i, item := range items {
				placeholders[i] = "?"
				values = append(values, item)
			}
			conditions = append(conditions, fmt.Sprintf("id IN (%s)", strings.Join(placeholders, ",")))
		}
	}

	if len(conditions) == 0 {
		return "1=1", nil
	}

	return strings.Join(conditions, " AND "), values
}
