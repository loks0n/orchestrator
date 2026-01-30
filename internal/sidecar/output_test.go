package sidecar

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"orchestrator/internal/job"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOutputHandler_ProcessUpload(t *testing.T) {
	var receivedContent []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("Expected PUT method, got %s", r.Method)
		}
		receivedContent, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create temp file
	tmpDir, _ := os.MkdirTemp("", "output-test")
	defer os.RemoveAll(tmpDir)

	filePath := filepath.Join(tmpDir, "output.txt")
	expectedContent := "output file content"
	os.WriteFile(filePath, []byte(expectedContent), 0o644)

	handler := NewOutputHandler(30*time.Second, 3)
	output := job.Output{
		ID:   "test-output",
		Type: "upload",
		Path: filePath,
		URL:  server.URL,
	}

	result := handler.Process(context.Background(), output)

	if result.Status != "success" {
		t.Errorf("Expected status success, got %s", result.Status)
	}
	if result.Error != nil {
		t.Errorf("Expected no error, got %v", result.Error)
	}
	if string(receivedContent) != expectedContent {
		t.Errorf("Expected content %q, got %q", expectedContent, string(receivedContent))
	}
}

func TestOutputHandler_ProcessUploadFileNotFound(t *testing.T) {
	handler := NewOutputHandler(30*time.Second, 3)
	output := job.Output{
		ID:   "test-output",
		Type: "upload",
		Path: "/nonexistent/file.txt",
		URL:  "http://example.com",
	}

	result := handler.Process(context.Background(), output)

	if result.Status != "failed" {
		t.Errorf("Expected status failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Error("Expected error")
	}
}

func TestOutputHandler_ProcessEventJSON(t *testing.T) {
	// Create temp file with JSON content
	tmpDir, _ := os.MkdirTemp("", "output-test")
	defer os.RemoveAll(tmpDir)

	filePath := filepath.Join(tmpDir, "output.json")
	jsonContent := `{"key": "value", "number": 42}`
	os.WriteFile(filePath, []byte(jsonContent), 0o644)

	handler := NewOutputHandler(30*time.Second, 3)
	output := job.Output{
		ID:   "test-output",
		Type: "event",
		Path: filePath,
	}

	result := handler.Process(context.Background(), output)

	if result.Status != "success" {
		t.Errorf("Expected status success, got %s", result.Status)
	}
	if result.Error != nil {
		t.Errorf("Expected no error, got %v", result.Error)
	}

	// Content should be parsed as JSON
	contentMap, ok := result.Content.(map[string]any)
	if !ok {
		t.Fatalf("Expected map content, got %T", result.Content)
	}
	if contentMap["key"] != "value" {
		t.Errorf("Expected key=value, got %v", contentMap["key"])
	}
}

func TestOutputHandler_ProcessEventPlainText(t *testing.T) {
	// Create temp file with plain text
	tmpDir, _ := os.MkdirTemp("", "output-test")
	defer os.RemoveAll(tmpDir)

	filePath := filepath.Join(tmpDir, "output.txt")
	textContent := "plain text content"
	os.WriteFile(filePath, []byte(textContent), 0o644)

	handler := NewOutputHandler(30*time.Second, 3)
	output := job.Output{
		ID:   "test-output",
		Type: "event",
		Path: filePath,
	}

	result := handler.Process(context.Background(), output)

	if result.Status != "success" {
		t.Errorf("Expected status success, got %s", result.Status)
	}

	// Content should be plain string
	contentStr, ok := result.Content.(string)
	if !ok {
		t.Fatalf("Expected string content, got %T", result.Content)
	}
	if contentStr != textContent {
		t.Errorf("Expected content %q, got %q", textContent, contentStr)
	}
}

func TestOutputHandler_ProcessUnknownType(t *testing.T) {
	handler := NewOutputHandler(30*time.Second, 3)
	output := job.Output{
		ID:   "test-output",
		Type: "unknown",
		Path: "/some/path",
	}

	result := handler.Process(context.Background(), output)

	if result.Status != "failed" {
		t.Errorf("Expected status failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Error("Expected error for unknown type")
	}
}

func TestOutputHandler_UploadServerError_Retries(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
	}))
	defer server.Close()

	tmpDir, _ := os.MkdirTemp("", "output-test")
	defer os.RemoveAll(tmpDir)

	filePath := filepath.Join(tmpDir, "output.txt")
	os.WriteFile(filePath, []byte("test content"), 0o644)

	handler := NewOutputHandler(5*time.Second, 2) // 2 retries = 3 total attempts
	output := job.Output{
		ID:   "test-output",
		Type: "upload",
		Path: filePath,
		URL:  server.URL,
	}

	result := handler.Process(context.Background(), output)

	if result.Status != "failed" {
		t.Errorf("Expected status failed, got %s", result.Status)
	}
	if attempts != 3 {
		t.Errorf("Expected 3 attempts (1 + 2 retries), got %d", attempts)
	}
}

func TestOutputHandler_UploadClientError_NoRetry(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("bad request"))
	}))
	defer server.Close()

	tmpDir, _ := os.MkdirTemp("", "output-test")
	defer os.RemoveAll(tmpDir)

	filePath := filepath.Join(tmpDir, "output.txt")
	os.WriteFile(filePath, []byte("test content"), 0o644)

	handler := NewOutputHandler(5*time.Second, 3)
	output := job.Output{
		ID:   "test-output",
		Type: "upload",
		Path: filePath,
		URL:  server.URL,
	}

	result := handler.Process(context.Background(), output)

	if result.Status != "failed" {
		t.Errorf("Expected status failed, got %s", result.Status)
	}
	// 4xx errors should not be retried
	if attempts != 1 {
		t.Errorf("Expected 1 attempt (no retries for 4xx), got %d", attempts)
	}
}

func TestOutputHandler_UploadContextCancelled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	tmpDir, _ := os.MkdirTemp("", "output-test")
	defer os.RemoveAll(tmpDir)

	filePath := filepath.Join(tmpDir, "output.txt")
	os.WriteFile(filePath, []byte("test content"), 0o644)

	handler := NewOutputHandler(30*time.Second, 3)
	output := job.Output{
		ID:   "test-output",
		Type: "upload",
		Path: filePath,
		URL:  server.URL,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	result := handler.Process(ctx, output)

	if result.Status != "failed" {
		t.Errorf("Expected status failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Error("Expected error for cancelled context")
	}
}

func TestOutputHandler_UploadSuccessAfterRetry(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	tmpDir, _ := os.MkdirTemp("", "output-test")
	defer os.RemoveAll(tmpDir)

	filePath := filepath.Join(tmpDir, "output.txt")
	os.WriteFile(filePath, []byte("test content"), 0o644)

	handler := NewOutputHandler(5*time.Second, 3)
	output := job.Output{
		ID:   "test-output",
		Type: "upload",
		Path: filePath,
		URL:  server.URL,
	}

	result := handler.Process(context.Background(), output)

	if result.Status != "success" {
		t.Errorf("Expected status success, got %s (error: %v)", result.Status, result.Error)
	}
	if attempts != 2 {
		t.Errorf("Expected 2 attempts, got %d", attempts)
	}
}

func TestOutputHandler_EventFileNotFound(t *testing.T) {
	handler := NewOutputHandler(30*time.Second, 3)
	output := job.Output{
		ID:   "test-output",
		Type: "event",
		Path: "/nonexistent/file.json",
	}

	result := handler.Process(context.Background(), output)

	if result.Status != "failed" {
		t.Errorf("Expected status failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Error("Expected error for missing file")
	}
}

func TestOutputHandler_EventEmptyFile(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "output-test")
	defer os.RemoveAll(tmpDir)

	filePath := filepath.Join(tmpDir, "empty.json")
	os.WriteFile(filePath, []byte(""), 0o644)

	handler := NewOutputHandler(30*time.Second, 3)
	output := job.Output{
		ID:   "test-output",
		Type: "event",
		Path: filePath,
	}

	result := handler.Process(context.Background(), output)

	if result.Status != "success" {
		t.Errorf("Expected status success, got %s", result.Status)
	}
	// Empty file should return empty string
	if result.Content != "" {
		t.Errorf("Expected empty content, got %v", result.Content)
	}
}
