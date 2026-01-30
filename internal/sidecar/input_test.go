package sidecar

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestInputHandler_Download(t *testing.T) {
	// Create test server
	expectedContent := "test file content"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(expectedContent))
	}))
	defer server.Close()

	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "input-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	handler := NewInputHandler(30 * time.Second)
	destPath := filepath.Join(tmpDir, "subdir", "downloaded.txt")

	err = handler.Download(context.Background(), server.URL, destPath)
	if err != nil {
		t.Fatalf("Download() error = %v", err)
	}

	// Verify file content
	content, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("Failed to read downloaded file: %v", err)
	}

	if string(content) != expectedContent {
		t.Errorf("Expected content %q, got %q", expectedContent, string(content))
	}
}

func TestInputHandler_DownloadError(t *testing.T) {
	// Create test server that returns error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	tmpDir, _ := os.MkdirTemp("", "input-test")
	defer os.RemoveAll(tmpDir)

	handler := NewInputHandler(30 * time.Second)
	destPath := filepath.Join(tmpDir, "downloaded.txt")

	err := handler.Download(context.Background(), server.URL, destPath)
	if err == nil {
		t.Error("Expected error for 404 response")
	}
}

func TestInputHandler_DownloadContextCancelled(t *testing.T) {
	// Create slow server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	tmpDir, _ := os.MkdirTemp("", "input-test")
	defer os.RemoveAll(tmpDir)

	handler := NewInputHandler(30 * time.Second)
	destPath := filepath.Join(tmpDir, "downloaded.txt")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := handler.Download(ctx, server.URL, destPath)
	if err == nil {
		t.Error("Expected error for cancelled context")
	}
}

func TestInputHandler_WriteFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "input-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	handler := NewInputHandler(30 * time.Second)
	destPath := filepath.Join(tmpDir, "subdir", "config.json")
	content := `{"env": "production", "debug": false}`

	err = handler.WriteFile(destPath, content)
	if err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	// Verify file content
	actual, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	if string(actual) != content {
		t.Errorf("Expected content %q, got %q", content, string(actual))
	}
}

func TestInputHandler_WriteFileEmptyContent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "input-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	handler := NewInputHandler(30 * time.Second)
	destPath := filepath.Join(tmpDir, "empty.txt")

	err = handler.WriteFile(destPath, "")
	if err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	info, err := os.Stat(destPath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	if info.Size() != 0 {
		t.Errorf("Expected empty file, got size %d", info.Size())
	}
}
