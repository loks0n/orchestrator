package sidecar

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"orchestrator/internal/job"
	"orchestrator/pkg/backoff"
	"os"
	"time"
)

// OutputHandler handles processing job outputs
type OutputHandler struct {
	httpClient *http.Client
	maxRetries int
}

// NewOutputHandler creates a new output handler
func NewOutputHandler(timeout time.Duration, maxRetries int) *OutputHandler {
	if maxRetries < 0 {
		maxRetries = 3
	}
	return &OutputHandler{
		httpClient: &http.Client{Timeout: timeout},
		maxRetries: maxRetries,
	}
}

// OutputResult represents the result of processing an output
type OutputResult struct {
	Output  job.Output
	Status  string
	Content any
	Error   error
}

// Process processes a single output based on its type
func (h *OutputHandler) Process(ctx context.Context, output job.Output) *OutputResult {
	result := &OutputResult{
		Output: output,
		Status: "success",
	}

	switch output.Type {
	case "upload":
		result.Error = h.upload(ctx, output.Path, output.URL)
	case "event":
		content, err := h.readForEvent(output.Path)
		result.Content = content
		result.Error = err
	default:
		result.Error = fmt.Errorf("unknown output type: %s", output.Type)
	}

	if result.Error != nil {
		result.Status = "failed"
	}

	return result
}

// upload uploads a file to the given URL using HTTP PUT with retry.
// Streams from the file to avoid loading entire content into memory.
func (h *OutputHandler) upload(ctx context.Context, filePath, url string) error {
	// Get file size upfront (don't retry file operations)
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("file not found: %w", err)
	}
	size := fileInfo.Size()

	var lastErr error
	for attempt := 0; attempt <= h.maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		if attempt > 0 {
			wait := backoff.Exponential(attempt, nil)
			slog.Debug("Retrying upload", "attempt", attempt, "backoff", wait, "path", filePath)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}

		// Open file fresh for each attempt (HTTP transport closes body after send)
		lastErr = h.uploadFile(ctx, filePath, size, url)
		if lastErr == nil {
			if attempt > 0 {
				slog.Info("Upload succeeded after retry", "attempt", attempt, "path", filePath)
			}
			return nil
		}

		// Don't retry on 4xx errors (client errors) - they won't succeed on retry
		if isClientError(lastErr) {
			return lastErr
		}

		slog.Warn("Upload failed", "attempt", attempt, "error", lastErr, "path", filePath)
	}

	return fmt.Errorf("upload failed after %d retries: %w", h.maxRetries, lastErr)
}

// uploadFile opens the file and streams it to the URL
func (h *OutputHandler) uploadFile(ctx context.Context, filePath string, size int64, url string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	return h.doUpload(ctx, file, size, url)
}

// doUpload performs a single upload attempt
func (h *OutputHandler) doUpload(ctx context.Context, body io.Reader, size int64, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, body)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.ContentLength = size
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		slog.Debug("Uploaded file", "bytes", size)
		return nil
	}

	respBody, _ := io.ReadAll(resp.Body)
	return &uploadError{
		statusCode: resp.StatusCode,
		message:    string(respBody),
	}
}

// uploadError represents an HTTP upload error
type uploadError struct {
	statusCode int
	message    string
}

func (e *uploadError) Error() string {
	return fmt.Sprintf("upload failed with status %d: %s", e.statusCode, e.message)
}

// isClientError checks if the error is a 4xx client error (shouldn't retry)
func isClientError(err error) bool {
	if ue, ok := err.(*uploadError); ok {
		return ue.statusCode >= 400 && ue.statusCode < 500
	}
	return false
}

// readForEvent reads a file and returns its content for inclusion in an event
func (h *OutputHandler) readForEvent(filePath string) (any, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Try to parse as JSON first
	var jsonContent any
	if err := json.Unmarshal(content, &jsonContent); err == nil {
		return jsonContent, nil
	}

	// Return as string if not valid JSON
	return string(content), nil
}
