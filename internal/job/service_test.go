package job

import (
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	t.Parallel()
	svc := &Service{}

	tests := []struct {
		name    string
		req     *Request
		wantErr bool
		errMsg  string
	}{
		{
			name:    "empty ID",
			req:     &Request{Image: "alpine"},
			wantErr: true,
			errMsg:  "job ID is required",
		},
		{
			name:    "empty image",
			req:     &Request{ID: "test-job"},
			wantErr: true,
			errMsg:  "image is required",
		},
		{
			name: "valid minimal request",
			req: &Request{
				ID:    "test-job",
				Image: "alpine",
			},
			wantErr: false,
		},
		{
			name: "output without ID",
			req: &Request{
				ID:    "test-job",
				Image: "alpine",
				Outputs: []Output{
					{Type: "upload", Path: "output"},
				},
			},
			wantErr: true,
			errMsg:  "ID is required",
		},
		{
			name: "output without type",
			req: &Request{
				ID:    "test-job",
				Image: "alpine",
				Outputs: []Output{
					{ID: "out1", Path: "output"},
				},
			},
			wantErr: true,
			errMsg:  "type is required",
		},
		{
			name: "upload output without URL",
			req: &Request{
				ID:    "test-job",
				Image: "alpine",
				Outputs: []Output{
					{ID: "out1", Type: "upload", Path: "output"},
				},
			},
			wantErr: true,
			errMsg:  "URL is required for upload type",
		},
		{
			name: "valid request with outputs",
			req: &Request{
				ID:    "test-job",
				Image: "alpine",
				Outputs: []Output{
					{ID: "out1", Type: "upload", Path: "output", URL: "http://example.com/upload"},
					{ID: "out2", Type: "event", Path: "result.json"},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := svc.validate(tt.req)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error containing %q", tt.errMsg)
				} else if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("Expected error containing %q, got %q", tt.errMsg, err.Error())
				}
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestApplyDefaults(t *testing.T) {
	t.Parallel()
	req := &Request{
		ID:    "test-job",
		Image: "alpine",
	}

	applyDefaults(req)

	// Check defaults were set
	if req.TimeoutSeconds != 1800 {
		t.Errorf("Expected default timeout 1800, got %d", req.TimeoutSeconds)
	}
	if req.CPU != 1 {
		t.Errorf("Expected default CPU 1, got %d", req.CPU)
	}
	if req.Memory != 512 {
		t.Errorf("Expected default memory 512, got %d", req.Memory)
	}
}

func TestApplyDefaults_PreservesExisting(t *testing.T) {
	t.Parallel()
	req := &Request{
		ID:             "test-job",
		Image:          "alpine",
		TimeoutSeconds: 3600,
		CPU:            4,
		Memory:         2048,
	}

	applyDefaults(req)

	// Check existing values were preserved
	if req.TimeoutSeconds != 3600 {
		t.Errorf("Expected preserved timeout 3600, got %d", req.TimeoutSeconds)
	}
	if req.CPU != 4 {
		t.Errorf("Expected preserved CPU 4, got %d", req.CPU)
	}
	if req.Memory != 2048 {
		t.Errorf("Expected preserved memory 2048, got %d", req.Memory)
	}
}

func TestValidate_Inputs(t *testing.T) {
	t.Parallel()
	svc := &Service{}

	tests := []struct {
		name    string
		req     *Request
		wantErr bool
		errMsg  string
	}{
		{
			name: "input without ID",
			req: &Request{
				ID:    "test-job",
				Image: "alpine",
				Inputs: []Input{
					{Type: "download", Path: "input.txt", URL: "http://example.com/file"},
				},
			},
			wantErr: true,
			errMsg:  "ID is required",
		},
		{
			name: "input without type",
			req: &Request{
				ID:    "test-job",
				Image: "alpine",
				Inputs: []Input{
					{ID: "in1", Path: "input.txt", URL: "http://example.com/file"},
				},
			},
			wantErr: true,
			errMsg:  "type is required",
		},
		{
			name: "input without path",
			req: &Request{
				ID:    "test-job",
				Image: "alpine",
				Inputs: []Input{
					{ID: "in1", Type: "download", URL: "http://example.com/file"},
				},
			},
			wantErr: true,
			errMsg:  "path is required",
		},
		{
			name: "download input without URL",
			req: &Request{
				ID:    "test-job",
				Image: "alpine",
				Inputs: []Input{
					{ID: "in1", Type: "download", Path: "input.txt"},
				},
			},
			wantErr: true,
			errMsg:  "URL is required for download type",
		},
		{
			name: "file input without content",
			req: &Request{
				ID:    "test-job",
				Image: "alpine",
				Inputs: []Input{
					{ID: "in1", Type: "file", Path: "config.json"},
				},
			},
			wantErr: true,
			errMsg:  "content is required for file type",
		},
		{
			name: "valid download input",
			req: &Request{
				ID:    "test-job",
				Image: "alpine",
				Inputs: []Input{
					{ID: "in1", Type: "download", Path: "input.txt", URL: "http://example.com/file"},
				},
			},
			wantErr: false,
		},
		{
			name: "valid file input",
			req: &Request{
				ID:    "test-job",
				Image: "alpine",
				Inputs: []Input{
					{ID: "in1", Type: "file", Path: "config.json", Content: `{"key": "value"}`},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid input type",
			req: &Request{
				ID:    "test-job",
				Image: "alpine",
				Inputs: []Input{
					{ID: "in1", Type: "invalid", Path: "input.txt"},
				},
			},
			wantErr: true,
			errMsg:  "invalid type",
		},
		{
			name: "invalid output type",
			req: &Request{
				ID:    "test-job",
				Image: "alpine",
				Outputs: []Output{
					{ID: "out1", Type: "invalid", Path: "output.txt"},
				},
			},
			wantErr: true,
			errMsg:  "invalid type",
		},
		{
			name: "multiple inputs mixed valid",
			req: &Request{
				ID:    "test-job",
				Image: "alpine",
				Inputs: []Input{
					{ID: "in1", Type: "download", Path: "data.csv", URL: "http://example.com/data"},
					{ID: "in2", Type: "file", Path: "config.json", Content: `{}`},
				},
			},
			wantErr: false,
		},
		{
			name: "path traversal attack",
			req: &Request{
				ID:    "test-job",
				Image: "alpine",
				Inputs: []Input{
					{ID: "in1", Type: "file", Path: "../../../etc/passwd", Content: "malicious"},
				},
			},
			wantErr: true,
			errMsg:  "path traversal",
		},
		{
			name: "absolute path not allowed",
			req: &Request{
				ID:    "test-job",
				Image: "alpine",
				Inputs: []Input{
					{ID: "in1", Type: "file", Path: "/etc/passwd", Content: "malicious"},
				},
			},
			wantErr: true,
			errMsg:  "must be relative",
		},
		{
			name: "invalid URL scheme",
			req: &Request{
				ID:    "test-job",
				Image: "alpine",
				Inputs: []Input{
					{ID: "in1", Type: "download", Path: "input.txt", URL: "ftp://example.com/file"},
				},
			},
			wantErr: true,
			errMsg:  "scheme must be http or https",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := svc.validate(tt.req)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error containing %q", tt.errMsg)
				} else if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("Expected error containing %q, got %q", tt.errMsg, err.Error())
				}
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}
