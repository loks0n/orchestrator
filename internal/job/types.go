package job

// Request represents a request to create a new job
type Request struct {
	ID             string            `json:"id"`
	Meta           map[string]string `json:"meta"`
	Image          string            `json:"image"`
	Command        string            `json:"command"`
	CPU            int               `json:"cpu"`
	Memory         int               `json:"memory"`
	Environment    map[string]string `json:"environment"`
	TimeoutSeconds int               `json:"timeoutSeconds"`
	Inputs         []Input           `json:"inputs,omitempty"`
	Outputs        []Output          `json:"outputs,omitempty"`
	Callback       *Callback         `json:"callback,omitempty"`
}

// Input represents input configuration for a job
type Input struct {
	ID      string `json:"id"`
	Type    string `json:"type"` // "download" | "file"
	Path    string `json:"path"`
	URL     string `json:"url,omitempty"`     // Required for "download"
	Content string `json:"content,omitempty"` // Required for "file"
}

// Output represents output configuration for a job
type Output struct {
	ID   string `json:"id"`
	Type string `json:"type"` // "upload" | "event"
	Path string `json:"path"`
	URL  string `json:"url,omitempty"` // Required for "upload"
}

// Callback represents callback configuration for a job
type Callback struct {
	URL    string   `json:"url"`
	Events []string `json:"events"`
	Key    string   `json:"key,omitempty"` // HMAC signing key
}

// Response represents the response when a job is created
type Response struct {
	ID     string `json:"id"`
	Status string `json:"status"` // "accepted"
}

// Status represents the current status of a job
type Status struct {
	ID       string `json:"id"`
	State    string `json:"status"`
	ExitCode *int   `json:"exitCode,omitempty"`
	Error    string `json:"error,omitempty"`
}

// ListResponse represents the response for listing jobs
type ListResponse struct {
	Jobs []Status `json:"jobs"`
}

// State constants
const (
	StateAccepted  = "accepted"
	StateRunning   = "running"
	StateCompleted = "completed"
	StateFailed    = "failed"
	StateCancelled = "cancelled"
)
