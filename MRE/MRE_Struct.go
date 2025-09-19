package MRE

// Result represents the result of a lookup operation
type Result struct {
	Epoch    uint64  `json:"epoch"`    // epoch used for the placement
	Primary  int16   `json:"primary"`  // shard ID of the primary node
	Replicas []int16 `json:"replicas"` // list of replica shard IDs (length = Replicas-1, may be empty)
	All      []int16 `json:"all"`      // combined list of [primary] + replicas
}

// LookupRequest represents a lookup request message
type LookupRequest struct {
	QueryID    string            `json:"query_id"`
	UserID     string            `json:"user_id"`
	LookupType string            `json:"lookup_type"`
	Fields     map[string]string `json:"fields"`
	Timestamp  string            `json:"timestamp"`
	Result     *Result           `json:"result,omitempty"`
}
