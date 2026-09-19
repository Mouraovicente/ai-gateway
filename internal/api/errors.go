package api

import (
	"encoding/json"
	"net/http"
)

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Type      string `json:"type"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

// WriteError writes the gateway's standard error envelope, always including
// the request id so a caller can correlate with trace_events.
func WriteError(w http.ResponseWriter, requestID string, status int, errType, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(errorBody{Error: errorDetail{Type: errType, Message: message, RequestID: requestID}})
}
