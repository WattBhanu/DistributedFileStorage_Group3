package network

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"
)

var client = &http.Client{Timeout: 3 * time.Second}

func Send(addr string, path string, body any) (*http.Response, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return client.Post("http://"+addr+path, "application/json", bytes.NewBuffer(b))
}

func SendAndDecode(addr string, path string, body any, result any) error {
	resp, err := Send(addr, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// TODO (Week 2): add network logic to handle failed or unexpected responses
	return json.NewDecoder(resp.Body).Decode(result)
}
