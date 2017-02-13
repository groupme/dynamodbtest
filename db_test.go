package dynamodbtest

import (
	"testing"
)

func TestAll(t *testing.T) {
	// Log output to aid debugging
	LogOutput = true

	// Start a new test process
	db, err := New()
	if err != nil {
		t.Fatal(err)
	}

	url := db.URL()
	if url != "http://localhost:8000" {
		t.Error("URL is not correct")
	}

	_err := db.Close()
	if _err != nil {
		t.Fatal(_err)
	}
}
