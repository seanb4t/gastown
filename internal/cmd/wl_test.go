package cmd

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestEscapeSQLString(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"hello", "hello"},
		{"it's a test", "it''s a test"},
		{"no quotes", "no quotes"},
		{"'start", "''start"},
		{"end'", "end''"},
		{"mul''tiple", "mul''''tiple"},
		{"", ""},
	}

	for _, tt := range tests {
		got := escapeSQLString(tt.input)
		if got != tt.expected {
			t.Errorf("escapeSQLString(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestGenerateCompletionID(t *testing.T) {
	id := generateCompletionID("w-abc123", "my-town")

	if !strings.HasPrefix(id, "c-") {
		t.Errorf("completion ID should start with 'c-', got %q", id)
	}

	// Should be c- followed by 16 hex chars (8 bytes)
	if len(id) != 2+16 {
		t.Errorf("completion ID should be 18 chars (c- + 16 hex), got %d: %q", len(id), id)
	}
}

func TestWlGetString(t *testing.T) {
	m := map[string]interface{}{
		"name":   "test",
		"number": float64(42),
		"null":   nil,
	}

	if got := wlGetString(m, "name"); got != "test" {
		t.Errorf("wlGetString(name) = %q, want %q", got, "test")
	}

	if got := wlGetString(m, "number"); got != "42" {
		t.Errorf("wlGetString(number) = %q, want %q", got, "42")
	}

	if got := wlGetString(m, "null"); got != "" {
		t.Errorf("wlGetString(null) = %q, want empty", got)
	}

	if got := wlGetString(m, "missing"); got != "" {
		t.Errorf("wlGetString(missing) = %q, want empty", got)
	}
}

func TestWlQueryResultParsing(t *testing.T) {
	jsonData := `{"rows": [{"id": "w-abc123", "title": "Fix bug", "status": "open", "claimed_by": ""}]}`

	var result wlQueryResult
	if err := json.Unmarshal([]byte(jsonData), &result); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	if got := wlGetString(row, "id"); got != "w-abc123" {
		t.Errorf("id = %q, want %q", got, "w-abc123")
	}
	if got := wlGetString(row, "title"); got != "Fix bug" {
		t.Errorf("title = %q, want %q", got, "Fix bug")
	}
	if got := wlGetString(row, "status"); got != "open" {
		t.Errorf("status = %q, want %q", got, "open")
	}
}

func TestWlCommandRegistered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "wl" {
			found = true
			break
		}
	}
	if !found {
		t.Error("wl command not registered with root command")
	}
}

func TestWlSubcommandsRegistered(t *testing.T) {
	subcommands := map[string]bool{
		"claim": false,
		"done":  false,
	}

	for _, cmd := range wlCmd.Commands() {
		if _, ok := subcommands[cmd.Name()]; ok {
			subcommands[cmd.Name()] = true
		}
	}

	for name, found := range subcommands {
		if !found {
			t.Errorf("subcommand %q not registered with wl command", name)
		}
	}
}

func TestWlDoneEvidenceFlag(t *testing.T) {
	f := wlDoneCmd.Flags().Lookup("evidence")
	if f == nil {
		t.Fatal("evidence flag not found on wl done command")
	}
}

func TestWlClaimArgsRequired(t *testing.T) {
	if wlClaimCmd.Args == nil {
		t.Error("wl claim should require exactly 1 argument")
	}
}

func TestWlDoneArgsRequired(t *testing.T) {
	if wlDoneCmd.Args == nil {
		t.Error("wl done should require exactly 1 argument")
	}
}

func TestWlCommandAlias(t *testing.T) {
	if len(wlCmd.Aliases) == 0 {
		t.Fatal("wl command should have aliases")
	}
	found := false
	for _, alias := range wlCmd.Aliases {
		if alias == "wasteland" {
			found = true
			break
		}
	}
	if !found {
		t.Error("wl command should have 'wasteland' alias")
	}
}

func TestWlQueryResultEmpty(t *testing.T) {
	jsonData := `{"rows": []}`

	var result wlQueryResult
	if err := json.Unmarshal([]byte(jsonData), &result); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(result.Rows))
	}
}
