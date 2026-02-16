package cmd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/doltserver"
	"github.com/steveyegge/gastown/internal/style"
	"github.com/steveyegge/gastown/internal/workspace"
)

const wlCommonsDB = "wl-commons"

var (
	wlDoneEvidence string
)

var wlCmd = &cobra.Command{
	Use:     "wl",
	Aliases: []string{"wasteland"},
	GroupID: GroupWork,
	Short:   "Wasteland federation commands",
	RunE:    requireSubcommand,
	Long: `Wasteland federation commands for the shared wanted board.

The Wasteland is a shared work coordination system backed by the wl-commons
DoltHub database. Towns post wanted items, claim work, and submit completions.

WORK LIFECYCLE:
  gt wl claim <id>    Claim a wanted item
  gt wl done <id>     Submit completion evidence

Examples:
  gt wl claim w-abc123                         # Claim a wanted item
  gt wl done w-abc123 --evidence 'https://...' # Submit completion`,
}

var wlClaimCmd = &cobra.Command{
	Use:   "claim <wanted-id>",
	Short: "Claim a wanted item",
	Long: `Claim a wanted item on the shared wanted board.

Updates the wanted row: claimed_by=<your town handle>, status='claimed'.
The item must exist and have status='open'.

In wild-west mode (Phase 1), this writes directly to the local wl-commons
database. In PR mode, this will create a DoltHub PR instead.

Examples:
  gt wl claim w-abc123`,
	Args: cobra.ExactArgs(1),
	RunE: runWlClaim,
}

var wlDoneCmd = &cobra.Command{
	Use:   "done <wanted-id>",
	Short: "Submit completion evidence for a wanted item",
	Long: `Submit completion evidence for a claimed wanted item.

Inserts a completion record and updates the wanted item status to 'in_review'.
The item must be claimed by your town.

The --evidence flag provides the evidence URL (PR link, commit hash, etc.).

A completion ID is generated as c-<hash> where hash is derived from the
wanted ID, town handle, and timestamp.

Examples:
  gt wl done w-abc123 --evidence 'https://github.com/org/repo/pull/123'
  gt wl done w-abc123 --evidence 'commit abc123def'`,
	Args: cobra.ExactArgs(1),
	RunE: runWlDone,
}

func init() {
	wlDoneCmd.Flags().StringVar(&wlDoneEvidence, "evidence", "", "Evidence URL or description (required)")
	_ = wlDoneCmd.MarkFlagRequired("evidence")

	wlCmd.AddCommand(wlClaimCmd)
	wlCmd.AddCommand(wlDoneCmd)

	rootCmd.AddCommand(wlCmd)
}

// runWlClaim claims a wanted item by updating its status and claimed_by fields.
func runWlClaim(cmd *cobra.Command, args []string) error {
	wantedID := args[0]

	townRoot, err := workspace.FindFromCwdOrError()
	if err != nil {
		return fmt.Errorf("not in a Gas Town workspace: %w", err)
	}

	townName, err := workspace.GetTownName(townRoot)
	if err != nil {
		return fmt.Errorf("getting town handle: %w", err)
	}

	// Verify wl-commons database exists
	if !doltserver.DatabaseExists(townRoot, wlCommonsDB) {
		return fmt.Errorf("database %q not found\nJoin a wasteland first with: gt wl join <org/db>", wlCommonsDB)
	}

	// Verify the wanted item exists and is open
	item, err := queryWantedItem(townRoot, wantedID)
	if err != nil {
		return fmt.Errorf("querying wanted item: %w", err)
	}

	if item.Status != "open" {
		return fmt.Errorf("wanted item %s is not open (status: %s)", wantedID, item.Status)
	}

	// Update: claimed_by + status
	query := fmt.Sprintf(
		"USE `%s`; UPDATE wanted SET claimed_by='%s', status='claimed', updated_at=NOW() WHERE id='%s' AND status='open'",
		wlCommonsDB,
		escapeSQLString(townName),
		escapeSQLString(wantedID),
	)

	if err := execWlSQL(townRoot, query); err != nil {
		return fmt.Errorf("claiming wanted item: %w", err)
	}

	fmt.Printf("%s Claimed %s\n", style.Bold.Render("✓"), wantedID)
	fmt.Printf("  Claimed by: %s\n", townName)
	fmt.Printf("  Title: %s\n", item.Title)

	return nil
}

// runWlDone submits completion evidence for a claimed wanted item.
func runWlDone(cmd *cobra.Command, args []string) error {
	wantedID := args[0]

	townRoot, err := workspace.FindFromCwdOrError()
	if err != nil {
		return fmt.Errorf("not in a Gas Town workspace: %w", err)
	}

	townName, err := workspace.GetTownName(townRoot)
	if err != nil {
		return fmt.Errorf("getting town handle: %w", err)
	}

	// Verify wl-commons database exists
	if !doltserver.DatabaseExists(townRoot, wlCommonsDB) {
		return fmt.Errorf("database %q not found\nJoin a wasteland first with: gt wl join <org/db>", wlCommonsDB)
	}

	// Verify the wanted item exists and is claimed by us
	item, err := queryWantedItem(townRoot, wantedID)
	if err != nil {
		return fmt.Errorf("querying wanted item: %w", err)
	}

	if item.Status != "claimed" {
		return fmt.Errorf("wanted item %s is not claimed (status: %s)", wantedID, item.Status)
	}

	if item.ClaimedBy != townName {
		return fmt.Errorf("wanted item %s is claimed by %q, not %q", wantedID, item.ClaimedBy, townName)
	}

	// Generate completion ID: c-<hash>
	completionID := generateCompletionID(wantedID, townName)

	// Insert completion + update wanted status in a single transaction
	query := fmt.Sprintf(
		"USE `%s`; "+
			"INSERT INTO completions (id, wanted_id, completed_by, evidence, completed_at) "+
			"VALUES ('%s', '%s', '%s', '%s', NOW()); "+
			"UPDATE wanted SET status='in_review', evidence_url='%s', updated_at=NOW() WHERE id='%s'",
		wlCommonsDB,
		escapeSQLString(completionID),
		escapeSQLString(wantedID),
		escapeSQLString(townName),
		escapeSQLString(wlDoneEvidence),
		escapeSQLString(wlDoneEvidence),
		escapeSQLString(wantedID),
	)

	if err := execWlSQL(townRoot, query); err != nil {
		return fmt.Errorf("submitting completion: %w", err)
	}

	fmt.Printf("%s Completion submitted for %s\n", style.Bold.Render("✓"), wantedID)
	fmt.Printf("  Completion ID: %s\n", completionID)
	fmt.Printf("  Completed by: %s\n", townName)
	fmt.Printf("  Evidence: %s\n", wlDoneEvidence)
	fmt.Printf("  Status: in_review\n")

	return nil
}

// wantedItem represents a row from the wanted table.
type wantedItem struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	Status    string `json:"status"`
	ClaimedBy string `json:"claimed_by"`
}

// queryWantedItem fetches a wanted item by ID from wl-commons.
func queryWantedItem(townRoot, wantedID string) (*wantedItem, error) {
	config := doltserver.DefaultConfig(townRoot)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := fmt.Sprintf(
		"USE `%s`; SELECT id, title, status, COALESCE(claimed_by, '') as claimed_by FROM wanted WHERE id='%s'",
		wlCommonsDB,
		escapeSQLString(wantedID),
	)

	output, err := runWlDoltSQL(ctx, config, "-r", "json", "-q", query)
	if err != nil {
		return nil, err
	}

	// Parse JSON result from dolt sql -r json
	var result wlQueryResult
	if err := json.Unmarshal(output, &result); err != nil {
		return nil, fmt.Errorf("parsing query result: %w (output: %s)", err, string(output))
	}

	if len(result.Rows) == 0 {
		return nil, fmt.Errorf("wanted item %q not found", wantedID)
	}

	row := result.Rows[0]
	return &wantedItem{
		ID:        wlGetString(row, "id"),
		Title:     wlGetString(row, "title"),
		Status:    wlGetString(row, "status"),
		ClaimedBy: wlGetString(row, "claimed_by"),
	}, nil
}

// wlQueryResult represents the JSON output from dolt sql -r json.
type wlQueryResult struct {
	Rows []map[string]interface{} `json:"rows"`
}

// wlGetString safely extracts a string from a map[string]interface{}.
func wlGetString(m map[string]interface{}, key string) string {
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return fmt.Sprintf("%v", v)
	}
	return s
}

// buildWlDoltCmd constructs a dolt command that works for both local and remote servers.
// Mirrors the logic in doltserver.buildDoltSQLCmd.
func buildWlDoltCmd(ctx context.Context, config *doltserver.Config, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "dolt", args...)

	if !config.IsRemote() {
		cmd.Dir = config.DataDir
	}

	if config.IsRemote() && config.Password != "" {
		cmd.Env = append(os.Environ(), "DOLT_CLI_PASSWORD="+config.Password)
	}

	return cmd
}

// runWlDoltSQL executes a dolt sql command and returns its stdout.
func runWlDoltSQL(ctx context.Context, config *doltserver.Config, args ...string) ([]byte, error) {
	sqlArgs := config.SQLArgs()
	fullArgs := make([]string, 0, len(sqlArgs)+1+len(args))
	fullArgs = append(fullArgs, "sql")
	fullArgs = append(fullArgs, sqlArgs...)
	fullArgs = append(fullArgs, args...)

	cmd := buildWlDoltCmd(ctx, config, fullArgs...)

	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("dolt sql failed: %w (stderr: %s)", err, strings.TrimSpace(stderrBuf.String()))
	}
	return output, nil
}

// execWlSQL executes a SQL statement (no result expected) via dolt sql CLI.
func execWlSQL(townRoot, query string) error {
	config := doltserver.DefaultConfig(townRoot)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sqlArgs := config.SQLArgs()
	fullArgs := make([]string, 0, len(sqlArgs)+3)
	fullArgs = append(fullArgs, "sql")
	fullArgs = append(fullArgs, sqlArgs...)
	fullArgs = append(fullArgs, "-q", query)

	cmd := buildWlDoltCmd(ctx, config, fullArgs...)

	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf
	if out, err := cmd.Output(); err != nil {
		return fmt.Errorf("dolt sql failed: %w (stderr: %s, stdout: %s)", err, strings.TrimSpace(stderrBuf.String()), strings.TrimSpace(string(out)))
	}
	return nil
}

// generateCompletionID creates a deterministic completion ID from components.
func generateCompletionID(wantedID, townHandle string) string {
	now := time.Now().UTC().Format(time.RFC3339)
	h := sha256.Sum256([]byte(wantedID + "|" + townHandle + "|" + now))
	return fmt.Sprintf("c-%x", h[:8])
}

// escapeSQLString escapes single quotes in SQL string values.
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
