package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// backupSnapshot creates a new, self-contained DuckDB database file. DuckDB
// performs the copy while it owns the live database; copying the live file
// directly could omit uncheckpointed WAL data or race with a write.
func (s *server) backupSnapshot(ctx context.Context) (string, func(), error) {
	dir, err := os.MkdirTemp("", "dhttp-backup-*")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	path := filepath.Join(dir, "backup.duckdb")
	conn, err := s.db.Conn(ctx)
	if err != nil {
		cleanup()
		return "", nil, err
	}
	defer conn.Close()
	var source string
	if err := conn.QueryRowContext(ctx, "SELECT current_database()").Scan(&source); err != nil {
		cleanup()
		return "", nil, err
	}
	// The alias is derived from an OS-generated temporary directory name.
	alias := "snapshot_" + strings.ReplaceAll(strings.TrimPrefix(filepath.Base(dir), "dhttp-backup-"), "-", "_")
	if _, err := conn.ExecContext(ctx, `ATTACH '`+strings.ReplaceAll(path, `'`, `''`)+`' AS "`+alias+`"`); err != nil {
		cleanup()
		return "", nil, err
	}
	_, copyErr := conn.ExecContext(ctx, `COPY FROM DATABASE "`+strings.ReplaceAll(source, `"`, `""`)+`" TO "`+alias+`"`)
	_, detachErr := conn.ExecContext(context.WithoutCancel(ctx), `DETACH "`+alias+`"`)
	if copyErr != nil || detachErr != nil {
		cleanup()
		return "", nil, errors.Join(copyErr, detachErr)
	}
	return path, cleanup, nil
}

func (s *server) backupHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if format := r.URL.Query().Get("fmt"); format != "" && format != "binary" {
		http.Error(w, "only binary DuckDB backups are supported", http.StatusBadRequest)
		return
	}
	path, cleanup, err := s.backupSnapshot(r.Context())
	if err != nil {
		http.Error(w, "backup failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer cleanup()
	f, err := os.Open(path)
	if err != nil {
		http.Error(w, "backup unavailable", http.StatusInternalServerError)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		http.Error(w, "backup unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", `attachment; filename="backup.duckdb"`)
	w.Header().Set("Content-Length", fmt.Sprint(info.Size()))
	_, _ = io.Copy(w, f)
}

// backupCommand downloads through HTTP, so the CLI does not open the live
// database in another process. A temporary file plus an exclusive hard link
// ensures a failed download never leaves an incomplete destination backup.
func backupCommand(args []string) error {
	flags := flag.NewFlagSet("backup", flag.ContinueOnError)
	serverURL := flags.String("url", "http://127.0.0.1:4001", "running server URL")
	out := flags.String("out", "", "destination .duckdb path (must not exist)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *out == "" || flags.NArg() != 0 {
		return errors.New("usage: dhttp backup -out FILE [-url http://127.0.0.1:4001]")
	}
	url := strings.TrimRight(*serverURL, "/") + "/db/backup"
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		return errors.New("backup URL must use http or https")
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	user, pass := os.Getenv("DUCKDB_HTTP_USER"), os.Getenv("DUCKDB_HTTP_PASSWORD")
	if user != "" || pass != "" {
		if user == "" || pass == "" {
			return errors.New("set both DUCKDB_HTTP_USER and DUCKDB_HTTP_PASSWORD")
		}
		req.SetBasicAuth(user, pass)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("backup request: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	if resp.Header.Get("Content-Type") != "application/octet-stream" {
		return fmt.Errorf("unexpected backup content type: %q", resp.Header.Get("Content-Type"))
	}
	file, err := os.CreateTemp(filepath.Dir(*out), ".dhttp-backup-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	n, copyErr := io.Copy(file, resp.Body)
	if copyErr == nil && resp.ContentLength >= 0 && n != resp.ContentLength {
		copyErr = io.ErrUnexpectedEOF
	}
	if copyErr == nil {
		copyErr = file.Sync()
	}
	closeErr := file.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return err
	}
	if err := os.Link(file.Name(), *out); err != nil {
		return fmt.Errorf("cannot create backup (destination must not exist): %w", err)
	}
	if dir, err := os.Open(filepath.Dir(*out)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	fmt.Printf("saved %d bytes to %s\n", n, *out)
	return nil
}
