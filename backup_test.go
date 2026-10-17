package main

import (
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestBackupSnapshotAndCommand(t *testing.T) {
	s, handler := testServer(t)
	if _, err := s.db.Exec("CREATE TABLE items (id INTEGER); INSERT INTO items VALUES (11), (22)"); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(handler)
	defer httpServer.Close()

	resp, err := http.Get(httpServer.URL + "/db/backup?fmt=binary")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("backup: %d: %s", resp.StatusCode, data)
	}
	if resp.Header.Get("Content-Type") != "application/octet-stream" {
		t.Fatal(resp.Header)
	}
	resp.Body.Close()

	path := filepath.Join(t.TempDir(), "snapshot.duckdb")
	if err := backupCommand([]string{"-url", httpServer.URL, "-out", path}); err != nil {
		t.Fatal(err)
	}
	backup, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
	var count, sum int
	if err := backup.QueryRow("SELECT count(*), sum(id) FROM items").Scan(&count, &sum); err != nil {
		t.Fatal(err)
	}
	if count != 2 || sum != 33 {
		t.Fatalf("unexpected backup contents: %d, %d", count, sum)
	}
	if _, err := s.db.Exec("INSERT INTO items VALUES (44)"); err != nil {
		t.Fatal(err)
	}
	if err := backup.QueryRow("SELECT count(*) FROM items").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("backup changed after live write: %d", count)
	}
	if err := backupCommand([]string{"-url", httpServer.URL, "-out", path}); err == nil {
		t.Fatal("backup overwrote existing file")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

func TestBackupErrorsAndAuth(t *testing.T) {
	s, _ := testServer(t)
	s.user, s.password = "user", "secret"
	h := s.routes()
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{"GET", "/db/backup", 401},
		{"POST", "/db/backup", 401},
	} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%s: %d", tc.path, w.Code)
		}
	}
	r := httptest.NewRequest("GET", "/db/backup?fmt=sql", nil)
	r.SetBasicAuth("user", "secret")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
}
