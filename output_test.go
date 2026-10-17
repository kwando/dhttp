package main

import (
	"encoding/csv"
	"encoding/json"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestQueryFormats(t *testing.T) {
	s, h := testServer(t)
	if _, err := s.db.Exec(`CREATE TABLE entries (id INTEGER, name VARCHAR); INSERT INTO entries VALUES (1, 'Ada'), (2, 'Grace')`); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		format, media string
	}{
		{"csv", "text/csv; charset=utf-8"},
		{"ndjson", "application/x-ndjson"},
		{"parquet", "application/vnd.apache.parquet"},
	} {
		t.Run(tc.format, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/db/query?format="+tc.format, strings.NewReader(`[["SELECT id, name FROM entries WHERE id >= ? ORDER BY id", 1]]`))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 200 {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			if got := w.Header().Get("Content-Type"); got != tc.media {
				t.Fatalf("content type %q", got)
			}
			if tc.format == "parquet" {
				data := w.Body.Bytes()
				if len(data) < 8 || string(data[:4]) != "PAR1" || string(data[len(data)-4:]) != "PAR1" {
					t.Fatal("invalid Parquet framing")
				}
				// Read the actual response back through DuckDB to check schema and rows.
				path := t.TempDir() + "/response.parquet"
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
				var count int
				if err := s.db.QueryRow("SELECT count(*) FROM read_parquet(?)", path).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 2 {
					t.Fatalf("parquet has %d rows", count)
				}
			} else if tc.format == "csv" {
				rows, err := csv.NewReader(w.Body).ReadAll()
				if err != nil {
					t.Fatal(err)
				}
				if len(rows) != 3 || rows[0][1] != "name" || rows[2][1] != "Grace" {
					t.Fatal(rows)
				}
			} else {
				lines := strings.Split(strings.TrimSpace(w.Body.String()), "\n")
				if len(lines) != 2 {
					t.Fatal(lines)
				}
				var row map[string]any
				if err := json.Unmarshal([]byte(lines[1]), &row); err != nil {
					t.Fatal(err)
				}
				if row["name"] != "Grace" {
					t.Fatal(row)
				}
			}
		})
	}
	r := httptest.NewRequest("GET", "/db/query?format=csv&q=SELECT%201%20AS%20n%3B", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "n\n1") {
		t.Fatalf("GET export: %d %q", w.Code, w.Body.String())
	}
	r = httptest.NewRequest("POST", "/db/query?format=csv", strings.NewReader(`[["WITH items AS (SELECT id FROM entries) SELECT id FROM items WHERE id = :id", {"id": 2}]]`))
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "id\n2") {
		t.Fatalf("named parameter export: %d %q", w.Code, w.Body.String())
	}
	code, result := send(t, h, "GET", "/db/query?q=SELECT%201%20AS%20n&format=json", "")
	if len(requireResults(t, code, result)) != 1 {
		t.Fatal(result)
	}
}

func TestFormatValidation(t *testing.T) {
	s, h := testServer(t)
	for _, tc := range []struct{ path, body string }{
		{"/db/query?format=csv", `["SELECT 1", "SELECT 2"]`},
		{"/db/query?format=parquet", `["CREATE TABLE should_not_exist AS SELECT 1"]`},
		{"/db/query?format=ndjson&transaction", `["SELECT 1"]`},
		{"/db/request?format=csv", `["SELECT 1"]`},
		{"/db/execute?format=csv", `["SELECT 1"]`},
		{"/db/query?format=xml", `["SELECT 1"]`},
		{"/db/query?format=csv", `["SELECT * FROM absent_table"]`},
	} {
		r := httptest.NewRequest("POST", tc.path, strings.NewReader(tc.body))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 400 {
			t.Errorf("%s: %d %s", tc.path, w.Code, w.Body.String())
		}
	}
	var exists bool
	if err := s.db.QueryRow("SELECT count(*) > 0 FROM information_schema.tables WHERE table_name = 'should_not_exist'").Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("write query executed")
	}
}
