package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

func testServer(t *testing.T) (*server, http.Handler) {
	t.Helper()
	db, err := sql.Open("duckdb", filepath.Join(t.TempDir(), "test.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	s := &server{db: db, maxUpload: 1 << 20}
	return s, s.routes()
}

func send(t *testing.T, h http.Handler, method, target, body string) (int, map[string]any) {
	t.Helper()
	r := httptest.NewRequest(method, target, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var result map[string]any
	if strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") {
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
	} else if w.Code != 200 {
		result = map[string]any{"error": w.Body.String()}
	}
	return w.Code, result
}

func requireResults(t *testing.T, code int, result map[string]any) []any {
	t.Helper()
	if code != 200 {
		t.Fatalf("status %d: %v", code, result)
	}
	results, ok := result["results"].([]any)
	if !ok {
		t.Fatalf("missing results: %v", result)
	}
	for _, item := range results {
		if err, ok := item.(map[string]any)["error"]; ok {
			t.Fatalf("statement failed: %v", err)
		}
	}
	return results
}

func TestSQLEndpoints(t *testing.T) {
	_, h := testServer(t)
	code, result := send(t, h, "POST", "/db/execute?transaction", `["CREATE TABLE people (id INTEGER, name VARCHAR)", ["INSERT INTO people VALUES (?, ?)", 1, "Ada"], ["INSERT INTO people VALUES (:id, :name)", {"id": 2, "name": "Grace"}]]`)
	if len(requireResults(t, code, result)) != 3 {
		t.Fatal(result)
	}
	code, result = send(t, h, "GET", "/db/query?q=SELECT%20id%2C%20name%20FROM%20people%20ORDER%20BY%20id", "")
	values := requireResults(t, code, result)[0].(map[string]any)["values"].([]any)
	if len(values) != 2 || values[0].([]any)[1] != "Ada" || values[1].([]any)[1] != "Grace" {
		t.Fatal(result)
	}
	types := result["results"].([]any)[0].(map[string]any)["types"].([]any)
	if types[0] != "integer" || types[1] != "text" {
		t.Fatal(types)
	}
	code, result = send(t, h, "POST", "/db/query?associative", `[["SELECT name FROM people WHERE id = ?", 2]]`)
	rows := requireResults(t, code, result)[0].(map[string]any)["rows"].([]any)
	if rows[0].(map[string]any)["name"] != "Grace" {
		t.Fatal(result)
	}
	code, result = send(t, h, "POST", "/db/request?transaction", `[["INSERT INTO people VALUES (?, ?)", 3, "Lin"], "SELECT count(*) FROM people"]`)
	if len(requireResults(t, code, result)) != 2 {
		t.Fatal(result)
	}
	code, result = send(t, h, "POST", "/db/execute?transaction", `["INSERT INTO people VALUES (4, 'rollback')", "INSERT INTO missing VALUES (1)"]`)
	if code != 200 || len(result["results"].([]any)) != 2 || result["results"].([]any)[1].(map[string]any)["error"] == nil {
		t.Fatal(result)
	}
	code, result = send(t, h, "GET", "/db/query?q=SELECT%20count(*)%20FROM%20people", "")
	count := requireResults(t, code, result)[0].(map[string]any)["values"].([]any)[0].([]any)[0]
	if count != float64(3) {
		t.Fatalf("transaction not rolled back: %v", result)
	}
}

func TestImports(t *testing.T) {
	s, h := testServer(t)
	for i, tc := range []struct{ format, body string }{
		{"json", `[{"id":1},{"id":2}]`},
		{"json", "{\"id\":3}\n{\"id\":4}\n"},
		{"csv", "id\n5\n6\n"},
	} {
		mode := "append"
		if i == 0 {
			mode = "create"
		}
		code, result := send(t, h, "POST", "/db/import/events?format="+tc.format+"&mode="+mode, tc.body)
		if code != 200 {
			t.Fatalf("%s: %v", tc.format, result)
		}
	}
	parquet := filepath.Join(t.TempDir(), "input.parquet")
	if _, err := s.db.Exec("COPY (SELECT 7 AS id) TO '" + parquet + "' (FORMAT PARQUET)"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(parquet)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/db/import/events?format=parquet&mode=append", bytes.NewReader(data))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("parquet: %d %s", w.Code, w.Body.String())
	}
	code, result := send(t, h, "GET", "/db/query?q=SELECT%20count(*)%20FROM%20events", "")
	count := requireResults(t, code, result)[0].(map[string]any)["values"].([]any)[0].([]any)[0]
	if count != float64(7) {
		t.Fatal(result)
	}
	code, result = send(t, h, "POST", "/db/import/events?format=ndjson&mode=append", "{\"id\":8}\n")
	if code != 200 {
		t.Fatalf("explicit ndjson: %v", result)
	}
	code, _ = send(t, h, "POST", "/db/import/events?format=csv&mode=replace", "unrelated\nabc\n")
	if code != 200 {
		t.Fatalf("replace failed: %d", code)
	}
	code, _ = send(t, h, "POST", "/db/import/events?format=parquet&mode=replace", "not a parquet file")
	if code == 200 {
		t.Fatal("invalid replacement should fail")
	}
	code, result = send(t, h, "GET", "/db/query?q=SELECT%20unrelated%20FROM%20events", "")
	if len(requireResults(t, code, result)) != 1 {
		t.Fatal(result)
	}
}

func TestAuthAndLimits(t *testing.T) {
	s, _ := testServer(t)
	s.user, s.password = "user", "secret"
	s.maxUpload = 3
	h := s.routes()
	r := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatal(w.Code)
	}
	r = httptest.NewRequest("POST", "/db/import/items?format=json", io.NopCloser(strings.NewReader(`[{"id":1}]`)))
	r.SetBasicAuth("user", "secret")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("unexpected status: %d %s", w.Code, w.Body.String())
	}
	r = httptest.NewRequest("GET", "/readyz", nil)
	r.SetBasicAuth("user", "secret")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
}
