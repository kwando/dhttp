package main

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

type server struct {
	db        *sql.DB
	maxUpload int64
	user      string
	password  string
}

type statement struct {
	sql  string
	args []any
}

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z_0-9]*$`)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "backup" {
		if err := backupCommand(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
		return
	}
	addr := flag.String("listen", "127.0.0.1:4001", "HTTP listen address")
	path := flag.String("db", "data.duckdb", "DuckDB database path")
	maxMB := flag.Int64("max-upload-mb", 512, "maximum import size in MiB")
	flag.Parse()
	if *maxMB <= 0 || *maxMB > (1<<63-1)/(1024*1024) {
		log.Fatal("max-upload-mb must be positive")
	}
	db, err := sql.Open("duckdb", *path)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	// Exactly one process owns the file; a single pooled connection also makes
	// multi-statement requests and imports easy to reason about.
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}
	s := &server{db: db, maxUpload: *maxMB * 1024 * 1024, user: os.Getenv("DUCKDB_HTTP_USER"), password: os.Getenv("DUCKDB_HTTP_PASSWORD")}
	if (s.user == "") != (s.password == "") {
		log.Fatal("set both DUCKDB_HTTP_USER and DUCKDB_HTTP_PASSWORD")
	}
	log.Printf("listening on %s (database %s)", *addr, *path)
	log.Fatal((&http.Server{Addr: *addr, Handler: s.routes(), ReadHeaderTimeout: 10 * time.Second}).ListenAndServe())
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := s.db.PingContext(r.Context()); err != nil {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/db/query", s.sqlHandler("query"))
	mux.HandleFunc("/db/execute", s.sqlHandler("execute"))
	mux.HandleFunc("/db/request", s.sqlHandler("request"))
	mux.HandleFunc("/db/backup", s.backupHandler)
	mux.HandleFunc("/db/import/", s.importHandler)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.user != "" {
			user, pass, ok := r.BasicAuth()
			if !ok || subtle.ConstantTimeCompare([]byte(user), []byte(s.user)) != 1 || subtle.ConstantTimeCompare([]byte(pass), []byte(s.password)) != 1 {
				w.Header().Set("WWW-Authenticate", `Basic realm="duckdb-http"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *server) sqlHandler(endpoint string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && !(endpoint == "query" && r.Method == http.MethodGet) {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var statements []statement
		var err error
		if r.Method == http.MethodGet {
			if q := r.URL.Query().Get("q"); q != "" {
				statements = []statement{{sql: q}}
			} else {
				err = errors.New("missing q")
			}
		} else {
			r.Body = http.MaxBytesReader(w, r.Body, 8<<20)
			statements, err = parseStatements(r.Body, r.Header.Get("Content-Type"))
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		format := strings.ToLower(r.URL.Query().Get("format"))
		if format != "" && format != "json" {
			if endpoint != "query" || (format != "csv" && format != "ndjson" && format != "parquet") {
				http.Error(w, "format must be json, csv, ndjson, or parquet on /db/query", http.StatusBadRequest)
				return
			}
			if len(statements) != 1 || !isRead(statements[0].sql) || r.URL.Query().Has("transaction") || r.URL.Query().Has("associative") || r.URL.Query().Has("pretty") || r.URL.Query().Has("timings") {
				http.Error(w, "non-JSON output requires one read query and no JSON or transaction options", http.StatusBadRequest)
				return
			}
			s.formattedQuery(w, r, statements[0], format)
			return
		}
		var exec runner = s.db
		var tx *sql.Tx
		if _, ok := r.URL.Query()["transaction"]; ok {
			tx, err = s.db.BeginTx(r.Context(), nil)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			defer tx.Rollback()
			exec = tx
		}
		results := make([]map[string]any, 0, len(statements))
		for _, st := range statements {
			read := isRead(st.sql)
			if endpoint == "query" && !read {
				results = append(results, map[string]any{"error": "not a read query"})
				if tx != nil {
					break
				}
				continue
			}
			if endpoint == "execute" && read {
				results = append(results, map[string]any{"error": "not a write statement"})
				if tx != nil {
					break
				}
				continue
			}
			start := time.Now()
			var result map[string]any
			if read {
				result, err = runQuery(r.Context(), exec, st, r.URL.Query().Has("associative"))
			} else {
				result, err = runExec(r.Context(), exec, st)
			}
			if err != nil {
				result = map[string]any{"error": err.Error()}
			}
			if r.URL.Query().Has("timings") {
				result["time"] = time.Since(start).Seconds()
			}
			results = append(results, result)
			if err != nil && tx != nil {
				break
			}
		}
		if tx != nil {
			failed := len(results) != len(statements)
			for _, result := range results {
				if _, ok := result["error"]; ok {
					failed = true
					break
				}
			}
			if !failed {
				if err := tx.Commit(); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"results": results}, r.URL.Query().Has("pretty"))
	}
}

type runner interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func runQuery(ctx context.Context, db runner, st statement, associative bool) (map[string]any, error) {
	rows, err := db.QueryContext(ctx, st.sql, st.args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	types := make([]string, len(columns))
	for i, t := range columnTypes {
		types[i] = compatibleType(t.DatabaseTypeName())
	}
	values := make([]any, 0)
	for rows.Next() {
		cells := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		for i, cell := range cells {
			cells[i] = jsonValue(cell)
		}
		if associative {
			row := make(map[string]any, len(columns))
			for i, col := range columns {
				row[col] = cells[i]
			}
			values = append(values, row)
		} else {
			values = append(values, cells)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if associative {
		byName := make(map[string]string, len(columns))
		for i, col := range columns {
			byName[col] = types[i]
		}
		return map[string]any{"types": byName, "rows": values}, nil
	}
	return map[string]any{"columns": columns, "types": types, "values": values}, nil
}

// Expose SQLite-like type names for common scalar types. These describe the
// result, not SQLite's declared column types (which DuckDB cannot provide).
func compatibleType(t string) string {
	t = strings.ToUpper(t)
	switch {
	case strings.Contains(t, "INT"):
		return "integer"
	case t == "VARCHAR" || t == "TEXT" || t == "UUID" || t == "DATE" || strings.HasPrefix(t, "TIMESTAMP") || t == "TIME":
		return "text"
	case t == "FLOAT" || t == "DOUBLE" || t == "REAL":
		return "real"
	case strings.HasPrefix(t, "DECIMAL"):
		return "numeric"
	case t == "BLOB":
		return "blob"
	default:
		return strings.ToLower(t)
	}
}

func jsonValue(v any) any {
	switch v := v.(type) {
	case []byte:
		return base64.StdEncoding.EncodeToString(v)
	case time.Time:
		return v.Format(time.RFC3339Nano)
	default:
		return v
	}
}

func runExec(ctx context.Context, db runner, st statement) (map[string]any, error) {
	res, err := db.ExecContext(ctx, st.sql, st.args...)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if n, err := res.RowsAffected(); err == nil && n >= 0 {
		out["rows_affected"] = n
	}
	return out, nil
}

func isRead(q string) bool {
	q = strings.TrimSpace(q)
	fields := strings.Fields(q)
	if len(fields) == 0 {
		return false
	}
	switch strings.ToUpper(fields[0]) {
	case "SELECT", "WITH", "VALUES", "SHOW", "DESCRIBE", "EXPLAIN", "SUMMARIZE":
		return true
	default:
		return false
	}
}

func parseStatements(body io.Reader, contentType string) ([]statement, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(strings.ToLower(contentType), "text/plain") {
		if strings.TrimSpace(string(data)) == "" {
			return nil, errors.New("empty statement")
		}
		return []statement{{sql: string(data)}}, nil
	}
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("expected JSON statement array: %w", err)
	}
	if len(raw) == 0 {
		return nil, errors.New("empty statement array")
	}
	out := make([]statement, 0, len(raw))
	for _, item := range raw {
		var st statement
		if len(item) > 0 && item[0] == '"' {
			if err := json.Unmarshal(item, &st.sql); err != nil {
				return nil, err
			}
		} else {
			var arr []json.RawMessage
			if err := json.Unmarshal(item, &arr); err != nil || len(arr) == 0 {
				return nil, errors.New("statement must be a SQL string or nonempty array")
			}
			if err := json.Unmarshal(arr[0], &st.sql); err != nil {
				return nil, errors.New("first array item must be SQL")
			}
			for _, param := range arr[1:] {
				v, err := parseParam(param)
				if err != nil {
					return nil, err
				}
				if named, ok := v.(map[string]any); ok {
					st.sql = duckDBNamedParams(st.sql)
					for key, val := range named {
						st.args = append(st.args, sql.Named(key, val))
					}
				} else {
					st.args = append(st.args, v)
				}
			}
		}
		if strings.TrimSpace(st.sql) == "" {
			return nil, errors.New("empty SQL statement")
		}
		out = append(out, st)
	}
	return out, nil
}

// DuckDB uses $name while rqlite/SQLite clients commonly use :name.
// Skip SQL strings, quoted identifiers and comments when translating.
func duckDBNamedParams(q string) string {
	var b strings.Builder
	state := byte(0)
	for i := 0; i < len(q); i++ {
		c := q[i]
		if state == '\'' || state == '"' {
			b.WriteByte(c)
			if c == state {
				if i+1 < len(q) && q[i+1] == c {
					b.WriteByte(q[i+1])
					i++
				} else {
					state = 0
				}
			}
			continue
		}
		if state == '-' {
			b.WriteByte(c)
			if c == '\n' {
				state = 0
			}
			continue
		}
		if state == '*' {
			b.WriteByte(c)
			if c == '*' && i+1 < len(q) && q[i+1] == '/' {
				i++
				b.WriteByte('/')
				state = 0
			}
			continue
		}
		if c == '\'' || c == '"' {
			state = c
			b.WriteByte(c)
			continue
		}
		if c == '-' && i+1 < len(q) && q[i+1] == '-' {
			b.WriteString("--")
			i++
			state = '-'
			continue
		}
		if c == '/' && i+1 < len(q) && q[i+1] == '*' {
			b.WriteString("/*")
			i++
			state = '*'
			continue
		}
		if c == ':' && i+1 < len(q) && (q[i+1] == '_' || q[i+1] >= 'A' && q[i+1] <= 'Z' || q[i+1] >= 'a' && q[i+1] <= 'z') && (i == 0 || q[i-1] != ':') {
			b.WriteByte('$')
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

func parseParam(raw json.RawMessage) (any, error) {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return convertParam(v)
}

func convertParam(v any) (any, error) {
	switch v := v.(type) {
	case json.Number:
		if i, err := strconv.ParseInt(string(v), 10, 64); err == nil {
			return i, nil
		}
		return strconv.ParseFloat(string(v), 64)
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			value, err := convertParam(item)
			if err != nil {
				return nil, err
			}
			out[key] = value
		}
		return out, nil
	case []any:
		// Like rqlite, a byte array parameter represents a BLOB.
		bytes := make([]byte, len(v))
		for i, item := range v {
			n, ok := item.(json.Number)
			if !ok {
				return nil, errors.New("array parameter must contain bytes")
			}
			x, err := strconv.ParseUint(string(n), 10, 8)
			if err != nil {
				return nil, errors.New("array parameter must contain bytes")
			}
			bytes[i] = byte(x)
		}
		return bytes, nil
	default:
		return v, nil
	}
}

func (s *server) importHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	table := strings.TrimPrefix(r.URL.Path, "/db/import/")
	if !identifier.MatchString(table) {
		http.Error(w, "table must be a simple SQL identifier", http.StatusBadRequest)
		return
	}
	format := strings.ToLower(r.URL.Query().Get("format"))
	var reader string
	switch format {
	case "parquet":
		reader = "read_parquet(?)"
	case "csv":
		reader = "read_csv_auto(?)"
	case "json":
		reader = "read_json_auto(?)"
	case "ndjson":
		reader = "read_json(?, format='newline_delimited')"
	default:
		http.Error(w, "format must be parquet, csv, json, or ndjson", http.StatusBadRequest)
		return
	}
	mode := r.URL.Query().Get("mode")
	if mode == "" {
		mode = "create"
	}
	if mode != "create" && mode != "append" && mode != "replace" {
		http.Error(w, "mode must be create, append, or replace", http.StatusBadRequest)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.maxUpload)
	f, err := os.CreateTemp("", "duckdb-http-import-*")
	if err != nil {
		http.Error(w, "cannot create upload file", http.StatusInternalServerError)
		return
	}
	defer os.Remove(f.Name())
	n, err := io.Copy(f, r.Body)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
		return
	}
	if n == 0 {
		http.Error(w, "empty upload", http.StatusBadRequest)
		return
	}
	tx, err := s.db.BeginTx(r.Context(), nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()
	name := `"` + table + `"`
	if mode == "replace" {
		if _, err := tx.ExecContext(r.Context(), "DROP TABLE IF EXISTS "+name); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	query := "CREATE TABLE " + name + " AS SELECT * FROM " + reader
	if mode == "append" {
		query = "INSERT INTO " + name + " BY NAME SELECT * FROM " + reader
	}
	result, err := tx.ExecContext(r.Context(), query, f.Name())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := tx.Commit(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	response := map[string]any{"table": table, "mode": mode, "format": format, "bytes": n}
	if rows, err := result.RowsAffected(); err == nil && rows >= 0 {
		response["rows_affected"] = rows
	}
	writeJSON(w, http.StatusOK, response, false)
}

func writeJSON(w http.ResponseWriter, status int, value any, pretty bool) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	if pretty {
		enc.SetIndent("", "  ")
	}
	_ = enc.Encode(value)
}
