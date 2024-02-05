package squirrel

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// NoContextSupport is returned if a db doesn't support Context.
var NoContextSupport = errors.New("DB does not support Context")

// ExecerContext is the interface that wraps the ExecContext method.
//
// Exec executes the given query as implemented by database/sql.ExecContext.
type ExecerContext interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// QueryerContext is the interface that wraps the QueryContext method.
//
// QueryContext executes the given query as implemented by database/sql.QueryContext.
type QueryerContext interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error)
}

// QueryRowerContext is the interface that wraps the QueryRowContext method.
//
// QueryRowContext executes the given query as implemented by database/sql.QueryRowContext.
type QueryRowerContext interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) RowScanner
}

// RunnerContext groups the Runner interface, along with the Context versions of each of
// its methods
type RunnerContext interface {
	QueryerContext
	QueryRowerContext
	ExecerContext
}

// WrapStdSqlCtx wraps a type implementing the standard SQL interface plus the context
// versions of the methods with methods that squirrel expects.
func WrapStdSqlCtx(stdSqlCtx StdSqlCtx) RunnerContext {
	return &stdsqlCtxRunner{stdSqlCtx}
}

// StdSqlCtx encompasses the standard methods of the *sql.DB type, along with the Context
// versions of those methods, and other types that wrap these methods.
type StdSqlCtx interface {
	StdSql
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

type stdsqlCtxRunner struct {
	StdSqlCtx
}

func (r *stdsqlCtxRunner) QueryRow(query string, args ...interface{}) RowScanner {
	return r.StdSqlCtx.QueryRow(query, args...)
}

func (r *stdsqlCtxRunner) QueryRowContext(ctx context.Context, query string, args ...interface{}) RowScanner {
	return r.StdSqlCtx.QueryRowContext(ctx, query, args...)
}

func (r *stdsqlCtxRunner) QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	rows, err := r.StdSqlCtx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return StdRowsWrapper{rows}, nil
}

// ExecContextWith ExecContexts the SQL returned by s with db.
func ExecContextWith(ctx context.Context, db ExecerContext, s Sqlizer) (res sql.Result, err error) {
	query, args, err := s.ToSql()
	if err != nil {
		return
	}
	return db.ExecContext(ctx, query, args...)
}

// QueryContextWith QueryContexts the SQL returned by s with db.
func QueryContextWith(ctx context.Context, db QueryerContext, s Sqlizer) (rows Rows, err error) {
	query, args, err := s.ToSql()
	if err != nil {
		return
	}
	return db.QueryContext(ctx, query, args...)
}

// QueryRowContextWith QueryRowContexts the SQL returned by s with db.
func QueryRowContextWith(ctx context.Context, db QueryRowerContext, s Sqlizer) RowScanner {
	query, args, err := s.ToSql()
	return &Row{RowScanner: db.QueryRowContext(ctx, query, args...), err: err}
}

type Pgx interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func WrapPgx(p Pgx) RunnerContext {
	return &pgxRunner{p}
}

type pgxRunner struct {
	Pgx
}

func (r *pgxRunner) QueryRowContext(ctx context.Context, query string, args ...interface{}) RowScanner {
	return r.Pgx.QueryRow(ctx, query, args...)
}

func (r *pgxRunner) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	ct, err := r.Pgx.Exec(ctx, query, args...)
	return commandTagWrapper{ct}, err
}

func (r *pgxRunner) QueryRow(query string, args ...interface{}) RowScanner {
	return r.Pgx.QueryRow(context.Background(), query, args...)
}

func (r *pgxRunner) QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	rows, err := r.Pgx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return PgxRowsWrapper{rows}, nil
}

type commandTagWrapper struct {
	pgconn.CommandTag
}

func (ct commandTagWrapper) RowsAffected() (int64, error) {
	return ct.CommandTag.RowsAffected(), nil
}

func (ct commandTagWrapper) LastInsertId() (int64, error) {
	return 0, errors.New("LastInsertId is not supported by pgx")
}

type Rows interface {
	Close()
	Err() error
	Next() bool
	Scan(dest ...any) error
	Columns() ([]string, error)
}

type StdRowsWrapper struct {
	*sql.Rows
}

func (r StdRowsWrapper) Close() {
	_ = r.Rows.Close()
}

func (r StdRowsWrapper) Err() error {
	return r.Rows.Err()
}

func (r StdRowsWrapper) Next() bool {
	return r.Rows.Next()
}

func (r StdRowsWrapper) Scan(dest ...any) error {
	return r.Rows.Scan(dest...)
}

func (r StdRowsWrapper) Columns() ([]string, error) {
	return r.Rows.Columns()
}

type PgxRowsWrapper struct {
	pgx.Rows
}

func (r PgxRowsWrapper) Close() {
	r.Rows.Close()
}

func (r PgxRowsWrapper) Err() error {
	return r.Rows.Err()
}

func (r PgxRowsWrapper) Next() bool {
	return r.Rows.Next()
}

func (r PgxRowsWrapper) Scan(dest ...any) error {
	return r.Rows.Scan(dest...)
}

func (r PgxRowsWrapper) Columns() ([]string, error) {
	fields := r.Rows.FieldDescriptions()

	if fields == nil {
		return nil, errors.New("no field descriptions")
	}

	cols := make([]string, len(fields))
	for i, f := range fields {
		cols[i] = string(f.Name)
	}

	return cols, nil
}
