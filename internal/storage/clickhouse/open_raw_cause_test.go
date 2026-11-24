package clickhouse

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// exceptionServer answers the native handshake with an exception whose message
// is message. Every value is made up.
func exceptionServer(t *testing.T, message string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	text := func(value string) []byte { return append(binary.AppendUvarint(nil, uint64(len(value))), value...) }
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = conn.Read(make([]byte, 4096))
				packet := binary.LittleEndian.AppendUint32([]byte{2}, 516)
				packet = append(packet, text("DB::Exception")...)
				packet = append(packet, text(message)...)
				packet = append(packet, text("")...)
				packet = append(packet, 0)
				_, _ = conn.Write(packet)
			}()
		}
	}()
	return listener.Addr().String()
}

// OpenRawCause keeps the error class and the driver's cause text as it came;
// Open still redacts the DSN's credentials from the same text.
func TestOpenRawCauseKeepsTheErrorClassAndTheCauseText(t *testing.T) {
	host := exceptionServer(t, "refused for zz9q and svc%3A7kZ2")
	config := DefaultConfig(fmt.Sprintf("clickhouse://svc%%3A:zz9q@%s/d", host))
	_, err := OpenRawCause(context.Background(), config)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("errors.Is(err, ErrUnavailable) is false: %v", err)
	}
	if !strings.HasPrefix(err.Error(), ErrUnavailable.Error()+": ") || !strings.Contains(err.Error(), "refused for zz9q and svc%3A7kZ2") {
		t.Errorf("the raw cause text is not in the error: %q", err)
	}
	var raw RawCauseError
	if !errors.As(err, &raw) || !slices.Equal(raw.Resolved(), []string{"svc:", "zz9q"}) {
		t.Errorf("the login and password the driver resolved are not carried: %v", raw.Resolved())
	}
	_, err = Open(context.Background(), config)
	if !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "zz9q") {
		t.Errorf("Open must keep redacting the DSN password: %v", err)
	}
}

// OpenRawCause returns an unredacted cause: only the two dho sync CLI openers
// may name it, and they hand the error to a print path that redacts it.
func TestOpenRawCauseHasExactlyTheTwoSyncCLICallers(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "..", "..", "..")
	fset := token.NewFileSet()
	var found []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && (entry.Name() == ".git" || entry.Name() == "node_modules") {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil || !strings.Contains(string(source), "OpenRawCause") {
			return err
		}
		parsed, err := parser.ParseFile(fset, path, source, 0)
		if err != nil {
			return nil
		}
		relative, _ := filepath.Rel(root, path)
		ast.Inspect(parsed, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.FuncDecl:
				if typed.Name.Name == "OpenRawCause" {
					return false
				}
			case *ast.SelectorExpr:
				if typed.Sel.Name == "OpenRawCause" {
					found = append(found, filepath.ToSlash(relative))
					return false
				}
			case *ast.Ident:
				if typed.Name == "OpenRawCause" {
					found = append(found, filepath.ToSlash(relative))
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	slices.Sort(found)
	if want := []string{"internal/synccli/inline.go", "internal/synccli/synccli.go"}; !slices.Equal(found, want) {
		t.Errorf("OpenRawCause is named in %v, want exactly %v", found, want)
	}
}
