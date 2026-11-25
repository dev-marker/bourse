// Package config reads a program's settings from its environment, the same way in every service: a
// default that works on a laptop, a clear error for a value that's wrong, and -h to list them all.
package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"text/tabwriter"
	"time"
)

// Env reads settings from environment variables, and remembers each one for Usage.
type Env struct {
	lookup func(string) (string, bool)
	vars   []variable
	errs   []error
}

// variable is one setting, the way Usage lists it.
type variable struct{ name, def, help string }

// New reads settings with lookup, which is os.LookupEnv outside of tests.
func New(lookup func(string) (string, bool)) *Env {
	return &Env{lookup: lookup}
}

// String is the variable's value, or def when it isn't set.
func (e *Env) String(name, def, help string) string {
	e.vars = append(e.vars, variable{name, def, help})
	if v, ok := e.lookup(name); ok && v != "" {
		return v
	}
	return def
}

// Secret is a value with no default, like a password: it must be set, and it's never shown.
func (e *Env) Secret(name, help string) string {
	e.vars = append(e.vars, variable{name, "(secret)", help})
	v, ok := e.lookup(name)
	if !ok || v == "" {
		e.errs = append(e.errs, fmt.Errorf("%s isn't set: it's %s", name, help))
	}
	return v
}

// Int is a whole number above zero.
func (e *Env) Int(name string, def int, help string) int {
	s := e.String(name, strconv.Itoa(def), help)
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		e.errs = append(e.errs, fmt.Errorf("%s is %q, and it must be a whole number above zero", name, s))
	}
	return n
}

// Float is a number above zero.
func (e *Env) Float(name string, def float64, help string) float64 {
	s := e.String(name, strconv.FormatFloat(def, 'f', -1, 64), help)
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f <= 0 {
		e.errs = append(e.errs, fmt.Errorf("%s is %q, and it must be a number above zero", name, s))
	}
	return f
}

// Duration is a length of time above zero, like 10s or 1m30s.
func (e *Env) Duration(name string, def time.Duration, help string) time.Duration {
	s := e.String(name, def.String(), help)
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		e.errs = append(e.errs, fmt.Errorf("%s is %q, and it must be a duration like 10s", name, s))
	}
	return d
}

// Err is every value that was wrong, each with its variable's name, or nil.
func (e *Env) Err() error {
	return errors.Join(e.errs...)
}

// Usage lists every setting, with its default and what it's for.
func (e *Env) Usage(w io.Writer) {
	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	for _, v := range e.vars {
		fmt.Fprintf(tw, "  %s\t%s\t%s\n", v.name, v.def, v.help)
	}
	tw.Flush()
}

// Check ends the reading of settings, the way flag.Parse does. With -h, it lists them and exits, and
// with any value that's wrong, it says which and why, and exits with status 2.
func (e *Env) Check(program string, args []string) {
	if slices.Contains(args, "-h") || slices.Contains(args, "--help") {
		fmt.Printf("%s reads its settings from the environment:\n", program)
		e.Usage(os.Stdout)
		os.Exit(0)
	}
	if err := e.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", program, err)
		os.Exit(2)
	}
}
