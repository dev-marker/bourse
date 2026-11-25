// Package web is the live page: plain HTML, CSS and JavaScript, built into the binary that serves it.
package web

import (
	"embed"
	"net/http"
)

// files are the page's files. They're in the binary, so there's nothing else to deploy.
//
//go:embed index.html style.css app.js
var files embed.FS

// Handler serves the page. Every response says no-cache, so a browser asks again after a deploy.
func Handler() http.Handler {
	fs := http.FileServerFS(files)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		fs.ServeHTTP(w, r)
	})
}
