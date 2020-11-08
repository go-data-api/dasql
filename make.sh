#!/bin/bash
set -e

function print_help {
	printf "Available Commands:\n";
	awk -v sq="'" '/^function run_([a-zA-Z0-9-]*)\s*/ {print "-e " sq NR "p" sq " -e " sq NR-1 "p" sq }' "$0" \
		| while read line; do eval "sed -n $line $0"; done \
		| paste -d"|" - - \
		| sed -e 's/^/  /' -e 's/function run_//' -e 's/#//' -e 's/{/	/' \
		| awk -F '|' '{ print "  " $2 "\t" $1}' \
		| expand -t 30
}

function run_test { # test the complete codebase and show coverage report
	command -v go >/dev/null 2>&1 || { echo "executable 'go' must be installed" >&2; exit 1; }	
	go test -race -covermode=atomic -coverprofile=/tmp/cover ./... \
		&& go tool cover -html=/tmp/cover 
}

case $1 in
	"test") run_test ;;
	*) print_help ;;
esac