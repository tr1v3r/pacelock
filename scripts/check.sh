#!/bin/sh
# Run from any directory; backend integration is selected by PACELOCK_* variables.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$root"
unformatted=$(gofmt -s -l .)
if [ -n "$unformatted" ]; then
    printf '%s\n' "$unformatted"
    exit 1
fi
for module in . driver/redis driver/postgres tests/contracts examples/postgres examples/redis; do
    (
        cd "$root/$module"
        go mod verify
        go vet ./...
        case $module in
        examples/*)
            # The runnable examples have no tests. Compile without forwarding
            # coverage flags: auto-selected Go 1.25.0 lacks the covdata tool
            # needed to synthesize coverage for a package without test files.
            go test -race -count=1 -timeout=5m ./...
            ;;
        *)
            go test -race -count=1 -timeout=5m "$@" ./...
            ;;
        esac
    )
done
