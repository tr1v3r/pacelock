#!/bin/sh
# Run from any directory; backend integration is selected by PACELOCK_* variables.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
unformatted=$(gofmt -l .)
if [ -n "$unformatted" ]; then
    printf '%s\n' "$unformatted"
    exit 1
fi
for module in . drivers/redis drivers/postgres tests/contracts examples/postgres; do
    (
        cd "$root/$module"
        go mod verify
        go vet ./...
        if [ "$module" = examples/postgres ]; then
            # The runnable example has no tests. Compile it without forwarding
            # coverage flags: auto-selected Go 1.25.0 lacks the covdata tool
            # needed to synthesize coverage for a package without test files.
            go test -race -count=1 -timeout=5m ./...
        else
            go test -race -count=1 -timeout=5m "$@" ./...
        fi
    )
done
