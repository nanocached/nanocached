// Development tools for the Go SDK, in their own module so the SDK's go.mod
// stays standard-library only and Dependabot (gomod, /sdk/go/tools) proposes
// their updates. `go install -C tools honnef.co/go/tools/cmd/staticcheck`
// from sdk/go installs the pinned version.
module github.com/nanocached/nanocached/sdk/go/tools

go 1.26.0

tool honnef.co/go/tools/cmd/staticcheck

require (
	github.com/BurntSushi/toml v1.4.1-0.20240526193622-a339e1f7089c // indirect
	golang.org/x/exp/typeparams v0.0.0-20231108232855-2478ac86f678 // indirect
	golang.org/x/mod v0.41.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/tools v0.51.0 // indirect
	honnef.co/go/tools v0.8.2 // indirect
)
