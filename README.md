## Orleans virtual actors in Go

Currently, this is a science project, *do not* use for anything important at the moment.

## What is this?

This is an experiment to build an implementation of the [Orleans actor model](https://dotnet.github.io/orleans/docs/) in Go
as opposed to in C# / .NET.


## Quickstart(-ish) for the brave

Pre-requisites:
* Go 1.19
* PostgreSQL (local or in container)
* Redis (local or in container)

1. Build binaries via `go build` in `cmd/silod/`, `cmd/client/` and `cmd/reminder/`
2. Create a postgres database and load the schema from `etc/schema.sql`
3. Create a .env file in `cmd/silod/` with the following contents (changed for your environment):
```
DATABASE_URL=postgres://user:password@localhost:5432/database
REDIS_HOST_PORT=localhost:6379
```
4. Run three instances of `silod` (from `./cmd/silod`) (mimic the `Procfile` or use `overmind start` if you have it installed) to have three nodes running
5. Run `CLUSTER_HOST=localhost CLUSTER_PORT=8091 ./client` (from `./cmd/client`) in another terminal to run against the first node
6. Run `CLUSTER_HOST=localhost CLUSTER_PORT=8092 ./reminder` (from `./cmd/reminder`)  in another terminal (optional) to register reminders against the second node
