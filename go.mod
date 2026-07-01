module github.com/planx-lab/planx-plugin-sqlserver

go 1.25.7

require (
	github.com/microsoft/go-mssqldb v1.10.0
	github.com/planx-lab/planx-sdk-go v0.0.0-00010101000000-000000000000
)

require (
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/planx-lab/planx-proto v0.0.0-00010101000000-000000000000 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	golang.org/x/crypto v0.50.0 // indirect
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/grpc v1.77.0 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)

replace github.com/planx-lab/planx-sdk-go => ../planx-sdk-go

replace github.com/planx-lab/planx-proto => ../planx-proto
