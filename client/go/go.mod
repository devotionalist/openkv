module github.com/devotionalist/openkv/client/go

go 1.22

require (
	github.com/devotionalist/openkv-client-go v0.0.0
	google.golang.org/grpc v1.67.1
	google.golang.org/protobuf v1.35.1
)

require (
	golang.org/x/net v0.28.0 // indirect
	golang.org/x/sys v0.24.0 // indirect
	golang.org/x/text v0.17.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240814211410-ddb44dafa142 // indirect
)

// Local workspace: prefer sibling SDK checkout.
replace github.com/devotionalist/openkv-client-go => ../../../openkv-client-go
