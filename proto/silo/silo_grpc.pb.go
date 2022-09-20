// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package silo

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// SiloServiceClient is the client API for SiloService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type SiloServiceClient interface {
	Ping(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*PingResponse, error)
	PlaceGrain(ctx context.Context, in *PlaceGrainRequest, opts ...grpc.CallOption) (*PlaceGrainResponse, error)
	ExecuteGrain(ctx context.Context, in *ExecuteGrainRequest, opts ...grpc.CallOption) (*ExecuteGrainResponse, error)
	RegisterGrainHandler(ctx context.Context, in *RegisterGrainHandlerRequest, opts ...grpc.CallOption) (SiloService_RegisterGrainHandlerClient, error)
	ResultStream(ctx context.Context, opts ...grpc.CallOption) (SiloService_ResultStreamClient, error)
}

type siloServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewSiloServiceClient(cc grpc.ClientConnInterface) SiloServiceClient {
	return &siloServiceClient{cc}
}

func (c *siloServiceClient) Ping(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*PingResponse, error) {
	out := new(PingResponse)
	err := c.cc.Invoke(ctx, "/silo.SiloService/Ping", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *siloServiceClient) PlaceGrain(ctx context.Context, in *PlaceGrainRequest, opts ...grpc.CallOption) (*PlaceGrainResponse, error) {
	out := new(PlaceGrainResponse)
	err := c.cc.Invoke(ctx, "/silo.SiloService/PlaceGrain", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *siloServiceClient) ExecuteGrain(ctx context.Context, in *ExecuteGrainRequest, opts ...grpc.CallOption) (*ExecuteGrainResponse, error) {
	out := new(ExecuteGrainResponse)
	err := c.cc.Invoke(ctx, "/silo.SiloService/ExecuteGrain", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *siloServiceClient) RegisterGrainHandler(ctx context.Context, in *RegisterGrainHandlerRequest, opts ...grpc.CallOption) (SiloService_RegisterGrainHandlerClient, error) {
	stream, err := c.cc.NewStream(ctx, &SiloService_ServiceDesc.Streams[0], "/silo.SiloService/RegisterGrainHandler", opts...)
	if err != nil {
		return nil, err
	}
	x := &siloServiceRegisterGrainHandlerClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type SiloService_RegisterGrainHandlerClient interface {
	Recv() (*GrainExecutionRequest, error)
	grpc.ClientStream
}

type siloServiceRegisterGrainHandlerClient struct {
	grpc.ClientStream
}

func (x *siloServiceRegisterGrainHandlerClient) Recv() (*GrainExecutionRequest, error) {
	m := new(GrainExecutionRequest)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *siloServiceClient) ResultStream(ctx context.Context, opts ...grpc.CallOption) (SiloService_ResultStreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &SiloService_ServiceDesc.Streams[1], "/silo.SiloService/ResultStream", opts...)
	if err != nil {
		return nil, err
	}
	x := &siloServiceResultStreamClient{stream}
	return x, nil
}

type SiloService_ResultStreamClient interface {
	Send(*GrainExecutionResult) error
	CloseAndRecv() (*emptypb.Empty, error)
	grpc.ClientStream
}

type siloServiceResultStreamClient struct {
	grpc.ClientStream
}

func (x *siloServiceResultStreamClient) Send(m *GrainExecutionResult) error {
	return x.ClientStream.SendMsg(m)
}

func (x *siloServiceResultStreamClient) CloseAndRecv() (*emptypb.Empty, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(emptypb.Empty)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// SiloServiceServer is the server API for SiloService service.
// All implementations must embed UnimplementedSiloServiceServer
// for forward compatibility
type SiloServiceServer interface {
	Ping(context.Context, *emptypb.Empty) (*PingResponse, error)
	PlaceGrain(context.Context, *PlaceGrainRequest) (*PlaceGrainResponse, error)
	ExecuteGrain(context.Context, *ExecuteGrainRequest) (*ExecuteGrainResponse, error)
	RegisterGrainHandler(*RegisterGrainHandlerRequest, SiloService_RegisterGrainHandlerServer) error
	ResultStream(SiloService_ResultStreamServer) error
	mustEmbedUnimplementedSiloServiceServer()
}

// UnimplementedSiloServiceServer must be embedded to have forward compatible implementations.
type UnimplementedSiloServiceServer struct {
}

func (UnimplementedSiloServiceServer) Ping(context.Context, *emptypb.Empty) (*PingResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Ping not implemented")
}
func (UnimplementedSiloServiceServer) PlaceGrain(context.Context, *PlaceGrainRequest) (*PlaceGrainResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PlaceGrain not implemented")
}
func (UnimplementedSiloServiceServer) ExecuteGrain(context.Context, *ExecuteGrainRequest) (*ExecuteGrainResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ExecuteGrain not implemented")
}
func (UnimplementedSiloServiceServer) RegisterGrainHandler(*RegisterGrainHandlerRequest, SiloService_RegisterGrainHandlerServer) error {
	return status.Errorf(codes.Unimplemented, "method RegisterGrainHandler not implemented")
}
func (UnimplementedSiloServiceServer) ResultStream(SiloService_ResultStreamServer) error {
	return status.Errorf(codes.Unimplemented, "method ResultStream not implemented")
}
func (UnimplementedSiloServiceServer) mustEmbedUnimplementedSiloServiceServer() {}

// UnsafeSiloServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to SiloServiceServer will
// result in compilation errors.
type UnsafeSiloServiceServer interface {
	mustEmbedUnimplementedSiloServiceServer()
}

func RegisterSiloServiceServer(s grpc.ServiceRegistrar, srv SiloServiceServer) {
	s.RegisterService(&SiloService_ServiceDesc, srv)
}

func _SiloService_Ping_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SiloServiceServer).Ping(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/silo.SiloService/Ping",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SiloServiceServer).Ping(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _SiloService_PlaceGrain_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PlaceGrainRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SiloServiceServer).PlaceGrain(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/silo.SiloService/PlaceGrain",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SiloServiceServer).PlaceGrain(ctx, req.(*PlaceGrainRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SiloService_ExecuteGrain_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ExecuteGrainRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SiloServiceServer).ExecuteGrain(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/silo.SiloService/ExecuteGrain",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SiloServiceServer).ExecuteGrain(ctx, req.(*ExecuteGrainRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SiloService_RegisterGrainHandler_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(RegisterGrainHandlerRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(SiloServiceServer).RegisterGrainHandler(m, &siloServiceRegisterGrainHandlerServer{stream})
}

type SiloService_RegisterGrainHandlerServer interface {
	Send(*GrainExecutionRequest) error
	grpc.ServerStream
}

type siloServiceRegisterGrainHandlerServer struct {
	grpc.ServerStream
}

func (x *siloServiceRegisterGrainHandlerServer) Send(m *GrainExecutionRequest) error {
	return x.ServerStream.SendMsg(m)
}

func _SiloService_ResultStream_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(SiloServiceServer).ResultStream(&siloServiceResultStreamServer{stream})
}

type SiloService_ResultStreamServer interface {
	SendAndClose(*emptypb.Empty) error
	Recv() (*GrainExecutionResult, error)
	grpc.ServerStream
}

type siloServiceResultStreamServer struct {
	grpc.ServerStream
}

func (x *siloServiceResultStreamServer) SendAndClose(m *emptypb.Empty) error {
	return x.ServerStream.SendMsg(m)
}

func (x *siloServiceResultStreamServer) Recv() (*GrainExecutionResult, error) {
	m := new(GrainExecutionResult)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// SiloService_ServiceDesc is the grpc.ServiceDesc for SiloService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var SiloService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "silo.SiloService",
	HandlerType: (*SiloServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Ping",
			Handler:    _SiloService_Ping_Handler,
		},
		{
			MethodName: "PlaceGrain",
			Handler:    _SiloService_PlaceGrain_Handler,
		},
		{
			MethodName: "ExecuteGrain",
			Handler:    _SiloService_ExecuteGrain_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "RegisterGrainHandler",
			Handler:       _SiloService_RegisterGrainHandler_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "ResultStream",
			Handler:       _SiloService_ResultStream_Handler,
			ClientStreams: true,
		},
	},
	Metadata: "silo/silo.proto",
}
