package embedclient

import (
	"context"
	"fmt"
	"net"

	invpb "github.com/project-kessel/inventory-api/api/kessel/inventory/v1beta2"
	relpb "github.com/project-kessel/relations-api/api/kessel/relations/v1beta1"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/bootstrapper"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/inventory"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/rebac"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/relations"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/schema"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

type BoxInstance struct {
	storage    *storage.SQLiteStorage
	engine     *rebac.Engine
	listener   *bufconn.Listener
	grpcServer *grpc.Server

	Conn *grpc.ClientConn

	InventoryClient invpb.KesselInventoryServiceClient
	TupleClient     relpb.KesselTupleServiceClient
	CheckClient     relpb.KesselCheckServiceClient
	LookupClient    relpb.KesselLookupServiceClient
}

// StartInProcess boots a zero-network, in-memory Kessel-in-a-Box instance via bufconn
func StartInProcess(ctx context.Context) (*BoxInstance, error) {
	db, err := storage.NewSQLiteStorage(":memory:")
	if err != nil {
		return nil, fmt.Errorf("failed to init in-memory sqlite: %w", err)
	}

	engine := rebac.NewEngine()
	validator, err := schema.NewValidator()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to init schema validator: %w", err)
	}

	boot := bootstrapper.NewBootstrapper(engine)
	invSvc := inventory.NewService(db, engine, validator, boot)
	relSvc := relations.NewService(engine)

	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	invpb.RegisterKesselInventoryServiceServer(s, invSvc)
	relpb.RegisterKesselTupleServiceServer(s, relSvc)
	relpb.RegisterKesselCheckServiceServer(s, relSvc)
	relpb.RegisterKesselLookupServiceServer(s, relSvc)

	go func() {
		_ = s.Serve(lis)
	}()

	bufDialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.DialContext(
		ctx,
		"passthrough://bufnet",
		grpc.WithContextDialer(bufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		s.Stop()
		lis.Close()
		db.Close()
		return nil, fmt.Errorf("failed to dial in-process bufconn: %w", err)
	}

	box := &BoxInstance{
		storage:         db,
		engine:          engine,
		listener:        lis,
		grpcServer:      s,
		Conn:            conn,
		InventoryClient: invpb.NewKesselInventoryServiceClient(conn),
		TupleClient:     relpb.NewKesselTupleServiceClient(conn),
		CheckClient:     relpb.NewKesselCheckServiceClient(conn),
		LookupClient:    relpb.NewKesselLookupServiceClient(conn),
	}

	return box, nil
}

func (b *BoxInstance) Close() error {
	if b.Conn != nil {
		_ = b.Conn.Close()
	}
	if b.grpcServer != nil {
		b.grpcServer.Stop()
	}
	if b.listener != nil {
		_ = b.listener.Close()
	}
	if b.storage != nil {
		_ = b.storage.Close()
	}
	return nil
}
