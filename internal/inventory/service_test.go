package inventory

import (
	"context"
	"testing"

	v1beta2 "github.com/project-kessel/inventory-api/api/kessel/inventory/v1beta2"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/bootstrapper"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/rebac"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/schema"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
)

type mockObjectsStream struct {
	grpc.ServerStreamingServer[v1beta2.StreamedListObjectsResponse]
	ctx   context.Context
	resps []*v1beta2.StreamedListObjectsResponse
}

func (m *mockObjectsStream) Context() context.Context { return m.ctx }
func (m *mockObjectsStream) Send(r *v1beta2.StreamedListObjectsResponse) error {
	m.resps = append(m.resps, r)
	return nil
}

type mockSubjectsStream struct {
	grpc.ServerStreamingServer[v1beta2.StreamedListSubjectsResponse]
	ctx   context.Context
	resps []*v1beta2.StreamedListSubjectsResponse
}

func (m *mockSubjectsStream) Context() context.Context { return m.ctx }
func (m *mockSubjectsStream) Send(r *v1beta2.StreamedListSubjectsResponse) error {
	m.resps = append(m.resps, r)
	return nil
}

func TestInventoryService_FullLifecycleAndCheckSelf(t *testing.T) {
	db, err := storage.NewSQLiteStorage(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	eng := rebac.NewEngine()
	val, err := schema.NewValidator()
	if err != nil {
		t.Fatal(err)
	}

	boot := bootstrapper.NewBootstrapper(eng)

	svc := NewService(db, eng, val, boot)
	ctx := context.Background()

	// 1. ReportResource
	commonStruct, _ := structpb.NewStruct(map[string]interface{}{"workspace_id": "default_default-org"})
	repStruct, _ := structpb.NewStruct(map[string]interface{}{"cpu_cores": 8})
	consoleHref := "https://console.example.com/hosts/srv-01"

	repResp, err := svc.ReportResource(ctx, &v1beta2.ReportResourceRequest{
		Type:         "host",
		ReporterType: "hbi",
		Representations: &v1beta2.ResourceRepresentations{
			Metadata: &v1beta2.RepresentationMetadata{
				LocalResourceId: "srv-01",
				ApiHref:         "https://api.example.com/hosts/srv-01",
				ConsoleHref:     &consoleHref,
			},
			Common:   commonStruct,
			Reporter: repStruct,
		},
	})
	if err != nil {
		t.Fatalf("ReportResource failed: %v", err)
	}
	if repResp == nil {
		t.Fatal("expected non-nil repResp")
	}

	// 2. Check with admin principal
	checkResp, err := svc.Check(ctx, &v1beta2.CheckRequest{
		Object:   &v1beta2.ResourceReference{ResourceType: "host", ResourceId: "srv-01"},
		Relation: "view",
		Subject: &v1beta2.SubjectReference{
			Resource: &v1beta2.ResourceReference{
				ResourceType: "principal",
				ResourceId:   "redhat/test-admin",
			},
		},
		Consistency: &v1beta2.Consistency{
			Requirement: &v1beta2.Consistency_MinimizeLatency{MinimizeLatency: true},
		},
	})
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if checkResp.Allowed != v1beta2.Allowed_ALLOWED_TRUE {
		t.Fatalf("expected admin-user to be ALLOWED_TRUE, got %v", checkResp.Allowed)
	}
	if checkResp.ConsistencyToken == nil || checkResp.ConsistencyToken.Token == "" {
		t.Fatal("expected non-empty consistency token")
	}

	// 3. CheckForUpdateBulk
	bulkUpResp, err := svc.CheckForUpdateBulk(ctx, &v1beta2.CheckForUpdateBulkRequest{
		Items: []*v1beta2.CheckBulkRequestItem{
			{
				Object:   &v1beta2.ResourceReference{ResourceType: "host", ResourceId: "srv-01"},
				Relation: "view",
				Subject: &v1beta2.SubjectReference{
					Resource: &v1beta2.ResourceReference{
						ResourceType: "principal",
						ResourceId:   "redhat/test-admin",
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CheckForUpdateBulk failed: %v", err)
	}
	if len(bulkUpResp.Pairs) != 1 {
		t.Fatalf("expected 1 pair, got %d", len(bulkUpResp.Pairs))
	}

	// 4. CheckSelf with x-rh-user-id metadata
	selfCtx := metadata.NewIncomingContext(ctx, metadata.Pairs("x-rh-user-id", "redhat/test-admin"))
	selfResp, err := svc.CheckSelf(selfCtx, &v1beta2.CheckSelfRequest{
		Object:   &v1beta2.ResourceReference{ResourceType: "host", ResourceId: "srv-01"},
		Relation: "view",
	})
	if err != nil {
		t.Fatalf("CheckSelf failed: %v", err)
	}
	if selfResp.Allowed != v1beta2.Allowed_ALLOWED_TRUE {
		t.Fatalf("expected CheckSelf to allow test-admin, got %v", selfResp.Allowed)
	}

	// 5. StreamedListObjects
	objStream := &mockObjectsStream{ctx: ctx}
	err = svc.StreamedListObjects(&v1beta2.StreamedListObjectsRequest{
		ObjectType: &v1beta2.RepresentationType{ResourceType: "host"},
		Relation:   "view",
		Subject: &v1beta2.SubjectReference{
			Resource: &v1beta2.ResourceReference{
				ResourceType: "principal",
				ResourceId:   "redhat/test-admin",
			},
		},
	}, objStream)
	if err != nil {
		t.Fatalf("StreamedListObjects failed: %v", err)
	}
	if len(objStream.resps) == 0 {
		t.Fatal("expected at least 1 streamed object")
	}

	// 6. DeleteResource
	_, err = svc.DeleteResource(ctx, &v1beta2.DeleteResourceRequest{
		Reference: &v1beta2.ResourceReference{
			ResourceType: "host",
			ResourceId:   "srv-01",
			Reporter: &v1beta2.ReporterReference{
				Type: "hbi",
			},
		},
	})
	if err != nil {
		t.Fatalf("DeleteResource failed: %v", err)
	}

	// Verify Check is now denied
	checkAfterDel, err := svc.Check(ctx, &v1beta2.CheckRequest{
		Object:   &v1beta2.ResourceReference{ResourceType: "host", ResourceId: "srv-01"},
		Relation: "view",
		Subject: &v1beta2.SubjectReference{
			Resource: &v1beta2.ResourceReference{
				ResourceType: "principal",
				ResourceId:   "redhat/test-admin",
			},
		},
	})
	if err != nil {
		t.Fatalf("Check after delete failed: %v", err)
	}
	if checkAfterDel.Allowed != v1beta2.Allowed_ALLOWED_FALSE {
		t.Fatalf("expected ALLOWED_FALSE after delete, got %v", checkAfterDel.Allowed)
	}
}
