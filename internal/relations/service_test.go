package relations

import (
	"context"
	"testing"

	v1beta1 "github.com/project-kessel/relations-api/api/kessel/relations/v1beta1"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/rebac"
	"google.golang.org/grpc"
)

type mockReadStream struct {
	grpc.ServerStreamingServer[v1beta1.ReadTuplesResponse]
	ctx  context.Context
	resps []*v1beta1.ReadTuplesResponse
}

func (m *mockReadStream) Context() context.Context {
	return m.ctx
}

func (m *mockReadStream) Send(r *v1beta1.ReadTuplesResponse) error {
	m.resps = append(m.resps, r)
	return nil
}

type mockSubjectStream struct {
	grpc.ServerStreamingServer[v1beta1.LookupSubjectsResponse]
	ctx  context.Context
	resps []*v1beta1.LookupSubjectsResponse
}

func (m *mockSubjectStream) Context() context.Context {
	return m.ctx
}

func (m *mockSubjectStream) Send(r *v1beta1.LookupSubjectsResponse) error {
	m.resps = append(m.resps, r)
	return nil
}

func TestRelationsService_CreateDeleteReadTuples(t *testing.T) {
	eng := rebac.NewEngine()
	svc := NewService(eng)
	ctx := context.Background()

	// 1. CreateTuples
	rel := &v1beta1.Relationship{
		Resource: &v1beta1.ObjectReference{
			Type: &v1beta1.ObjectType{Namespace: "rbac", Name: "workspace"},
			Id:   "ws-01",
		},
		Relation: "parent",
		Subject: &v1beta1.SubjectReference{
			Subject: &v1beta1.ObjectReference{
				Type: &v1beta1.ObjectType{Namespace: "rbac", Name: "workspace"},
				Id:   "root-01",
			},
		},
	}
	createResp, err := svc.CreateTuples(ctx, &v1beta1.CreateTuplesRequest{
		Tuples: []*v1beta1.Relationship{rel},
	})
	if err != nil {
		t.Fatalf("failed to create tuples: %v", err)
	}
	if createResp.ConsistencyToken == nil || createResp.ConsistencyToken.Token == "" {
		t.Fatal("expected non-empty consistency token")
	}

	// 2. ReadTuples
	readStream := &mockReadStream{ctx: ctx}
	resType := "workspace"
	resID := "ws-01"
	err = svc.ReadTuples(&v1beta1.ReadTuplesRequest{
		Filter: &v1beta1.RelationTupleFilter{
			ResourceType: &resType,
			ResourceId:   &resID,
		},
	}, readStream)
	if err != nil {
		t.Fatalf("failed to read tuples: %v", err)
	}
	if len(readStream.resps) != 1 {
		t.Fatalf("expected 1 tuple read, got %d", len(readStream.resps))
	}

	// 3. DeleteTuples
	delResp, err := svc.DeleteTuples(ctx, &v1beta1.DeleteTuplesRequest{
		Filter: &v1beta1.RelationTupleFilter{
			ResourceType: &resType,
			ResourceId:   &resID,
		},
	})
	if err != nil {
		t.Fatalf("failed to delete tuples: %v", err)
	}
	if delResp.ConsistencyToken == nil {
		t.Fatal("expected consistency token on delete")
	}

	// Verify it's gone
	readStream2 := &mockReadStream{ctx: ctx}
	_ = svc.ReadTuples(&v1beta1.ReadTuplesRequest{
		Filter: &v1beta1.RelationTupleFilter{
			ResourceType: &resType,
			ResourceId:   &resID,
		},
	}, readStream2)
	if len(readStream2.resps) != 0 {
		t.Fatalf("expected 0 tuples after delete, got %d", len(readStream2.resps))
	}
}

func TestRelationsService_CheckBulkAndLock(t *testing.T) {
	eng := rebac.NewEngine()
	svc := NewService(eng)
	ctx := context.Background()

	// Direct check
	eng.WriteTuple(rebac.Tuple{
		Object:   rebac.ObjectRef{Type: "doc", ID: "doc-1"},
		Relation: "view",
		Subject:  rebac.SubjectRef{ObjectRef: rebac.ObjectRef{Type: "principal", ID: "alice"}},
	})

	cResp, err := svc.CheckBulk(ctx, &v1beta1.CheckBulkRequest{
		Items: []*v1beta1.CheckBulkRequestItem{
			{
				Resource: &v1beta1.ObjectReference{Type: &v1beta1.ObjectType{Name: "doc"}, Id: "doc-1"},
				Relation: "view",
				Subject: &v1beta1.SubjectReference{
					Subject: &v1beta1.ObjectReference{Type: &v1beta1.ObjectType{Name: "principal"}, Id: "alice"},
				},
			},
			{
				Resource: &v1beta1.ObjectReference{Type: &v1beta1.ObjectType{Name: "doc"}, Id: "doc-1"},
				Relation: "edit",
				Subject: &v1beta1.SubjectReference{
					Subject: &v1beta1.ObjectReference{Type: &v1beta1.ObjectType{Name: "principal"}, Id: "alice"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CheckBulk failed: %v", err)
	}
	if len(cResp.Pairs) != 2 {
		t.Fatalf("expected 2 pairs, got %d", len(cResp.Pairs))
	}

	// Acquire lock
	lResp, err := svc.AcquireLock(ctx, &v1beta1.AcquireLockRequest{LockId: "res-lock-1"})
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	if lResp.LockToken == "" {
		t.Fatal("expected non-empty lock token")
	}
}
