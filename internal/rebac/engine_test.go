package rebac

import (
	"context"
	"os"
	"testing"
)

func TestEngine_BasicTupleAndIndexing(t *testing.T) {
	e := NewEngine()

	t1 := Tuple{
		Object:   ObjectRef{Type: "host", ID: "srv-01"},
		Relation: "workspace",
		Subject:  SubjectRef{ObjectRef: ObjectRef{Namespace: "rbac", Type: "workspace", ID: "ws-prod"}},
	}
	e.WriteTuple(t1)

	if len(e.ListAllTuples()) != 1 {
		t.Fatalf("expected 1 tuple, got %d", len(e.ListAllTuples()))
	}

	// Verify indexing
	objTuples := e.byObject["host:srv-01"]
	if len(objTuples) != 1 {
		t.Fatalf("expected byObject index to have 1 tuple, got %d", len(objTuples))
	}

	subTuples := e.bySubject["rbac/workspace:ws-prod"]
	if len(subTuples) != 1 {
		t.Fatalf("expected bySubject index to have 1 tuple, got %d", len(subTuples))
	}

	// Delete
	e.DeleteTuple(t1)
	if len(e.ListAllTuples()) != 0 {
		t.Fatalf("expected 0 tuples after delete, got %d", len(e.ListAllTuples()))
	}
	if len(e.byObject) != 0 {
		t.Fatalf("expected byObject index to be empty, got %d", len(e.byObject))
	}
}

func TestEngine_DeleteTuplesByFilter(t *testing.T) {
	e := NewEngine()

	e.WriteTuple(Tuple{
		Object:   ObjectRef{Type: "host", ID: "srv-01"},
		Relation: "workspace",
		Subject:  SubjectRef{ObjectRef: ObjectRef{Namespace: "rbac", Type: "workspace", ID: "ws-prod"}},
	})
	e.WriteTuple(Tuple{
		Object:   ObjectRef{Type: "host", ID: "srv-02"},
		Relation: "workspace",
		Subject:  SubjectRef{ObjectRef: ObjectRef{Namespace: "rbac", Type: "workspace", ID: "ws-prod"}},
	})
	e.WriteTuple(Tuple{
		Object:   ObjectRef{Type: "k8s_cluster", ID: "cls-01"},
		Relation: "workspace",
		Subject:  SubjectRef{ObjectRef: ObjectRef{Namespace: "rbac", Type: "workspace", ID: "ws-prod"}},
	})

	resType := "host"
	resID := "srv-01"
	del := e.DeleteTuplesByFilter(nil, &resType, &resID, nil, nil, nil, nil, nil)
	if del != 1 {
		t.Fatalf("expected 1 deleted tuple, got %d", del)
	}

	if len(e.ListAllTuples()) != 2 {
		t.Fatalf("expected 2 tuples remaining, got %d", len(e.ListAllTuples()))
	}
}

func TestEngine_CustomRoleConfigAndWildcard(t *testing.T) {
	e := NewEngine()

	tmpFile, err := os.CreateTemp("", "roles-*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	configJSON := `{
		"roles": {
			"inventory_operator": ["inventory:*", "host:view", "host:edit"],
			"security_auditor": ["security:scan:read"]
		},
		"macro_expansions": {
			"view": ["read", "get", "list"]
		}
	}`
	if _, err := tmpFile.Write([]byte(configJSON)); err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()

	if err := e.LoadRoleConfigFile(tmpFile.Name()); err != nil {
		t.Fatalf("failed to load role config: %v", err)
	}

	// Host srv-01 -> workspace ws-prod
	e.WriteTuple(Tuple{
		Object:   ObjectRef{Type: "host", ID: "srv-01"},
		Relation: "workspace",
		Subject:  SubjectRef{ObjectRef: ObjectRef{Namespace: "rbac", Type: "workspace", ID: "ws-prod"}},
	})

	// ws-prod -> user_grant -> rb-01
	e.WriteTuple(Tuple{
		Object:   ObjectRef{Namespace: "rbac", Type: "workspace", ID: "ws-prod"},
		Relation: "user_grant",
		Subject:  SubjectRef{ObjectRef: ObjectRef{Namespace: "rbac", Type: "role_binding", ID: "rb-01"}},
	})

	// rb-01 -> subject -> principal:alice
	e.WriteTuple(Tuple{
		Object:   ObjectRef{Namespace: "rbac", Type: "role_binding", ID: "rb-01"},
		Relation: "subject",
		Subject:  SubjectRef{ObjectRef: ObjectRef{Namespace: "rbac", Type: "principal", ID: "alice"}},
	})

	// rb-01 -> granted -> role:inventory_operator
	e.WriteTuple(Tuple{
		Object:   ObjectRef{Namespace: "rbac", Type: "role_binding", ID: "rb-01"},
		Relation: "granted",
		Subject:  SubjectRef{ObjectRef: ObjectRef{Namespace: "rbac", Type: "role", ID: "inventory_operator"}},
	})

	alice := SubjectRef{ObjectRef: ObjectRef{Namespace: "rbac", Type: "principal", ID: "alice"}}

	// Alice should have view on host:srv-01 via host:*
	ctx := context.Background()
	allowed, err := e.CheckPermission(ctx, ObjectRef{Type: "host", ID: "srv-01"}, "view", alice)
	if err != nil || !allowed {
		t.Fatalf("expected alice to have view permission, got allowed=%v, err=%v", allowed, err)
	}

	// Alice should NOT have patch:apply
	denied, err := e.CheckPermission(ctx, ObjectRef{Type: "host", ID: "srv-01"}, "patch:apply", alice)
	if err != nil || denied {
		t.Fatalf("expected alice to NOT have patch:apply permission, got allowed=%v", denied)
	}
}

func TestEngine_LookupSubjects(t *testing.T) {
	e := NewEngine()

	// Direct grant
	e.WriteTuple(Tuple{
		Object:   ObjectRef{Type: "host", ID: "srv-direct"},
		Relation: "view",
		Subject:  SubjectRef{ObjectRef: ObjectRef{Namespace: "rbac", Type: "principal", ID: "bob"}},
	})

	subjs, err := e.LookupSubjects(context.Background(), ObjectRef{Type: "host", ID: "srv-direct"}, "view", "principal")
	if err != nil {
		t.Fatal(err)
	}
	if len(subjs) != 1 || subjs[0].ID != "bob" {
		t.Fatalf("expected [bob], got %+v", subjs)
	}
}
