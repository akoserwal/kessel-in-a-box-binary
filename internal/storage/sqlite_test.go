package storage

import (
	"testing"
)

func TestSQLiteStorage_SchemaParityAndMetadata(t *testing.T) {
	db, err := NewSQLiteStorage(":memory:")
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	defer db.Close()

	commonPayload := map[string]interface{}{"workspace_id": "ws-test", "labels": map[string]interface{}{"env": "prod"}}
	repPayload := map[string]interface{}{"cpu_cores": 16}

	meta := ResourceMetadata{
		ApiHref:         "https://api.example.com/v1/hosts/srv-01",
		ConsoleHref:     "https://console.example.com/insights/inventory/srv-01",
		ReporterVersion: "0.4.1",
	}

	resID, token, err := db.UpsertResource(
		"host", "hbi", "srv-01", "inst-1",
		"ws-test", "tx-12345",
		commonPayload, repPayload,
		meta,
	)
	if err != nil {
		t.Fatalf("UpsertResource failed: %v", err)
	}
	if resID == "" || token == "" {
		t.Fatalf("expected non-empty resID and token, got %q, %q", resID, token)
	}

	// Verify GetResourceKTN
	ktn, err := db.GetResourceKTN("host", "srv-01")
	if err != nil {
		t.Fatalf("GetResourceKTN failed: %v", err)
	}
	if ktn != token {
		t.Fatalf("expected ktn %q, got %q", token, ktn)
	}

	// Verify ListResourcesByWorkspace
	wsResources, err := db.ListResourcesByWorkspace("ws-test", 10)
	if err != nil {
		t.Fatalf("ListResourcesByWorkspace failed: %v", err)
	}
	if len(wsResources) != 1 || wsResources[0].ID != resID {
		t.Fatalf("expected 1 resource in ws-test, got %+v", wsResources)
	}

	// Verify empty workspace returns empty
	otherResources, err := db.ListResourcesByWorkspace("ws-other", 10)
	if err != nil {
		t.Fatalf("ListResourcesByWorkspace failed: %v", err)
	}
	if len(otherResources) != 0 {
		t.Fatalf("expected 0 resources in ws-other, got %d", len(otherResources))
	}

	// Verify SoftDeleteResource
	delID, err := db.SoftDeleteResource("hbi", "srv-01", "host", "inst-1")
	if err != nil {
		t.Fatalf("SoftDeleteResource failed: %v", err)
	}
	if delID != resID {
		t.Fatalf("expected delID %q, got %q", resID, delID)
	}

	// Verify it no longer appears in ListResources
	active, err := db.ListResources(10)
	if err != nil {
		t.Fatalf("ListResources failed: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("expected 0 active resources, got %d", len(active))
	}
}
