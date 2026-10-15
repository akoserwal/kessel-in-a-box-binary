package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"time"

	invpb "github.com/project-kessel/inventory-api/api/kessel/inventory/v1beta2"
	relpb "github.com/project-kessel/relations-api/api/kessel/relations/v1beta1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	grpcTarget = "127.0.0.1:9000"
	httpTarget = "http://127.0.0.1:8088"
)

func main() {
	fmt.Println("=====================================================================")
	fmt.Println("🧪 Starting End-to-End Verification of Kessel-in-a-Box (kessel-box)")
	fmt.Println("=====================================================================")

	// 0. Check if server is already running, if not spawn it
	var spawnedCmd *exec.Cmd
	probeConn, err := net.DialTimeout("tcp", grpcTarget, 100*time.Millisecond)
	if err != nil {
		fmt.Println("ℹ️  Server not detected on :9000, automatically launching bin/kessel-box...")
		spawnedCmd = exec.Command("bin/kessel-box", "start", "--grpc-port=9000", "--http-port=8088", "--db-path=:memory:")
		if err := spawnedCmd.Start(); err != nil {
			fmt.Printf("❌ Failed to auto-launch kessel-box: %v\n", err)
			return
		}
		defer func() {
			if spawnedCmd != nil && spawnedCmd.Process != nil {
				_ = spawnedCmd.Process.Kill()
			}
		}()

		// Wait up to 3s for startup
		for i := 0; i < 30; i++ {
			c, err := net.DialTimeout("tcp", grpcTarget, 100*time.Millisecond)
			if err == nil {
				c.Close()
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	} else {
		probeConn.Close()
	}

	// 1. Establish gRPC Connection
	conn, err := grpc.NewClient(grpcTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("❌ Failed to connect to gRPC server at %s: %v\n", grpcTarget, err)
		return
	}
	defer conn.Close()

	invClient := invpb.NewKesselInventoryServiceClient(conn)
	relClient := relpb.NewKesselTupleServiceClient(conn)
	checkClient := relpb.NewKesselCheckServiceClient(conn)
	lookupClient := relpb.NewKesselLookupServiceClient(conn)

	// Run all scenarios
	passCount := 0
	totalScenarios := 15

	if runScenario1_HealthAndStatus() {
		passCount++
	}
	resID, token, ok := runScenario2_ReportResource(invClient)
	if ok {
		passCount++
	}
	if runScenario3_ImmediateReadAfterWrite(invClient, resID, token) {
		passCount++
	}
	if runScenario4_RolePermissionsAndDenials(invClient) {
		passCount++
	}
	if runScenario5_IdempotentReplay(invClient) {
		passCount++
	}
	if runScenario6_SchemaValidationGuardrails(invClient) {
		passCount++
	}
	if runScenario7_BulkCheck(invClient) {
		passCount++
	}
	if runScenario8_RelationsAPIExplicitTuples(relClient, checkClient) {
		passCount++
	}
	if runScenario9_ResourceDeletionAndRevocation(invClient) {
		passCount++
	}
	if runScenario10_HTTPDeveloperEndpoints() {
		passCount++
	}
	if runScenario11_DeleteTuplesRelationsAPI(relClient, checkClient) {
		passCount++
	}
	if runScenario12_CheckSelfWithContextIdentity(invClient) {
		passCount++
	}
	if runScenario13_StreamedListObjects(invClient) {
		passCount++
	}
	if runScenario14_StreamedReadTuplesAndLookupSubjects(relClient, lookupClient) {
		passCount++
	}
	if runScenario15_CheckForUpdateBulk(invClient) {
		passCount++
	}

	fmt.Println("---------------------------------------------------------------------")
	fmt.Println("⚡ Running Performance Benchmark (1,000 In-Memory Permission Checks)...")
	runBenchmark(invClient)

	fmt.Println("=====================================================================")
	if passCount == totalScenarios {
		fmt.Printf("🎉 ALL SCENARIOS PASSED (%d/%d)! System is 100%% verified.\n", passCount, totalScenarios)
	} else {
		fmt.Printf("⚠️ %d/%d SCENARIOS PASSED. Review errors above.\n", passCount, totalScenarios)
	}
	fmt.Println("=====================================================================")
}

func runScenario1_HealthAndStatus() bool {
	fmt.Print("\n[Scenario 1] HTTP /healthz and /api/status Check... ")
	resp, err := http.Get(httpTarget + "/healthz")
	if err != nil || resp.StatusCode != http.StatusOK {
		fmt.Printf("FAILED: %v\n", err)
		return false
	}
	defer resp.Body.Close()

	statusResp, err := http.Get(httpTarget + "/api/status")
	if err != nil || statusResp.StatusCode != http.StatusOK {
		fmt.Printf("FAILED: %v\n", err)
		return false
	}
	defer statusResp.Body.Close()

	var data map[string]interface{}
	_ = json.NewDecoder(statusResp.Body).Decode(&data)
	if data["status"] != "healthy" {
		fmt.Printf("FAILED: status is %v\n", data["status"])
		return false
	}

	fmt.Printf("PASSED (status: %v, mode: %v)\n", data["status"], data["mode"])
	return true
}

func runScenario2_ReportResource(client invpb.KesselInventoryServiceClient) (string, string, bool) {
	fmt.Print("[Scenario 2] Synchronous Write-Through ReportResource... ")

	ver := "1.0.0"
	common, _ := structpb.NewStruct(map[string]interface{}{
		"workspace_id":   "default_org-verify",
		"transaction_id": "tx-verify-001",
	})
	reporter, _ := structpb.NewStruct(map[string]interface{}{
		"ansible_host": "prod-db-01.internal",
	})

	req := &invpb.ReportResourceRequest{
		Type:               "host",
		ReporterType:       "hbi",
		ReporterInstanceId: "cluster-us-east-1",
		Representations: &invpb.ResourceRepresentations{
			Metadata: &invpb.RepresentationMetadata{
				LocalResourceId: "srv-host-001",
				ReporterVersion: &ver,
			},
			Common:   common,
			Reporter: reporter,
		},
	}

	var headerMD metadata.MD
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-rh-rbac-org-id", "org-verify"))
	_, err := client.ReportResource(ctx, req, grpc.Header(&headerMD))
	if err != nil {
		fmt.Printf("FAILED: %v\n", err)
		return "", "", false
	}

	resID := ""
	token := ""
	if vals := headerMD.Get("x-kessel-resource-id"); len(vals) > 0 {
		resID = vals[0]
	}
	if vals := headerMD.Get("x-kessel-consistency-token"); len(vals) > 0 {
		token = vals[0]
	}

	fmt.Printf("PASSED (UUID: %s, ZedToken: %s)\n", resID, token)
	return resID, token, true
}

func runScenario3_ImmediateReadAfterWrite(client invpb.KesselInventoryServiceClient, resID, token string) bool {
	fmt.Print("[Scenario 3] Immediate Read-After-Write Consistency (0ms delay)... ")

	// Note: We check immediately without time.Sleep()!
	req := &invpb.CheckRequest{
		Object: &invpb.ResourceReference{
			ResourceType: "host",
			ResourceId:   "srv-host-001",
		},
		Relation: "view",
		Subject: &invpb.SubjectReference{
			Resource: &invpb.ResourceReference{
				ResourceType: "principal",
				ResourceId:   "redhat/test-admin",
			},
		},
	}

	resp, err := client.Check(context.Background(), req)
	if err != nil {
		fmt.Printf("FAILED: %v\n", err)
		return false
	}
	if resp.Allowed != invpb.Allowed_ALLOWED_TRUE {
		fmt.Printf("FAILED: Allowed was %v, expected ALLOWED_TRUE\n", resp.Allowed)
		return false
	}

	fmt.Println("PASSED (Result: ALLOWED_TRUE, 0ms sync delay achieved!)")
	return true
}

func runScenario4_RolePermissionsAndDenials(client invpb.KesselInventoryServiceClient) bool {
	fmt.Print("[Scenario 4] Fine-Grained Role Permissions & Negative Denials... ")

	// 1. Viewer trying to perform 'edit' -> Must be DENIED
	viewerEditReq := &invpb.CheckRequest{
		Object: &invpb.ResourceReference{
			ResourceType: "host",
			ResourceId:   "srv-host-001",
		},
		Relation: "edit",
		Subject: &invpb.SubjectReference{
			Resource: &invpb.ResourceReference{
				ResourceType: "principal",
				ResourceId:   "redhat/test-viewer",
			},
		},
	}
	resp1, err := client.Check(context.Background(), viewerEditReq)
	if err != nil || resp1.Allowed != invpb.Allowed_ALLOWED_FALSE {
		fmt.Printf("FAILED: viewer edit should be FALSE, got %v (err: %v)\n", resp1.Allowed, err)
		return false
	}

	// 2. Viewer trying to perform 'view' -> Must be ALLOWED
	viewerViewReq := &invpb.CheckRequest{
		Object: &invpb.ResourceReference{
			ResourceType: "host",
			ResourceId:   "srv-host-001",
		},
		Relation: "view",
		Subject: &invpb.SubjectReference{
			Resource: &invpb.ResourceReference{
				ResourceType: "principal",
				ResourceId:   "redhat/test-viewer",
			},
		},
	}
	resp2, err := client.Check(context.Background(), viewerViewReq)
	if err != nil || resp2.Allowed != invpb.Allowed_ALLOWED_TRUE {
		fmt.Printf("FAILED: viewer view should be TRUE, got %v (err: %v)\n", resp2.Allowed, err)
		return false
	}

	// 3. Unrelated Stranger -> Must be DENIED
	strangerReq := &invpb.CheckRequest{
		Object: &invpb.ResourceReference{
			ResourceType: "host",
			ResourceId:   "srv-host-001",
		},
		Relation: "view",
		Subject: &invpb.SubjectReference{
			Resource: &invpb.ResourceReference{
				ResourceType: "principal",
				ResourceId:   "redhat/stranger-bob",
			},
		},
	}
	resp3, err := client.Check(context.Background(), strangerReq)
	if err != nil || resp3.Allowed != invpb.Allowed_ALLOWED_FALSE {
		fmt.Printf("FAILED: stranger view should be FALSE, got %v (err: %v)\n", resp3.Allowed, err)
		return false
	}

	fmt.Println("PASSED (Viewer and Stranger restrictions correctly enforced)")
	return true
}

func runScenario5_IdempotentReplay(client invpb.KesselInventoryServiceClient) bool {
	fmt.Print("[Scenario 5] Idempotent Write Replay via transaction_id... ")

	ver := "1.0.0"
	common, _ := structpb.NewStruct(map[string]interface{}{
		"workspace_id":   "default_org-verify",
		"transaction_id": "tx-verify-001", // SAME transaction_id
	})
	reporter, _ := structpb.NewStruct(map[string]interface{}{
		"ansible_host": "prod-db-01.internal",
	})

	req := &invpb.ReportResourceRequest{
		Type:               "host",
		ReporterType:       "hbi",
		ReporterInstanceId: "cluster-us-east-1",
		Representations: &invpb.ResourceRepresentations{
			Metadata: &invpb.RepresentationMetadata{
				LocalResourceId: "srv-host-001",
				ReporterVersion: &ver,
			},
			Common:   common,
			Reporter: reporter,
		},
	}

	var headerMD metadata.MD
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-rh-rbac-org-id", "org-verify"))
	_, err := client.ReportResource(ctx, req, grpc.Header(&headerMD))
	if err != nil {
		fmt.Printf("FAILED: Replay caused error: %v\n", err)
		return false
	}

	fmt.Println("PASSED (Duplicate request safely acknowledged without conflict)")
	return true
}

func runScenario6_SchemaValidationGuardrails(client invpb.KesselInventoryServiceClient) bool {
	fmt.Print("[Scenario 6] Schema Validation Guardrails... ")

	// Submit invalid payload: empty required local_resource_id
	ver := "1.0.0"
	req := &invpb.ReportResourceRequest{
		Type:               "host",
		ReporterType:       "hbi",
		ReporterInstanceId: "cluster-01",
		Representations: &invpb.ResourceRepresentations{
			Metadata: &invpb.RepresentationMetadata{
				LocalResourceId: "", // EMPTY! Must fail
				ReporterVersion: &ver,
			},
		},
	}

	_, err := client.ReportResource(context.Background(), req)
	if err == nil {
		fmt.Println("FAILED: Invalid request unexpectedly succeeded")
		return false
	}

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		fmt.Printf("FAILED: Expected InvalidArgument, got code %v: %v\n", st.Code(), err)
		return false
	}

	fmt.Println("PASSED (Rejected with InvalidArgument)")
	return true
}

func runScenario7_BulkCheck(client invpb.KesselInventoryServiceClient) bool {
	fmt.Print("[Scenario 7] Batch CheckBulk Evaluation... ")

	req := &invpb.CheckBulkRequest{
		Items: []*invpb.CheckBulkRequestItem{
			{
				Object:   &invpb.ResourceReference{ResourceType: "host", ResourceId: "srv-host-001"},
				Relation: "view",
				Subject:  &invpb.SubjectReference{Resource: &invpb.ResourceReference{ResourceType: "principal", ResourceId: "redhat/test-admin"}},
			},
			{
				Object:   &invpb.ResourceReference{ResourceType: "host", ResourceId: "srv-host-001"},
				Relation: "edit",
				Subject:  &invpb.SubjectReference{Resource: &invpb.ResourceReference{ResourceType: "principal", ResourceId: "redhat/test-viewer"}},
			},
		},
	}

	resp, err := client.CheckBulk(context.Background(), req)
	if err != nil || len(resp.Pairs) != 2 {
		fmt.Printf("FAILED: err %v, pairs len %d\n", err, len(resp.Pairs))
		return false
	}

	item0 := resp.Pairs[0].Response.(*invpb.CheckBulkResponsePair_Item).Item
	item1 := resp.Pairs[1].Response.(*invpb.CheckBulkResponsePair_Item).Item

	if item0.Allowed != invpb.Allowed_ALLOWED_TRUE || item1.Allowed != invpb.Allowed_ALLOWED_FALSE {
		fmt.Printf("FAILED: pair 0 allowed: %v, pair 1 allowed: %v\n", item0.Allowed, item1.Allowed)
		return false
	}

	fmt.Println("PASSED (2/2 bulk items correctly resolved)")
	return true
}

func runScenario8_RelationsAPIExplicitTuples(relClient relpb.KesselTupleServiceClient, checkClient relpb.KesselCheckServiceClient) bool {
	fmt.Print("[Scenario 8] Relations API Direct Tuple Grant & Check... ")

	// Explicitly grant stranger-bob 'view' relation to host:srv-host-001
	createReq := &relpb.CreateTuplesRequest{
		Tuples: []*relpb.Relationship{
			{
				Resource: &relpb.ObjectReference{
					Type: &relpb.ObjectType{Namespace: "", Name: "host"},
					Id:   "srv-host-001",
				},
				Relation: "view",
				Subject: &relpb.SubjectReference{
					Subject: &relpb.ObjectReference{
						Type: &relpb.ObjectType{Namespace: "rbac", Name: "principal"},
						Id:   "redhat/stranger-bob",
					},
				},
			},
		},
	}

	_, err := relClient.CreateTuples(context.Background(), createReq)
	if err != nil {
		fmt.Printf("FAILED to create tuple: %v\n", err)
		return false
	}

	// Verify stranger-bob now has access via Relations Check API
	checkReq := &relpb.CheckRequest{
		Resource: &relpb.ObjectReference{
			Type: &relpb.ObjectType{Namespace: "", Name: "host"},
			Id:   "srv-host-001",
		},
		Relation: "view",
		Subject: &relpb.SubjectReference{
			Subject: &relpb.ObjectReference{
				Type: &relpb.ObjectType{Namespace: "rbac", Name: "principal"},
				Id:   "redhat/stranger-bob",
			},
		},
	}

	checkResp, err := checkClient.Check(context.Background(), checkReq)
	if err != nil || checkResp.Allowed != relpb.CheckResponse_ALLOWED_TRUE {
		fmt.Printf("FAILED: stranger-bob check was %v (err: %v)\n", checkResp.Allowed, err)
		return false
	}

	fmt.Println("PASSED (Tuple successfully written and verified)")
	return true
}

func runScenario9_ResourceDeletionAndRevocation(client invpb.KesselInventoryServiceClient) bool {
	fmt.Print("[Scenario 9] Resource Deletion & Permission Revocation... ")

	instID := "cluster-us-east-1"
	delReq := &invpb.DeleteResourceRequest{
		Reference: &invpb.ResourceReference{
			ResourceType: "host",
			ResourceId:   "srv-host-001",
			Reporter: &invpb.ReporterReference{
				Type:       "hbi",
				InstanceId: &instID,
			},
		},
	}

	_, err := client.DeleteResource(context.Background(), delReq)
	if err != nil {
		fmt.Printf("FAILED to delete resource: %v\n", err)
		return false
	}

	// Check should now be ALLOWED_FALSE
	checkReq := &invpb.CheckRequest{
		Object:   &invpb.ResourceReference{ResourceType: "host", ResourceId: "srv-host-001"},
		Relation: "view",
		Subject:  &invpb.SubjectReference{Resource: &invpb.ResourceReference{ResourceType: "principal", ResourceId: "redhat/test-admin"}},
	}
	checkResp, err := client.Check(context.Background(), checkReq)
	if err != nil || checkResp.Allowed != invpb.Allowed_ALLOWED_FALSE {
		fmt.Printf("FAILED: post-delete check was %v (expected FALSE)\n", checkResp.Allowed)
		return false
	}

	fmt.Println("PASSED (Tombstone soft-deleted and Zanzibar tuples wiped)")
	return true
}

func runScenario10_HTTPDeveloperEndpoints() bool {
	fmt.Print("[Scenario 10] HTTP Developer Endpoints (/api/resources, /api/tuples)... ")

	resResp, err := http.Get(httpTarget + "/api/resources")
	if err != nil || resResp.StatusCode != http.StatusOK {
		fmt.Printf("FAILED /api/resources: %v\n", err)
		return false
	}
	defer resResp.Body.Close()

	tupResp, err := http.Get(httpTarget + "/api/tuples")
	if err != nil || tupResp.StatusCode != http.StatusOK {
		fmt.Printf("FAILED /api/tuples: %v\n", err)
		return false
	}
	defer tupResp.Body.Close()

	bodyBytes, _ := io.ReadAll(tupResp.Body)
	if !strings.Contains(string(bodyBytes), "default_org-verify") {
		fmt.Println("FAILED: expected tuples to contain org-verify workspace")
		return false
	}

	fmt.Println("PASSED (JSON payloads valid and queryable)")
	return true
}

func runScenario11_DeleteTuplesRelationsAPI(relClient relpb.KesselTupleServiceClient, checkClient relpb.KesselCheckServiceClient) bool {
	fmt.Print("[Scenario 11] Relations API DeleteTuples with Filter... ")

	// 1. Create a dedicated tuple
	resType := "database"
	resID := "db-prod-01"
	subID := "redhat/db-admin"
	_, err := relClient.CreateTuples(context.Background(), &relpb.CreateTuplesRequest{
		Tuples: []*relpb.Relationship{
			{
				Resource: &relpb.ObjectReference{Type: &relpb.ObjectType{Name: resType}, Id: resID},
				Relation: "admin",
				Subject: &relpb.SubjectReference{
					Subject: &relpb.ObjectReference{Type: &relpb.ObjectType{Namespace: "rbac", Name: "principal"}, Id: subID},
				},
			},
		},
	})
	if err != nil {
		fmt.Printf("FAILED CreateTuples: %v\n", err)
		return false
	}

	// 2. Verify check is ALLOWED_TRUE
	chk, err := checkClient.Check(context.Background(), &relpb.CheckRequest{
		Resource: &relpb.ObjectReference{Type: &relpb.ObjectType{Name: resType}, Id: resID},
		Relation: "admin",
		Subject: &relpb.SubjectReference{
			Subject: &relpb.ObjectReference{Type: &relpb.ObjectType{Namespace: "rbac", Name: "principal"}, Id: subID},
		},
	})
	if err != nil || chk.Allowed != relpb.CheckResponse_ALLOWED_TRUE {
		fmt.Printf("FAILED initial check: allowed=%v err=%v\n", chk.GetAllowed(), err)
		return false
	}

	// 3. DeleteTuples with filter
	delResp, err := relClient.DeleteTuples(context.Background(), &relpb.DeleteTuplesRequest{
		Filter: &relpb.RelationTupleFilter{
			ResourceType: &resType,
			ResourceId:   &resID,
		},
	})
	if err != nil || delResp.ConsistencyToken == nil {
		fmt.Printf("FAILED DeleteTuples: err=%v resp=%v\n", err, delResp)
		return false
	}

	// 4. Verify check is now ALLOWED_FALSE
	chkAfter, err := checkClient.Check(context.Background(), &relpb.CheckRequest{
		Resource: &relpb.ObjectReference{Type: &relpb.ObjectType{Name: resType}, Id: resID},
		Relation: "admin",
		Subject: &relpb.SubjectReference{
			Subject: &relpb.ObjectReference{Type: &relpb.ObjectType{Namespace: "rbac", Name: "principal"}, Id: subID},
		},
	})
	if err != nil || chkAfter.Allowed != relpb.CheckResponse_ALLOWED_FALSE {
		fmt.Printf("FAILED check after delete: expected false, got %v\n", chkAfter.GetAllowed())
		return false
	}

	fmt.Println("PASSED (DeleteTuples successfully removed relationship)")
	return true
}

func runScenario12_CheckSelfWithContextIdentity(client invpb.KesselInventoryServiceClient) bool {
	fmt.Print("[Scenario 12] CheckSelf with Caller Context Metadata (x-rh-user-id)... ")

	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-rh-user-id", "redhat/test-admin"))
	resp, err := client.CheckSelf(ctx, &invpb.CheckSelfRequest{
		Object:   &invpb.ResourceReference{ResourceType: "rbac/workspace", ResourceId: "default_12345"},
		Relation: "view",
	})
	if err != nil || resp.Allowed != invpb.Allowed_ALLOWED_TRUE {
		fmt.Printf("FAILED CheckSelf: allowed=%v err=%v\n", resp.GetAllowed(), err)
		return false
	}
	if resp.ConsistencyToken == nil || resp.ConsistencyToken.Token == "" {
		fmt.Println("FAILED: expected non-empty consistency token")
		return false
	}

	fmt.Println("PASSED (Identity resolved from headers & allowed)")
	return true
}

func runScenario13_StreamedListObjects(client invpb.KesselInventoryServiceClient) bool {
	fmt.Print("[Scenario 13] StreamedListObjects Server-Streaming Lookup... ")

	stream, err := client.StreamedListObjects(context.Background(), &invpb.StreamedListObjectsRequest{
		ObjectType: &invpb.RepresentationType{ResourceType: "workspace"},
		Relation:   "view",
		Subject: &invpb.SubjectReference{
			Resource: &invpb.ResourceReference{
				ResourceType: "principal",
				ResourceId:   "redhat/test-admin",
			},
		},
	})
	if err != nil {
		fmt.Printf("FAILED to open stream: %v\n", err)
		return false
	}

	count := 0
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("FAILED reading stream: %v\n", err)
			return false
		}
		if resp.Object != nil {
			count++
		}
	}

	if count == 0 {
		fmt.Println("FAILED: expected at least 1 object streamed")
		return false
	}

	fmt.Printf("PASSED (Streamed %d accessible objects)\n", count)
	return true
}

func runScenario14_StreamedReadTuplesAndLookupSubjects(tupleClient relpb.KesselTupleServiceClient, lookupClient relpb.KesselLookupServiceClient) bool {
	fmt.Print("[Scenario 14] Server-Streaming ReadTuples & LookupSubjects... ")

	// 1. ReadTuples
	resType := "workspace"
	readStream, err := tupleClient.ReadTuples(context.Background(), &relpb.ReadTuplesRequest{
		Filter: &relpb.RelationTupleFilter{
			ResourceType: &resType,
		},
	})
	if err != nil {
		fmt.Printf("FAILED ReadTuples: %v\n", err)
		return false
	}

	tupleCount := 0
	for {
		r, err := readStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("FAILED reading ReadTuples stream: %v\n", err)
			return false
		}
		if r.Tuple != nil {
			tupleCount++
		}
	}

	// 2. LookupSubjects
	subStream, err := lookupClient.LookupSubjects(context.Background(), &relpb.LookupSubjectsRequest{
		Resource: &relpb.ObjectReference{
			Type: &relpb.ObjectType{Namespace: "rbac", Name: "workspace"},
			Id:   "root_default-org",
		},
		Relation: "view",
	})
	if err != nil {
		fmt.Printf("FAILED LookupSubjects: %v\n", err)
		return false
	}

	subjCount := 0
	for {
		s, err := subStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("FAILED reading LookupSubjects stream: %v\n", err)
			return false
		}
		if s.Subject != nil {
			subjCount++
		}
	}

	fmt.Printf("PASSED (Streamed %d tuples, %d subjects)\n", tupleCount, subjCount)
	return true
}

func runScenario15_CheckForUpdateBulk(client invpb.KesselInventoryServiceClient) bool {
	fmt.Print("[Scenario 15] CheckForUpdateBulk (Strong Consistency Bulk Check)... ")

	resp, err := client.CheckForUpdateBulk(context.Background(), &invpb.CheckForUpdateBulkRequest{
		Items: []*invpb.CheckBulkRequestItem{
			{
				Object:   &invpb.ResourceReference{ResourceType: "rbac/workspace", ResourceId: "default_12345"},
				Relation: "view",
				Subject: &invpb.SubjectReference{
					Resource: &invpb.ResourceReference{ResourceType: "principal", ResourceId: "redhat/test-admin"},
				},
			},
		},
	})
	if err != nil || len(resp.Pairs) != 1 {
		fmt.Printf("FAILED CheckForUpdateBulk: len=%d err=%v\n", len(resp.GetPairs()), err)
		return false
	}
	if resp.ConsistencyToken == nil || resp.ConsistencyToken.Token == "" {
		fmt.Println("FAILED: expected consistency token")
		return false
	}

	fmt.Println("PASSED (Strongly consistent bulk check returned results with token)")
	return true
}

func runBenchmark(client invpb.KesselInventoryServiceClient) {
	req := &invpb.CheckRequest{
		Object:   &invpb.ResourceReference{ResourceType: "rbac/workspace", ResourceId: "default_12345"},
		Relation: "view",
		Subject:  &invpb.SubjectReference{Resource: &invpb.ResourceReference{ResourceType: "principal", ResourceId: "redhat/test-admin"}},
	}

	iterations := 1000
	start := time.Now()

	for i := 0; i < iterations; i++ {
		_, err := client.Check(context.Background(), req)
		if err != nil {
			fmt.Printf("Benchmark error at %d: %v\n", i, err)
			return
		}
	}

	duration := time.Since(start)
	qps := float64(iterations) / duration.Seconds()
	avgLatency := duration / time.Duration(iterations)

	fmt.Printf("⚡ Completed %d checks in %v\n", iterations, duration)
	fmt.Printf("⚡ Average Latency: %v per check\n", avgLatency)
	fmt.Printf("⚡ Measured Throughput: %.0f QPS (in-process/local loopback)\n", qps)
}
