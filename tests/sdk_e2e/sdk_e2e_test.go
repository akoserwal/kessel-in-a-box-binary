package sdk_e2e

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/project-kessel/kessel-sdk-go/kessel/inventory/v1beta2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
)

func getFreePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func ensureKesselBoxBinary(t *testing.T) string {
	wd, err := os.Getwd()
	require.NoError(t, err)

	binPath := filepath.Clean(filepath.Join(wd, "../../bin/kessel-box"))
	if _, err := os.Stat(binPath); err == nil {
		return binPath
	}

	// Compile it if not found
	cmd := exec.Command("go", "build", "-o", binPath, "./cmd/kessel-box")
	cmd.Dir = filepath.Clean(filepath.Join(wd, "../.."))
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "failed to build kessel-box binary: %s", string(out))
	return binPath
}

func TestOfficialKesselSDKGo_WithKesselBox(t *testing.T) {
	binPath := ensureKesselBoxBinary(t)
	grpcPort := getFreePort(t)
	httpPort := getFreePort(t)

	// 1. Launch kessel-box as an independent sub-process
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.CommandContext(ctx, binPath, "start",
		fmt.Sprintf("--grpc-port=%d", grpcPort),
		fmt.Sprintf("--http-port=%d", httpPort),
		"--db-path=:memory:",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	require.NoError(t, err, "failed to start kessel-box process")
	defer func() {
		cancel()
		_ = cmd.Process.Kill()
	}()

	// 2. Poll until gRPC port is ready (max 3 seconds)
	target := fmt.Sprintf("127.0.0.1:%d", grpcPort)
	ready := false
	for i := 0; i < 30; i++ {
		conn, err := net.DialTimeout("tcp", target, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			ready = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.True(t, ready, "kessel-box did not become ready in time on port %d", grpcPort)

	// 3. Connect via the official kessel-sdk-go ClientBuilder
	sdkClient, conn, err := v1beta2.NewClientBuilder(target).Insecure().Build()
	require.NoError(t, err, "failed to construct official kessel-sdk-go client")
	defer conn.Close()

	// 4. Report Resource using official SDK types
	callCtx := context.Background()
	callCtx = metadata.NewOutgoingContext(callCtx, metadata.Pairs("x-rh-rbac-org-id", "sdk-org-e2e"))

	localID := fmt.Sprintf("sdk-host-%d", time.Now().UnixNano()%100000)
	ver := "1.0.0"
	commonMap, err := structpb.NewStruct(map[string]interface{}{
		"workspace_id": "default_sdk-org-e2e",
	})
	require.NoError(t, err)

	reportReq := &v1beta2.ReportResourceRequest{
		Type:               "host",
		ReporterType:       "hbi",
		ReporterInstanceId: "cluster-official-sdk",
		Representations: &v1beta2.ResourceRepresentations{
			Metadata: &v1beta2.RepresentationMetadata{
				LocalResourceId: localID,
				ReporterVersion: &ver,
			},
			Common: commonMap,
		},
	}

	reportResp, err := sdkClient.ReportResource(callCtx, reportReq)
	require.NoError(t, err, "official sdk ReportResource failed")
	require.NotNil(t, reportResp)

	// 5. Immediate Check using official SDK types (verifying 0ms causal consistency)
	checkReq := &v1beta2.CheckRequest{
		Object: &v1beta2.ResourceReference{
			ResourceType: "host",
			ResourceId:   localID,
		},
		Relation: "view",
		Subject: &v1beta2.SubjectReference{
			Resource: &v1beta2.ResourceReference{
				ResourceType: "principal",
				ResourceId:   "redhat/test-admin",
			},
		},
	}

	checkResp, err := sdkClient.Check(callCtx, checkReq)
	require.NoError(t, err, "official sdk Check failed")
	assert.Equal(t, v1beta2.Allowed_ALLOWED_TRUE, checkResp.GetAllowed(), "test-admin must immediately have view permission on reported host")

	// 6. Delete Resource using official SDK types
	delReq := &v1beta2.DeleteResourceRequest{
		Reference: &v1beta2.ResourceReference{
			ResourceType: "host",
			ResourceId:   localID,
			Reporter: &v1beta2.ReporterReference{
				Type:       "hbi",
				InstanceId: &reportReq.ReporterInstanceId,
			},
		},
	}

	_, err = sdkClient.DeleteResource(callCtx, delReq)
	require.NoError(t, err, "official sdk DeleteResource failed")

	// 7. Verify Check after delete is denied
	postDelResp, err := sdkClient.Check(callCtx, checkReq)
	require.NoError(t, err)
	assert.Equal(t, v1beta2.Allowed_ALLOWED_FALSE, postDelResp.GetAllowed(), "permission must be revoked after delete")
}

func TestOfficialKesselSDKGo_WithCausalInterceptor(t *testing.T) {
	binPath := ensureKesselBoxBinary(t)
	grpcPort := getFreePort(t)
	httpPort := getFreePort(t)

	// 1. Launch kessel-box
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.CommandContext(ctx, binPath, "start",
		fmt.Sprintf("--grpc-port=%d", grpcPort),
		fmt.Sprintf("--http-port=%d", httpPort),
		"--db-path=:memory:",
	)
	err := cmd.Start()
	require.NoError(t, err)
	defer func() {
		cancel()
		_ = cmd.Process.Kill()
	}()

	target := fmt.Sprintf("127.0.0.1:%d", grpcPort)
	for i := 0; i < 30; i++ {
		conn, err := net.DialTimeout("tcp", target, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// 2. Instantiate official client with CausalConsistencyInterceptor and Tracker
	tracker := v1beta2.NewCausalTokenTracker()
	builder := v1beta2.NewClientBuilder(target).Insecure()
	sdkClient, conn, err := v1beta2.WithCausalInterceptor(builder, tracker).Build()
	require.NoError(t, err)
	defer conn.Close()

	// 3. Report Resource (write operation)
	callCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-rh-rbac-org-id", "causal-org"))
	localID := fmt.Sprintf("causal-host-%d", time.Now().UnixNano()%100000)
	ver := "1.0.0"
	commonMap, _ := structpb.NewStruct(map[string]interface{}{
		"workspace_id": "default_causal-org",
	})

	reportReq := &v1beta2.ReportResourceRequest{
		Type:               "host",
		ReporterType:       "hbi",
		ReporterInstanceId: "cluster-causal",
		Representations: &v1beta2.ResourceRepresentations{
			Metadata: &v1beta2.RepresentationMetadata{
				LocalResourceId: localID,
				ReporterVersion: &ver,
			},
			Common: commonMap,
		},
	}

	_, err = sdkClient.ReportResource(callCtx, reportReq)
	require.NoError(t, err)

	// 4. Assert tracker automatically captured consistency token from x-kessel-consistency-token header!
	capturedToken := tracker.GetLatestToken()
	assert.NotEmpty(t, capturedToken, "Causal token tracker must automatically capture token from ReportResource header")
	t.Logf("Captured causal consistency token: %s", capturedToken)

	// 5. Check permission with NO explicit consistency specified
	// The CausalUnaryClientInterceptor will automatically inject AtLeastAsFresh with the captured token!
	checkReq := &v1beta2.CheckRequest{
		Object:   &v1beta2.ResourceReference{ResourceType: "host", ResourceId: localID},
		Relation: "view",
		Subject:  &v1beta2.SubjectReference{Resource: &v1beta2.ResourceReference{ResourceType: "principal", ResourceId: "redhat/test-admin"}},
	}

	checkResp, err := sdkClient.Check(callCtx, checkReq)
	require.NoError(t, err)
	assert.Equal(t, v1beta2.Allowed_ALLOWED_TRUE, checkResp.GetAllowed(), "Check must succeed with 100% causal consistency")
}

func TestOfficialKesselSDKGo_ThreeLinesOfCodeExperience(t *testing.T) {
	binPath := ensureKesselBoxBinary(t)
	grpcPort := getFreePort(t)
	httpPort := getFreePort(t)

	// 1. Launch kessel-box
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.CommandContext(ctx, binPath, "start",
		fmt.Sprintf("--grpc-port=%d", grpcPort),
		fmt.Sprintf("--http-port=%d", httpPort),
		"--db-path=:memory:",
	)
	err := cmd.Start()
	require.NoError(t, err)
	defer func() {
		cancel()
		_ = cmd.Process.Kill()
	}()

	target := fmt.Sprintf("127.0.0.1:%d", grpcPort)
	for i := 0; i < 30; i++ {
		conn, err := net.DialTimeout("tcp", target, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// 2. Client configuration: Auto Causal Consistency + Auto Workspace Resolution
	tracker := v1beta2.NewCausalTokenTracker()
	resolver := v1beta2.NewCachedWorkspaceResolver(1 * time.Hour)
	builder := v1beta2.NewClientBuilder(target).Insecure()
	builder = v1beta2.WithCausalInterceptor(builder, tracker)
	builder = v1beta2.WithAutoWorkspace(builder, resolver)
	sdkClient, conn, err := builder.Build()
	require.NoError(t, err)
	defer conn.Close()

	// 3. The 3-Lines-of-Code Developer Experience:
	// A. Strongly typed fluent builder (zero structpb maps, zero manual workspace resolution!)
	localHostID := fmt.Sprintf("srv-devex-%d", time.Now().UnixNano()%10000)
	req, err := v1beta2.NewHostBuilder().
		WithLocalID(localHostID).
		WithAnsibleHost("devex.example.com").
		WithFQDN("devex.example.com").
		Build()
	require.NoError(t, err)

	// B. Single call to ReportResource with caller's org context
	callCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-rh-rbac-org-id", "org-devex-team"))
	_, err = sdkClient.ReportResource(callCtx, req)
	require.NoError(t, err)

	// C. Immediate Check: token was automatically captured and workspace was auto-injected!
	checkResp, err := sdkClient.Check(callCtx, &v1beta2.CheckRequest{
		Object:   &v1beta2.ResourceReference{ResourceType: "host", ResourceId: localHostID},
		Relation: "view",
		Subject:  &v1beta2.SubjectReference{Resource: &v1beta2.ResourceReference{ResourceType: "principal", ResourceId: "redhat/test-admin"}},
	})
	require.NoError(t, err)
	assert.Equal(t, v1beta2.Allowed_ALLOWED_TRUE, checkResp.GetAllowed(), "Zero-friction 3-lines-of-code report must succeed immediately")
}

