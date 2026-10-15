package sdk_e2e

import (
	"context"
	"encoding/json"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// TestNewServiceProvider_PermissiveMode tests reporting a completely new resource type
// and new service provider WITHOUT any pre-existing schema definition.
// Kessel-in-a-Box dynamically ingests the resource and wires ReBAC instantly.
func TestNewServiceProvider_PermissiveMode(t *testing.T) {
	binPath := ensureKesselBoxBinary(t)
	grpcPort := getFreePort(t)
	httpPort := getFreePort(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.CommandContext(ctx, binPath, "start",
		fmt.Sprintf("--grpc-port=%d", grpcPort),
		fmt.Sprintf("--http-port=%d", httpPort),
		"--db-path=:memory:",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	defer func() {
		cancel()
		_ = cmd.Process.Kill()
	}()

	target := fmt.Sprintf("127.0.0.1:%d", grpcPort)
	waitForPort(t, target)

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	invClient := v1beta2.NewKesselInventoryServiceClient(conn)

	// New Service Provider: "cost_governance_engine"
	// New Resource Type: "cloud_billing_account"
	payload, err := structpb.NewStruct(map[string]interface{}{
		"workspace_id":   "default_org123",
		"account_number": "AWS-99887766",
		"currency":       "USD",
		"monthly_budget": 50000.0,
	})
	require.NoError(t, err)

	callCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-rh-rbac-org-id", "org123"))

	var headerMD metadata.MD
	reportResp, err := invClient.ReportResource(callCtx, &v1beta2.ReportResourceRequest{
		Type:               "cloud_billing_account",
		ReporterType:       "cost_governance_engine",
		ReporterInstanceId: "prod-us-east-1",
		Representations: &v1beta2.ResourceRepresentations{
			Metadata: &v1beta2.RepresentationMetadata{
				LocalResourceId: "act-001",
			},
			Common: payload,
		},
	}, grpc.Header(&headerMD))

	require.NoError(t, err)
	assert.NotNil(t, reportResp)
	assert.NotEmpty(t, headerMD.Get("x-kessel-consistency-token"), "should return causal consistency token")

	// Immediate ReBAC permission check:
	// Verify user with admin role on default_org123 can view the new cloud_billing_account
	checkResp, err := invClient.Check(callCtx, &v1beta2.CheckRequest{
		Object: &v1beta2.ResourceReference{
			ResourceType: "cloud_billing_account",
			ResourceId:   "act-001",
		},
		Relation: "view",
		Subject: &v1beta2.SubjectReference{
			Resource: &v1beta2.ResourceReference{
				ResourceType: "principal",
				ResourceId:   "redhat/test-admin",
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, v1beta2.Allowed_ALLOWED_TRUE, checkResp.GetAllowed(), "admin user should have view permission on new resource")
}

// TestNewServiceProvider_CustomSchemaDir tests defining a brand new JSON schema in an external
// directory, starting kessel-box with --schemas-dir, and verifying that validation correctly
// passes valid representations and blocks invalid ones.
func TestNewServiceProvider_CustomSchemaDir(t *testing.T) {
	// 1. Create a temporary directory for custom schemas
	tempDir, err := os.MkdirTemp("", "kessel_custom_schemas_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Define a custom JSON schema for new resource "security_scan"
	// Requires "severity" (enum: LOW, MEDIUM, HIGH, CRITICAL) and "vulnerabilities_count" (integer >= 0)
	scanSchema := map[string]interface{}{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title":   "Security Scan Common Representation",
		"type":    "object",
		"required": []string{"severity", "vulnerabilities_count"},
		"properties": map[string]interface{}{
			"severity": map[string]interface{}{
				"type": "string",
				"enum": []string{"LOW", "MEDIUM", "HIGH", "CRITICAL"},
			},
			"vulnerabilities_count": map[string]interface{}{
				"type":    "integer",
				"minimum": 0,
			},
		},
	}
	schemaBytes, err := json.MarshalIndent(scanSchema, "", "  ")
	require.NoError(t, err)

	// Save as security_scan.json (which our flexible validator maps to security_scan/common_representation.json)
	schemaFile := filepath.Join(tempDir, "security_scan.json")
	require.NoError(t, os.WriteFile(schemaFile, schemaBytes, 0644))

	// 2. Launch kessel-box pointing to the custom schemas directory
	binPath := ensureKesselBoxBinary(t)
	grpcPort := getFreePort(t)
	httpPort := getFreePort(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.CommandContext(ctx, binPath, "start",
		fmt.Sprintf("--grpc-port=%d", grpcPort),
		fmt.Sprintf("--http-port=%d", httpPort),
		fmt.Sprintf("--schemas-dir=%s", tempDir),
		"--db-path=:memory:",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	defer func() {
		cancel()
		_ = cmd.Process.Kill()
	}()

	target := fmt.Sprintf("127.0.0.1:%d", grpcPort)
	waitForPort(t, target)

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	invClient := v1beta2.NewKesselInventoryServiceClient(conn)

	// Case A: Report INVALID payload (invalid severity "EXTREME" and negative vulnerabilities_count)
	invalidPayload, err := structpb.NewStruct(map[string]interface{}{
		"severity":              "EXTREME", // Not in enum
		"vulnerabilities_count": -5,        // Below minimum
	})
	require.NoError(t, err)

	_, err = invClient.ReportResource(context.Background(), &v1beta2.ReportResourceRequest{
		Type:               "security_scan",
		ReporterType:       "trivy_scanner",
		ReporterInstanceId: "cluster-alpha",
		Representations: &v1beta2.ResourceRepresentations{
			Metadata: &v1beta2.RepresentationMetadata{
				LocalResourceId: "scan-bad-001",
			},
			Common: invalidPayload,
		},
	})
	require.Error(t, err, "reporting payload violating custom schema must fail")
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "schema validation error for security_scan")

	// Case B: Report VALID payload conforming to custom schema
	validPayload, err := structpb.NewStruct(map[string]interface{}{
		"workspace_id":          "default_org123",
		"severity":              "CRITICAL",
		"vulnerabilities_count": 12,
	})
	require.NoError(t, err)

	reportResp, err := invClient.ReportResource(context.Background(), &v1beta2.ReportResourceRequest{
		Type:               "security_scan",
		ReporterType:       "trivy_scanner",
		ReporterInstanceId: "cluster-alpha",
		Representations: &v1beta2.ResourceRepresentations{
			Metadata: &v1beta2.RepresentationMetadata{
				LocalResourceId: "scan-good-001",
			},
			Common: validPayload,
		},
	})
	require.NoError(t, err, "reporting payload conforming to custom schema must succeed")
	assert.NotNil(t, reportResp)
}

func waitForPort(t *testing.T, target string) {
	for i := 0; i < 30; i++ {
		conn, err := net.DialTimeout("tcp", target, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("target %s did not become ready in time", target)
}
