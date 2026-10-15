package inventory

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"

	v1beta2 "github.com/project-kessel/inventory-api/api/kessel/inventory/v1beta2"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/bootstrapper"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/rebac"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/schema"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Service struct {
	v1beta2.UnimplementedKesselInventoryServiceServer

	storage      *storage.SQLiteStorage
	engine       *rebac.Engine
	validator    *schema.Validator
	bootstrapper *bootstrapper.Bootstrapper
	cdcDelay     time.Duration
}

func NewService(
	storage *storage.SQLiteStorage,
	engine *rebac.Engine,
	validator *schema.Validator,
	bootstrapper *bootstrapper.Bootstrapper,
) *Service {
	return &Service{
		storage:      storage,
		engine:       engine,
		validator:    validator,
		bootstrapper: bootstrapper,
	}
}

func (s *Service) SetCDCDelay(delay time.Duration) {
	s.cdcDelay = delay
}

func (s *Service) ReportResource(ctx context.Context, req *v1beta2.ReportResourceRequest) (*v1beta2.ReportResourceResponse, error) {
	if req == nil || req.Representations == nil || req.Representations.Metadata == nil {
		return nil, status.Error(codes.InvalidArgument, "representations and representations.metadata are required")
	}

	resType := req.Type
	reporterType := req.ReporterType
	reporterInstanceID := req.ReporterInstanceId
	localID := req.Representations.Metadata.LocalResourceId

	if resType == "" || reporterType == "" || localID == "" {
		return nil, status.Error(codes.InvalidArgument, "type, reporter_type, and representations.metadata.local_resource_id are required")
	}

	var commonMap map[string]interface{}
	if req.Representations.Common != nil {
		commonMap = req.Representations.Common.AsMap()
	} else {
		commonMap = make(map[string]interface{})
	}

	var reporterMap map[string]interface{}
	if req.Representations.Reporter != nil {
		reporterMap = req.Representations.Reporter.AsMap()
	} else {
		reporterMap = make(map[string]interface{})
	}

	// 1. Resolve or Auto-Bootstrap Workspace
	orgID := "default-org"
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("x-rh-rbac-org-id"); len(vals) > 0 && vals[0] != "" {
			orgID = vals[0]
		}
	}
	defaultWSID := s.bootstrapper.EnsureTenantWorkspaces(orgID)

	workspaceID, _ := commonMap["workspace_id"].(string)
	if workspaceID == "" {
		workspaceID = defaultWSID
		commonMap["workspace_id"] = workspaceID
	}

	// 2. Validate against embedded JSON Schemas
	if err := s.validator.ValidateCommon(resType, commonMap); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "common representation schema validation failed: %v", err)
	}
	if err := s.validator.ValidateReporter(resType, reporterType, reporterMap); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "reporter representation schema validation failed: %v", err)
	}

	// 3. Extract transaction_id for idempotency
	var transactionID string
	if tid, ok := commonMap["transaction_id"].(string); ok {
		transactionID = tid
	}

	// Extract resource metadata
	var meta storage.ResourceMetadata
	if req.Representations.Metadata != nil {
		meta.ApiHref = req.Representations.Metadata.ApiHref
		if req.Representations.Metadata.ConsoleHref != nil {
			meta.ConsoleHref = *req.Representations.Metadata.ConsoleHref
		}
		if req.Representations.Metadata.ReporterVersion != nil {
			meta.ReporterVersion = *req.Representations.Metadata.ReporterVersion
		}
	}

	// 4. Save to SQLite Storage
	resourceID, token, err := s.storage.UpsertResource(
		resType, reporterType, localID, reporterInstanceID,
		workspaceID, transactionID,
		commonMap, reporterMap,
		meta,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to persist resource: %v", err)
	}

	// 5. WRITE-THROUGH TO EMBEDDED ZANZIBAR ENGINE
	writeTuples := func() {
		targetWS := rebac.ObjectRef{Namespace: "rbac", Type: "workspace", ID: workspaceID}

		// Link by local resource ID
		s.engine.WriteTuple(rebac.Tuple{
			Object:   rebac.ObjectRef{Type: resType, ID: localID},
			Relation: "workspace",
			Subject:  rebac.SubjectRef{ObjectRef: targetWS},
		})

		// Link by canonical UUID
		s.engine.WriteTuple(rebac.Tuple{
			Object:   rebac.ObjectRef{Type: resType, ID: resourceID},
			Relation: "workspace",
			Subject:  rebac.SubjectRef{ObjectRef: targetWS},
		})
	}

	if s.cdcDelay > 0 && req.WriteVisibility == v1beta2.WriteVisibility_MINIMIZE_LATENCY {
		time.AfterFunc(s.cdcDelay, writeTuples)
	} else {
		writeTuples()
	}

	// 6. Return ZedToken and Resource ID via gRPC response headers
	md := metadata.Pairs(
		"x-kessel-consistency-token", token,
		"x-kessel-resource-id", resourceID,
	)
	_ = grpc.SetHeader(ctx, md)

	return &v1beta2.ReportResourceResponse{}, nil
}

func (s *Service) Check(ctx context.Context, req *v1beta2.CheckRequest) (*v1beta2.CheckResponse, error) {
	if req == nil || req.Object == nil || req.Subject == nil || req.Subject.Resource == nil {
		return nil, status.Error(codes.InvalidArgument, "object and subject.resource are required")
	}

	object := rebac.ObjectRef{
		Type: req.Object.ResourceType,
		ID:   req.Object.ResourceId,
	}

	subType := req.Subject.Resource.ResourceType
	var subNamespace string
	if subType == "principal" || subType == "user" || subType == "group" {
		subNamespace = "rbac"
	}

	subject := rebac.SubjectRef{
		ObjectRef: rebac.ObjectRef{
			Namespace: subNamespace,
			Type:      subType,
			ID:        req.Subject.Resource.ResourceId,
		},
		Relation: req.Subject.GetRelation(),
	}

	// Process Consistency parameter
	if req.Consistency != nil {
		if req.Consistency.GetAtLeastAsAcknowledged() {
			// Query stored KTN from database
			_, _ = s.storage.GetResourceKTN(req.Object.ResourceType, req.Object.ResourceId)
		}
	}

	allowed, err := s.engine.CheckPermission(ctx, object, req.Relation, subject)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "permission evaluation failed: %v", err)
	}

	respAllowed := v1beta2.Allowed_ALLOWED_FALSE
	if allowed {
		respAllowed = v1beta2.Allowed_ALLOWED_TRUE
	}

	token := s.engine.GetLatestToken()
	return &v1beta2.CheckResponse{
		Allowed:          respAllowed,
		ConsistencyToken: &v1beta2.ConsistencyToken{Token: token},
	}, nil
}

func (s *Service) CheckForUpdate(ctx context.Context, req *v1beta2.CheckForUpdateRequest) (*v1beta2.CheckForUpdateResponse, error) {
	if req == nil || req.Object == nil || req.Subject == nil || req.Subject.Resource == nil {
		return nil, status.Error(codes.InvalidArgument, "object and subject.resource are required")
	}

	object := rebac.ObjectRef{
		Type: req.Object.ResourceType,
		ID:   req.Object.ResourceId,
	}

	subType := req.Subject.Resource.ResourceType
	var subNamespace string
	if subType == "principal" || subType == "user" || subType == "group" {
		subNamespace = "rbac"
	}

	subject := rebac.SubjectRef{
		ObjectRef: rebac.ObjectRef{
			Namespace: subNamespace,
			Type:      subType,
			ID:        req.Subject.Resource.ResourceId,
		},
		Relation: req.Subject.GetRelation(),
	}

	allowed, err := s.engine.CheckPermission(ctx, object, req.Relation, subject)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "permission evaluation failed: %v", err)
	}

	respAllowed := v1beta2.Allowed_ALLOWED_FALSE
	if allowed {
		respAllowed = v1beta2.Allowed_ALLOWED_TRUE
	}

	token := s.engine.GetLatestToken()
	return &v1beta2.CheckForUpdateResponse{
		Allowed:          respAllowed,
		ConsistencyToken: &v1beta2.ConsistencyToken{Token: token},
	}, nil
}

func (s *Service) CheckBulk(ctx context.Context, req *v1beta2.CheckBulkRequest) (*v1beta2.CheckBulkResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}

	var pairs []*v1beta2.CheckBulkResponsePair
	for _, item := range req.Items {
		cResp, err := s.Check(ctx, &v1beta2.CheckRequest{
			Object:      item.Object,
			Relation:    item.Relation,
			Subject:     item.Subject,
			Consistency: req.Consistency,
		})
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, &v1beta2.CheckBulkResponsePair{
			Request: item,
			Response: &v1beta2.CheckBulkResponsePair_Item{
				Item: &v1beta2.CheckBulkResponseItem{
					Allowed: cResp.Allowed,
				},
			},
		})
	}

	token := s.engine.GetLatestToken()
	return &v1beta2.CheckBulkResponse{
		Pairs:            pairs,
		ConsistencyToken: &v1beta2.ConsistencyToken{Token: token},
	}, nil
}

func (s *Service) CheckForUpdateBulk(ctx context.Context, req *v1beta2.CheckForUpdateBulkRequest) (*v1beta2.CheckForUpdateBulkResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}

	var pairs []*v1beta2.CheckForUpdateBulkResponsePair
	for _, item := range req.Items {
		cResp, err := s.CheckForUpdate(ctx, &v1beta2.CheckForUpdateRequest{
			Object:   item.Object,
			Relation: item.Relation,
			Subject:  item.Subject,
		})
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, &v1beta2.CheckForUpdateBulkResponsePair{
			Request: item,
			Response: &v1beta2.CheckForUpdateBulkResponsePair_Item{
				Item: &v1beta2.CheckForUpdateBulkResponseItem{
					Allowed: cResp.Allowed,
				},
			},
		})
	}

	token := s.engine.GetLatestToken()
	return &v1beta2.CheckForUpdateBulkResponse{
		Pairs:            pairs,
		ConsistencyToken: &v1beta2.ConsistencyToken{Token: token},
	}, nil
}

func (s *Service) resolvePrincipal(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("x-rh-user-id"); len(vals) > 0 && vals[0] != "" {
			return vals[0]
		}
		if vals := md.Get("x-rh-identity"); len(vals) > 0 && vals[0] != "" {
			raw := vals[0]
			if decoded, err := base64.StdEncoding.DecodeString(raw); err == nil {
				raw = string(decoded)
			}
			var idMap map[string]interface{}
			if err := json.Unmarshal([]byte(raw), &idMap); err == nil {
				if idObj, ok := idMap["identity"].(map[string]interface{}); ok {
					if userObj, ok := idObj["user"].(map[string]interface{}); ok {
						if uname, ok := userObj["username"].(string); ok && uname != "" {
							return uname
						}
					}
				}
			}
		}
	}
	return "redhat/test-user"
}

func (s *Service) CheckSelf(ctx context.Context, req *v1beta2.CheckSelfRequest) (*v1beta2.CheckSelfResponse, error) {
	if req == nil || req.Object == nil {
		return nil, status.Error(codes.InvalidArgument, "object is required")
	}

	principal := s.resolvePrincipal(ctx)
	checkReq := &v1beta2.CheckRequest{
		Object:   req.Object,
		Relation: req.Relation,
		Subject: &v1beta2.SubjectReference{
			Resource: &v1beta2.ResourceReference{
				ResourceType: "principal",
				ResourceId:   principal,
			},
		},
		Consistency: req.Consistency,
	}

	resp, err := s.Check(ctx, checkReq)
	if err != nil {
		return nil, err
	}

	return &v1beta2.CheckSelfResponse{
		Allowed:          resp.Allowed,
		ConsistencyToken: resp.ConsistencyToken,
	}, nil
}

func (s *Service) CheckSelfBulk(ctx context.Context, req *v1beta2.CheckSelfBulkRequest) (*v1beta2.CheckSelfBulkResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}

	principal := s.resolvePrincipal(ctx)
	var pairs []*v1beta2.CheckSelfBulkResponsePair
	for _, item := range req.Items {
		cResp, err := s.Check(ctx, &v1beta2.CheckRequest{
			Object:   item.Object,
			Relation: item.Relation,
			Subject: &v1beta2.SubjectReference{
				Resource: &v1beta2.ResourceReference{
					ResourceType: "principal",
					ResourceId:   principal,
				},
			},
			Consistency: req.Consistency,
		})
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, &v1beta2.CheckSelfBulkResponsePair{
			Request: item,
			Response: &v1beta2.CheckSelfBulkResponsePair_Item{
				Item: &v1beta2.CheckSelfBulkResponseItem{
					Allowed: cResp.Allowed,
				},
			},
		})
	}

	token := s.engine.GetLatestToken()
	return &v1beta2.CheckSelfBulkResponse{
		Pairs:            pairs,
		ConsistencyToken: &v1beta2.ConsistencyToken{Token: token},
	}, nil
}

func (s *Service) StreamedListObjects(req *v1beta2.StreamedListObjectsRequest, stream grpc.ServerStreamingServer[v1beta2.StreamedListObjectsResponse]) error {
	if req == nil || req.ObjectType == nil || req.Subject == nil || req.Subject.Resource == nil {
		return status.Error(codes.InvalidArgument, "object_type and subject are required")
	}

	subType := req.Subject.Resource.ResourceType
	var subNs string
	if subType == "principal" || subType == "user" || subType == "group" {
		subNs = "rbac"
	}
	subject := rebac.SubjectRef{
		ObjectRef: rebac.ObjectRef{
			Namespace: subNs,
			Type:      subType,
			ID:        req.Subject.Resource.ResourceId,
		},
		Relation: req.Subject.GetRelation(),
	}

	ids, err := s.engine.LookupResources(stream.Context(), req.ObjectType.ResourceType, req.Relation, subject)
	if err != nil {
		return status.Errorf(codes.Internal, "streamed list objects failed: %v", err)
	}

	token := s.engine.GetLatestToken()
	for _, id := range ids {
		err := stream.Send(&v1beta2.StreamedListObjectsResponse{
			Object: &v1beta2.ResourceReference{
				ResourceType: req.ObjectType.ResourceType,
				ResourceId:   id,
			},
			ConsistencyToken: &v1beta2.ConsistencyToken{Token: token},
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) StreamedListSubjects(req *v1beta2.StreamedListSubjectsRequest, stream grpc.ServerStreamingServer[v1beta2.StreamedListSubjectsResponse]) error {
	if req == nil || req.Resource == nil || req.Relation == "" {
		return status.Error(codes.InvalidArgument, "resource and relation are required")
	}

	object := rebac.ObjectRef{
		Type: req.Resource.ResourceType,
		ID:   req.Resource.ResourceId,
	}

	subjectType := ""
	if req.SubjectType != nil {
		subjectType = req.SubjectType.ResourceType
	}

	subjects, err := s.engine.LookupSubjects(stream.Context(), object, req.Relation, subjectType)
	if err != nil {
		return status.Errorf(codes.Internal, "streamed list subjects failed: %v", err)
	}

	token := s.engine.GetLatestToken()
	for _, subj := range subjects {
		var subRel *string
		if subj.Relation != "" {
			r := subj.Relation
			subRel = &r
		}

		err := stream.Send(&v1beta2.StreamedListSubjectsResponse{
			Subject: &v1beta2.SubjectReference{
				Resource: &v1beta2.ResourceReference{
					ResourceType: subj.Type,
					ResourceId:   subj.ID,
				},
				Relation: subRel,
			},
			ConsistencyToken: &v1beta2.ConsistencyToken{Token: token},
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) DeleteResource(ctx context.Context, req *v1beta2.DeleteResourceRequest) (*v1beta2.DeleteResourceResponse, error) {
	if req == nil || req.Reference == nil || req.Reference.Reporter == nil {
		return nil, status.Error(codes.InvalidArgument, "reference and reference.reporter are required")
	}

	resType := req.Reference.ResourceType
	localID := req.Reference.ResourceId
	reporterType := req.Reference.Reporter.Type
	var reporterInstanceID string
	if req.Reference.Reporter.InstanceId != nil {
		reporterInstanceID = *req.Reference.Reporter.InstanceId
	}

	resourceID, err := s.storage.SoftDeleteResource(reporterType, localID, resType, reporterInstanceID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil, status.Error(codes.NotFound, "resource not found")
		}
		return nil, status.Errorf(codes.Internal, "failed to delete resource: %v", err)
	}

	// Remove Zanzibar tuples
	s.engine.DeleteTuplesForResource(resType, localID)
	s.engine.DeleteTuplesForResource(resType, resourceID)

	return &v1beta2.DeleteResourceResponse{}, nil
}
