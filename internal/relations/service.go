package relations

import (
	"context"
	"fmt"
	"io"

	"github.com/google/uuid"
	v1beta1 "github.com/project-kessel/relations-api/api/kessel/relations/v1beta1"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/rebac"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	v1beta1.UnimplementedKesselTupleServiceServer
	v1beta1.UnimplementedKesselCheckServiceServer
	v1beta1.UnimplementedKesselLookupServiceServer

	engine *rebac.Engine
}

func NewService(engine *rebac.Engine) *Service {
	return &Service{
		engine: engine,
	}
}

// CreateTuples creates relationship tuples in the embedded Zanzibar engine
func (s *Service) CreateTuples(ctx context.Context, req *v1beta1.CreateTuplesRequest) (*v1beta1.CreateTuplesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}

	for _, rel := range req.Tuples {
		if rel == nil || rel.Resource == nil || rel.Subject == nil || rel.Subject.Subject == nil {
			continue
		}

		objNamespace := ""
		objType := ""
		if rel.Resource.Type != nil {
			objNamespace = rel.Resource.Type.Namespace
			objType = rel.Resource.Type.Name
		}

		obj := rebac.ObjectRef{
			Namespace: objNamespace,
			Type:      objType,
			ID:        rel.Resource.Id,
		}

		subNamespace := ""
		subType := ""
		if rel.Subject.Subject.Type != nil {
			subNamespace = rel.Subject.Subject.Type.Namespace
			subType = rel.Subject.Subject.Type.Name
		}

		subRel := ""
		if rel.Subject.Relation != nil {
			subRel = *rel.Subject.Relation
		}

		subj := rebac.SubjectRef{
			ObjectRef: rebac.ObjectRef{
				Namespace: subNamespace,
				Type:      subType,
				ID:        rel.Subject.Subject.Id,
			},
			Relation: subRel,
		}

		s.engine.WriteTuple(rebac.Tuple{
			Object:   obj,
			Relation: rel.Relation,
			Subject:  subj,
		})
	}

	token := s.engine.GetLatestToken()
	return &v1beta1.CreateTuplesResponse{
		ConsistencyToken: &v1beta1.ConsistencyToken{Token: token},
	}, nil
}

// DeleteTuples removes relationship tuples from the embedded Zanzibar engine matching filter
func (s *Service) DeleteTuples(ctx context.Context, req *v1beta1.DeleteTuplesRequest) (*v1beta1.DeleteTuplesResponse, error) {
	if req == nil || req.Filter == nil {
		return nil, status.Error(codes.InvalidArgument, "request and filter cannot be nil")
	}

	f := req.Filter
	var subNs, subType, subID, subRel *string
	if f.SubjectFilter != nil {
		subNs = f.SubjectFilter.SubjectNamespace
		subType = f.SubjectFilter.SubjectType
		subID = f.SubjectFilter.SubjectId
		subRel = f.SubjectFilter.Relation
	}

	_ = s.engine.DeleteTuplesByFilter(
		f.ResourceNamespace,
		f.ResourceType,
		f.ResourceId,
		f.Relation,
		subNs,
		subType,
		subID,
		subRel,
	)

	token := s.engine.GetLatestToken()
	return &v1beta1.DeleteTuplesResponse{
		ConsistencyToken: &v1beta1.ConsistencyToken{Token: token},
	}, nil
}

// ReadTuples streams tuples matching the specified filter
func (s *Service) ReadTuples(req *v1beta1.ReadTuplesRequest, stream grpc.ServerStreamingServer[v1beta1.ReadTuplesResponse]) error {
	if req == nil || req.Filter == nil {
		return status.Error(codes.InvalidArgument, "request and filter cannot be nil")
	}

	f := req.Filter
	tuples := s.engine.ListAllTuples()
	token := s.engine.GetLatestToken()

	for _, t := range tuples {
		if f.ResourceNamespace != nil && *f.ResourceNamespace != "" && t.Object.Namespace != *f.ResourceNamespace {
			continue
		}
		if f.ResourceType != nil && *f.ResourceType != "" && t.Object.Type != *f.ResourceType {
			continue
		}
		if f.ResourceId != nil && *f.ResourceId != "" && t.Object.ID != *f.ResourceId {
			continue
		}
		if f.Relation != nil && *f.Relation != "" && t.Relation != *f.Relation {
			continue
		}
		if f.SubjectFilter != nil {
			sf := f.SubjectFilter
			if sf.SubjectNamespace != nil && *sf.SubjectNamespace != "" && t.Subject.Namespace != *sf.SubjectNamespace {
				continue
			}
			if sf.SubjectType != nil && *sf.SubjectType != "" && t.Subject.Type != *sf.SubjectType {
				continue
			}
			if sf.SubjectId != nil && *sf.SubjectId != "" && t.Subject.ID != *sf.SubjectId {
				continue
			}
			if sf.Relation != nil && *sf.Relation != "" && t.Subject.Relation != *sf.Relation {
				continue
			}
		}

		var subRel *string
		if t.Subject.Relation != "" {
			r := t.Subject.Relation
			subRel = &r
		}

		resp := &v1beta1.ReadTuplesResponse{
			Tuple: &v1beta1.Relationship{
				Resource: &v1beta1.ObjectReference{
					Type: &v1beta1.ObjectType{
						Namespace: t.Object.Namespace,
						Name:      t.Object.Type,
					},
					Id: t.Object.ID,
				},
				Relation: t.Relation,
				Subject: &v1beta1.SubjectReference{
					Subject: &v1beta1.ObjectReference{
						Type: &v1beta1.ObjectType{
							Namespace: t.Subject.Namespace,
							Name:      t.Subject.Type,
						},
						Id: t.Subject.ID,
					},
					Relation: subRel,
				},
			},
			ConsistencyToken: &v1beta1.ConsistencyToken{Token: token},
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	return nil
}

// ImportBulkTuples streams tuples in client-side streaming mode
func (s *Service) ImportBulkTuples(stream grpc.ClientStreamingServer[v1beta1.ImportBulkTuplesRequest, v1beta1.ImportBulkTuplesResponse]) error {
	var count uint64
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&v1beta1.ImportBulkTuplesResponse{
				NumImported: count,
			})
		}
		if err != nil {
			return err
		}

		for _, rel := range req.Tuples {
			if rel == nil || rel.Resource == nil || rel.Subject == nil || rel.Subject.Subject == nil {
				continue
			}
			objNs := ""
			objType := ""
			if rel.Resource.Type != nil {
				objNs = rel.Resource.Type.Namespace
				objType = rel.Resource.Type.Name
			}
			subNs := ""
			subType := ""
			if rel.Subject.Subject.Type != nil {
				subNs = rel.Subject.Subject.Type.Namespace
				subType = rel.Subject.Subject.Type.Name
			}
			subRel := ""
			if rel.Subject.Relation != nil {
				subRel = *rel.Subject.Relation
			}

			s.engine.WriteTuple(rebac.Tuple{
				Object: rebac.ObjectRef{
					Namespace: objNs,
					Type:      objType,
					ID:        rel.Resource.Id,
				},
				Relation: rel.Relation,
				Subject: rebac.SubjectRef{
					ObjectRef: rebac.ObjectRef{
						Namespace: subNs,
						Type:      subType,
						ID:        rel.Subject.Subject.Id,
					},
					Relation: subRel,
				},
			})
			count++
		}
	}
}

// AcquireLock provides lock acquisition for fencing checks
func (s *Service) AcquireLock(ctx context.Context, req *v1beta1.AcquireLockRequest) (*v1beta1.AcquireLockResponse, error) {
	if req == nil || req.LockId == "" {
		return nil, status.Error(codes.InvalidArgument, "lock_id is required")
	}
	lockToken := fmt.Sprintf("lock_%s_%s", req.LockId, uuid.NewString()[:8])
	return &v1beta1.AcquireLockResponse{
		LockToken: lockToken,
	}, nil
}

// Check evaluates permission
func (s *Service) Check(ctx context.Context, req *v1beta1.CheckRequest) (*v1beta1.CheckResponse, error) {
	if req == nil || req.Resource == nil || req.Subject == nil || req.Subject.Subject == nil {
		return nil, status.Error(codes.InvalidArgument, "resource and subject are required")
	}

	objNamespace := ""
	objType := ""
	if req.Resource.Type != nil {
		objNamespace = req.Resource.Type.Namespace
		objType = req.Resource.Type.Name
	}
	object := rebac.ObjectRef{
		Namespace: objNamespace,
		Type:      objType,
		ID:        req.Resource.Id,
	}

	subNamespace := ""
	subType := ""
	if req.Subject.Subject.Type != nil {
		subNamespace = req.Subject.Subject.Type.Namespace
		subType = req.Subject.Subject.Type.Name
	}
	subRel := ""
	if req.Subject.Relation != nil {
		subRel = *req.Subject.Relation
	}
	subject := rebac.SubjectRef{
		ObjectRef: rebac.ObjectRef{
			Namespace: subNamespace,
			Type:      subType,
			ID:        req.Subject.Subject.Id,
		},
		Relation: subRel,
	}

	allowed, err := s.engine.CheckPermission(ctx, object, req.Relation, subject)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check failed: %v", err)
	}

	respAllowed := v1beta1.CheckResponse_ALLOWED_FALSE
	if allowed {
		respAllowed = v1beta1.CheckResponse_ALLOWED_TRUE
	}

	token := s.engine.GetLatestToken()
	return &v1beta1.CheckResponse{
		Allowed:          respAllowed,
		ConsistencyToken: &v1beta1.ConsistencyToken{Token: token},
	}, nil
}

// CheckForUpdate evaluates strongly consistent permission
func (s *Service) CheckForUpdate(ctx context.Context, req *v1beta1.CheckForUpdateRequest) (*v1beta1.CheckForUpdateResponse, error) {
	cResp, err := s.Check(ctx, &v1beta1.CheckRequest{
		Resource: req.Resource,
		Relation: req.Relation,
		Subject:  req.Subject,
	})
	if err != nil {
		return nil, err
	}
	return &v1beta1.CheckForUpdateResponse{
		Allowed:          v1beta1.CheckForUpdateResponse_Allowed(cResp.Allowed),
		ConsistencyToken: cResp.ConsistencyToken,
	}, nil
}

// CheckBulk evaluates permissions in bulk
func (s *Service) CheckBulk(ctx context.Context, req *v1beta1.CheckBulkRequest) (*v1beta1.CheckBulkResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}

	var pairs []*v1beta1.CheckBulkResponsePair
	for _, item := range req.Items {
		cResp, err := s.Check(ctx, &v1beta1.CheckRequest{
			Resource: item.Resource,
			Relation: item.Relation,
			Subject:  item.Subject,
		})
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, &v1beta1.CheckBulkResponsePair{
			Request: item,
			Response: &v1beta1.CheckBulkResponsePair_Item{
				Item: &v1beta1.CheckBulkResponseItem{
					Allowed: v1beta1.CheckBulkResponseItem_Allowed(cResp.Allowed),
				},
			},
		})
	}

	token := s.engine.GetLatestToken()
	return &v1beta1.CheckBulkResponse{
		Pairs:            pairs,
		ConsistencyToken: &v1beta1.ConsistencyToken{Token: token},
	}, nil
}

// CheckForUpdateBulk evaluates strong consistency bulk checks
func (s *Service) CheckForUpdateBulk(ctx context.Context, req *v1beta1.CheckForUpdateBulkRequest) (*v1beta1.CheckForUpdateBulkResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}

	var pairs []*v1beta1.CheckBulkResponsePair
	for _, item := range req.Items {
		cResp, err := s.Check(ctx, &v1beta1.CheckRequest{
			Resource: item.Resource,
			Relation: item.Relation,
			Subject:  item.Subject,
		})
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, &v1beta1.CheckBulkResponsePair{
			Request: item,
			Response: &v1beta1.CheckBulkResponsePair_Item{
				Item: &v1beta1.CheckBulkResponseItem{
					Allowed: v1beta1.CheckBulkResponseItem_Allowed(cResp.Allowed),
				},
			},
		})
	}

	token := s.engine.GetLatestToken()
	return &v1beta1.CheckForUpdateBulkResponse{
		Pairs:            pairs,
		ConsistencyToken: &v1beta1.ConsistencyToken{Token: token},
	}, nil
}

// LookupResources streams matching resource IDs
func (s *Service) LookupResources(req *v1beta1.LookupResourcesRequest, stream grpc.ServerStreamingServer[v1beta1.LookupResourcesResponse]) error {
	if req == nil || req.ResourceType == nil || req.Subject == nil || req.Subject.Subject == nil {
		return status.Error(codes.InvalidArgument, "resource_type and subject are required")
	}

	subNamespace := ""
	subType := ""
	if req.Subject.Subject.Type != nil {
		subNamespace = req.Subject.Subject.Type.Namespace
		subType = req.Subject.Subject.Type.Name
	}
	subject := rebac.SubjectRef{
		ObjectRef: rebac.ObjectRef{
			Namespace: subNamespace,
			Type:      subType,
			ID:        req.Subject.Subject.Id,
		},
	}

	ids, err := s.engine.LookupResources(stream.Context(), req.ResourceType.Name, req.Relation, subject)
	if err != nil {
		return status.Errorf(codes.Internal, "lookup failed: %v", err)
	}

	token := s.engine.GetLatestToken()
	for _, id := range ids {
		err := stream.Send(&v1beta1.LookupResourcesResponse{
			Resource: &v1beta1.ObjectReference{
				Type: req.ResourceType,
				Id:   id,
			},
			ConsistencyToken: &v1beta1.ConsistencyToken{Token: token},
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// LookupSubjects streams subjects who have permission on resource
func (s *Service) LookupSubjects(req *v1beta1.LookupSubjectsRequest, stream grpc.ServerStreamingServer[v1beta1.LookupSubjectsResponse]) error {
	if req == nil || req.Resource == nil || req.Relation == "" {
		return status.Error(codes.InvalidArgument, "resource and relation are required")
	}

	resNs := ""
	resType := ""
	if req.Resource.Type != nil {
		resNs = req.Resource.Type.Namespace
		resType = req.Resource.Type.Name
	}
	object := rebac.ObjectRef{
		Namespace: resNs,
		Type:      resType,
		ID:        req.Resource.Id,
	}

	subjectType := ""
	if req.SubjectType != nil {
		subjectType = req.SubjectType.Name
	}

	subjects, err := s.engine.LookupSubjects(stream.Context(), object, req.Relation, subjectType)
	if err != nil {
		return status.Errorf(codes.Internal, "lookup subjects failed: %v", err)
	}

	token := s.engine.GetLatestToken()
	for _, subj := range subjects {
		var subRel *string
		if subj.Relation != "" {
			r := subj.Relation
			subRel = &r
		}

		err := stream.Send(&v1beta1.LookupSubjectsResponse{
			Subject: &v1beta1.SubjectReference{
				Subject: &v1beta1.ObjectReference{
					Type: &v1beta1.ObjectType{
						Namespace: subj.Namespace,
						Name:      subj.Type,
					},
					Id: subj.ID,
				},
				Relation: subRel,
			},
			ConsistencyToken: &v1beta1.ConsistencyToken{Token: token},
		})
		if err != nil {
			return err
		}
	}

	return nil
}
