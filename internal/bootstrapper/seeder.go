package bootstrapper

import (
	"fmt"
	"sync"

	"github.com/project-kessel/kessel-in-a-box-binary/internal/rebac"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/storage"
)

type Bootstrapper struct {
	engine     *rebac.Engine
	mu         sync.Mutex
	seededOrgs map[string]bool
}

func NewBootstrapper(engine *rebac.Engine) *Bootstrapper {
	b := &Bootstrapper{
		engine:     engine,
		seededOrgs: make(map[string]bool),
	}
	// Seed default global org
	b.EnsureTenantWorkspaces("default-org")
	b.EnsureTenantWorkspaces("12345")
	b.EnsureTenantWorkspaces("redhat")
	return b
}

// EnsureTenantWorkspaces ensures ROOT and DEFAULT workspaces exist for an org
func (b *Bootstrapper) EnsureTenantWorkspaces(orgID string) string {
	b.mu.Lock()
	defer b.mu.Unlock()

	if orgID == "" {
		orgID = "default-org"
	}

	defaultWSID := fmt.Sprintf("default_%s", orgID)
	rootWSID := fmt.Sprintf("root_%s", orgID)

	if b.seededOrgs[orgID] {
		return defaultWSID
	}

	rootWS := rebac.ObjectRef{Namespace: "rbac", Type: "workspace", ID: rootWSID}
	defaultWS := rebac.ObjectRef{Namespace: "rbac", Type: "workspace", ID: defaultWSID}

	// 1. Workspace hierarchy: DEFAULT -> parent -> ROOT
	b.engine.WriteTuple(rebac.Tuple{
		Object:   defaultWS,
		Relation: "parent",
		Subject:  rebac.SubjectRef{ObjectRef: rootWS},
	})

	// 2. Admin Role Binding:
	// workspace -> user_grant -> admin_binding
	// admin_binding -> subject -> principal:redhat/test-admin
	// admin_binding -> granted -> role:admin
	adminBinding := rebac.ObjectRef{Namespace: "rbac", Type: "role_binding", ID: fmt.Sprintf("admin_binding_%s", orgID)}
	adminRole := rebac.ObjectRef{Namespace: "rbac", Type: "role", ID: "admin"}
	testAdminPrincipal := rebac.SubjectRef{
		ObjectRef: rebac.ObjectRef{Namespace: "rbac", Type: "principal", ID: "redhat/test-admin"},
	}

	b.engine.WriteTuple(rebac.Tuple{
		Object:   rootWS,
		Relation: "user_grant",
		Subject:  rebac.SubjectRef{ObjectRef: adminBinding},
	})
	b.engine.WriteTuple(rebac.Tuple{
		Object:   adminBinding,
		Relation: "subject",
		Subject:  testAdminPrincipal,
	})
	b.engine.WriteTuple(rebac.Tuple{
		Object:   adminBinding,
		Relation: "granted",
		Subject:  rebac.SubjectRef{ObjectRef: adminRole},
	})

	// 3. Viewer Role Binding:
	// workspace -> user_grant -> viewer_binding
	// viewer_binding -> subject -> principal:redhat/test-viewer
	// viewer_binding -> granted -> role:viewer
	viewerBinding := rebac.ObjectRef{Namespace: "rbac", Type: "role_binding", ID: fmt.Sprintf("viewer_binding_%s", orgID)}
	viewerRole := rebac.ObjectRef{Namespace: "rbac", Type: "role", ID: "viewer"}
	testViewerPrincipal := rebac.SubjectRef{
		ObjectRef: rebac.ObjectRef{Namespace: "rbac", Type: "principal", ID: "redhat/test-viewer"},
	}

	b.engine.WriteTuple(rebac.Tuple{
		Object:   rootWS,
		Relation: "user_grant",
		Subject:  rebac.SubjectRef{ObjectRef: viewerBinding},
	})
	b.engine.WriteTuple(rebac.Tuple{
		Object:   viewerBinding,
		Relation: "subject",
		Subject:  testViewerPrincipal,
	})
	b.engine.WriteTuple(rebac.Tuple{
		Object:   viewerBinding,
		Relation: "granted",
		Subject:  rebac.SubjectRef{ObjectRef: viewerRole},
	})

	b.seededOrgs[orgID] = true
	return defaultWSID
}

// SeedProfile populates demo resources into storage and ReBAC graph
func (b *Bootstrapper) SeedProfile(profile, orgID string, db *storage.SQLiteStorage) error {
	if orgID == "" {
		orgID = "org-12345"
	}
	defaultWSID := b.EnsureTenantWorkspaces(orgID)

	switch profile {
	case "k8s-demo":
		clusters := []string{"ocp-prod-us-east", "ocp-stage-eu-west"}
		for _, name := range clusters {
			commonPayload := map[string]interface{}{
				"workspace_id": defaultWSID,
			}
			repPayload := map[string]interface{}{
				"cluster_id":   name,
				"display_name": name,
			}
			_, _, err := db.UpsertResource("k8s_cluster", "k8s", name, "k8s-manager-01", defaultWSID, "", commonPayload, repPayload)
			if err != nil {
				return err
			}
			b.engine.WriteTuple(rebac.Tuple{
				Object:   rebac.ObjectRef{Namespace: "rbac", Type: "k8s_cluster", ID: name},
				Relation: "workspace",
				Subject:  rebac.SubjectRef{ObjectRef: rebac.ObjectRef{Namespace: "rbac", Type: "workspace", ID: defaultWSID}},
			})
		}
	default: // "hbi-demo" or default
		hosts := []struct {
			localID string
			fqdn    string
		}{
			{"web-frontend-01", "frontend01.example.com"},
			{"api-backend-02", "api02.example.com"},
			{"db-primary-03", "db03.example.com"},
		}
		for _, h := range hosts {
			commonPayload := map[string]interface{}{
				"workspace_id": defaultWSID,
			}
			repPayload := map[string]interface{}{
				"ansible_host": h.fqdn,
				"fqdn":         h.fqdn,
			}
			_, _, err := db.UpsertResource("host", "hbi", h.localID, "cluster-prod-01", defaultWSID, "", commonPayload, repPayload)
			if err != nil {
				return err
			}
			b.engine.WriteTuple(rebac.Tuple{
				Object:   rebac.ObjectRef{Namespace: "rbac", Type: "host", ID: h.localID},
				Relation: "workspace",
				Subject:  rebac.SubjectRef{ObjectRef: rebac.ObjectRef{Namespace: "rbac", Type: "workspace", ID: defaultWSID}},
			})
		}
	}

	return nil
}
