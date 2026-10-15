package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	invpb "github.com/project-kessel/inventory-api/api/kessel/inventory/v1beta2"
	relpb "github.com/project-kessel/relations-api/api/kessel/relations/v1beta1"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/inventory"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/rebac"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/relations"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	grpcPort int
	httpPort int

	storage  *storage.SQLiteStorage
	engine   *rebac.Engine
	invSvc   *inventory.Service
	relSvc   *relations.Service

	grpcServer *grpc.Server
	httpServer *http.Server
	wg         sync.WaitGroup
}

func NewServer(
	grpcPort, httpPort int,
	storage *storage.SQLiteStorage,
	engine *rebac.Engine,
	invSvc *inventory.Service,
	relSvc *relations.Service,
) *Server {
	return &Server{
		grpcPort: grpcPort,
		httpPort: httpPort,
		storage:  storage,
		engine:   engine,
		invSvc:   invSvc,
		relSvc:   relSvc,
	}
}

func (s *Server) Start() error {
	// 1. Start gRPC Server
	grpcLis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.grpcPort))
	if err != nil {
		return fmt.Errorf("failed to listen on gRPC port %d: %w", s.grpcPort, err)
	}

	s.grpcServer = grpc.NewServer()
	invpb.RegisterKesselInventoryServiceServer(s.grpcServer, s.invSvc)
	relpb.RegisterKesselTupleServiceServer(s.grpcServer, s.relSvc)
	relpb.RegisterKesselCheckServiceServer(s.grpcServer, s.relSvc)
	relpb.RegisterKesselLookupServiceServer(s.grpcServer, s.relSvc)
	reflection.Register(s.grpcServer)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		fmt.Printf("🚀 [kessel-box] gRPC server listening on :%d (reflection enabled)\n", s.grpcPort)
		if err := s.grpcServer.Serve(grpcLis); err != nil && err != grpc.ErrServerStopped {
			fmt.Printf("gRPC server error: %v\n", err)
		}
	}()

	// 2. Start HTTP Gateway & Developer UI
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/resources", s.handleResources)
	mux.HandleFunc("/api/tuples", s.handleTuples)
	mux.HandleFunc("/api/check", s.handleCheck)
	mux.HandleFunc("/", s.handleIndex)

	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.httpPort),
		Handler: mux,
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		fmt.Printf("🌐 [kessel-box] HTTP Developer UI & API listening on http://localhost:%d\n", s.httpPort)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("HTTP server error: %v\n", err)
		}
	}()

	return nil
}

func (s *Server) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.httpServer.Shutdown(ctx)
	}
	s.wg.Wait()
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok","server":"kessel-in-a-box"}`))
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	resources, _ := s.storage.ListResources(100)
	tuples := s.engine.ListAllTuples()

	resp := map[string]interface{}{
		"status":          "healthy",
		"mode":            "kessel-in-a-box",
		"total_resources": len(resources),
		"total_tuples":    len(tuples),
		"timestamp":       time.Now().UTC(),
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleResources(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	wsID := r.URL.Query().Get("workspace_id")
	var resources []storage.ResourceRecord
	var err error
	if wsID != "" {
		resources, err = s.storage.ListResourcesByWorkspace(wsID, 200)
	} else {
		resources, err = s.storage.ListResources(200)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(resources)
}

func (s *Server) handleTuples(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	tuples := s.engine.ListAllTuples()
	_ = json.NewEncoder(w).Encode(tuples)
}

type CheckJSONRequest struct {
	Object   string `json:"object"`
	Relation string `json:"relation"`
	Subject  string `json:"subject"`
}

func (s *Server) handleCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req CheckJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	obj := rebac.ParseObjectRef(req.Object)
	subj := rebac.ParseSubjectRef(req.Subject)

	allowed, err := s.engine.CheckPermission(r.Context(), obj, req.Relation, subj)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"allowed":  allowed,
		"object":   obj.String(),
		"relation": req.Relation,
		"subject":  subj.String(),
	})
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	html := `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Kessel-in-a-Box Developer Console</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.28.1/cytoscape.min.js"></script>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 0; padding: 20px; background: #0f172a; color: #f8fafc; }
        h1, h2, h3 { color: #38bdf8; margin-top: 0; }
        .card { background: #1e293b; border-radius: 8px; padding: 20px; margin-bottom: 20px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); border: 1px solid #334155; }
        .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { text-align: left; padding: 10px; border-bottom: 1px solid #334155; font-size: 13px; }
        th { color: #94a3b8; }
        input, select, button { padding: 10px; border-radius: 6px; border: 1px solid #475569; background: #0f172a; color: #fff; font-size: 14px; }
        button { background: #0284c7; cursor: pointer; border: none; font-weight: bold; }
        button:hover { background: #0369a1; }
        .badge { background: #3b82f6; color: #fff; padding: 3px 6px; border-radius: 4px; font-size: 11px; }
        #cy { width: 100%; height: 420px; background: #090d16; border-radius: 6px; border: 1px solid #334155; }
    </style>
</head>
<body>
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:20px;">
        <div>
            <h1>⚡ Kessel-in-a-Box Developer Console</h1>
            <p style="color:#94a3b8; margin:0;">Zero-dependency all-in-one Project Kessel local engine (Inventory v1beta2 + Relations v1beta1 + Embedded Zanzibar)</p>
        </div>
        <div>
            <span style="background:#10b981; color:#fff; padding:6px 12px; border-radius:20px; font-size:13px; font-weight:bold;">● In-Memory & Synchronous</span>
        </div>
    </div>

    <div class="card">
        <h2>Interactive Zanzibar Relationship Graph Visualizer</h2>
        <div id="cy"></div>
    </div>

    <div class="grid">
        <div class="card">
            <h2>Live Permission Checker</h2>
            <form id="checkForm">
                <div style="display:flex; flex-direction:column; gap:12px;">
                    <div>
                        <label style="font-size:13px; color:#94a3b8;">Object Reference:</label>
                        <input type="text" id="obj" value="rbac/workspace:default_12345" style="width:100%; box-sizing:border-box; margin-top:4px;" required />
                    </div>
                    <div>
                        <label style="font-size:13px; color:#94a3b8;">Relation / Verb:</label>
                        <input type="text" id="rel" value="view" style="width:100%; box-sizing:border-box; margin-top:4px;" required />
                    </div>
                    <div>
                        <label style="font-size:13px; color:#94a3b8;">Subject Reference:</label>
                        <input type="text" id="subj" value="rbac/principal:redhat/test-admin" style="width:100%; box-sizing:border-box; margin-top:4px;" required />
                    </div>
                    <button type="submit">Evaluate Permission</button>
                </div>
            </form>
            <div id="checkResult" style="margin-top:15px; font-weight:bold; font-size:16px;"></div>
        </div>

        <div class="card" style="max-height:450px; overflow-y:auto;">
            <h2>Active Relationship Tuples</h2>
            <table id="tuplesTable">
                <thead><tr><th>Object</th><th>Relation</th><th>Subject</th></tr></thead>
                <tbody></tbody>
            </table>
        </div>
    </div>

    <div class="card" style="max-height:300px; overflow-y:auto;">
        <h2>Registered Inventory Resources</h2>
        <table id="resourcesTable">
            <thead><tr><th>UUID</th><th>Type</th><th>Created At</th></tr></thead>
            <tbody></tbody>
        </table>
    </div>

    <script>
        let cy = null;

        function initCy() {
            if (typeof cytoscape === 'undefined') return;
            cy = cytoscape({
                container: document.getElementById('cy'),
                style: [
                    {
                        selector: 'node',
                        style: {
                            'label': 'data(label)',
                            'color': '#f8fafc',
                            'font-size': '11px',
                            'text-valign': 'bottom',
                            'text-margin-y': '5px',
                            'background-color': '#0284c7',
                            'width': '30px',
                            'height': '30px'
                        }
                    },
                    {
                        selector: 'node[type="workspace"]',
                        style: { 'background-color': '#38bdf8', 'shape': 'rectangle', 'width': '36px', 'height': '36px' }
                    },
                    {
                        selector: 'node[type="role_binding"]',
                        style: { 'background-color': '#f59e0b', 'shape': 'hexagon', 'width': '32px', 'height': '32px' }
                    },
                    {
                        selector: 'node[type="role"]',
                        style: { 'background-color': '#a855f7', 'shape': 'diamond', 'width': '34px', 'height': '34px' }
                    },
                    {
                        selector: 'node[type="principal"]',
                        style: { 'background-color': '#10b981', 'shape': 'ellipse', 'width': '32px', 'height': '32px' }
                    },
                    {
                        selector: 'edge',
                        style: {
                            'width': 2,
                            'line-color': '#475569',
                            'target-arrow-color': '#475569',
                            'target-arrow-shape': 'triangle',
                            'curve-style': 'bezier',
                            'label': 'data(label)',
                            'font-size': '10px',
                            'color': '#94a3b8',
                            'text-background-color': '#0f172a',
                            'text-background-opacity': 0.8
                        }
                    },
                    {
                        selector: '.highlighted',
                        style: {
                            'background-color': '#10b981',
                            'line-color': '#10b981',
                            'target-arrow-color': '#10b981',
                            'width': 3
                        }
                    }
                ],
                layout: { name: 'cose', animate: false }
            });
        }

        async function loadData() {
            try {
                const tupRes = await fetch('/api/tuples');
                const tuples = await tupRes.json();
                const tBody = document.querySelector('#tuplesTable tbody');
                tBody.innerHTML = (tuples || []).map(t => {
                    const objStr = (t.object.namespace ? t.object.namespace + '/' : '') + t.object.type + ':' + t.object.id;
                    const subStr = (t.subject.namespace ? t.subject.namespace + '/' : '') + t.subject.type + ':' + t.subject.id + (t.subject.relation ? '#' + t.subject.relation : '');
                    return '<tr><td>' + objStr + '</td><td><span class="badge">' + t.relation + '</span></td><td>' + subStr + '</td></tr>';
                }).join('');

                const resRes = await fetch('/api/resources');
                const resources = await resRes.json();
                const rBody = document.querySelector('#resourcesTable tbody');
                rBody.innerHTML = (resources || []).map(r => '<tr><td><code>' + r.id + '</code></td><td>' + r.type + '</td><td>' + new Date(r.created_at).toLocaleTimeString() + '</td></tr>').join('');

                if (cy && tuples) {
                    const elements = [];
                    const addedNodes = new Set();

                    tuples.forEach((t, i) => {
                        const objId = (t.object.namespace ? t.object.namespace + '/' : '') + t.object.type + ':' + t.object.id;
                        const subId = (t.subject.namespace ? t.subject.namespace + '/' : '') + t.subject.type + ':' + t.subject.id;

                        if (!addedNodes.has(objId)) {
                            addedNodes.add(objId);
                            elements.push({ data: { id: objId, label: t.object.type + ':' + t.object.id, type: t.object.type } });
                        }
                        if (!addedNodes.has(subId)) {
                            addedNodes.add(subId);
                            elements.push({ data: { id: subId, label: t.subject.type + ':' + t.subject.id, type: t.subject.type } });
                        }

                        elements.push({
                            data: {
                                id: 'e_' + i,
                                source: objId,
                                target: subId,
                                label: t.relation
                            }
                        });
                    });

                    cy.elements().remove();
                    cy.add(elements);
                    cy.layout({ name: 'cose', animate: false, padding: 30 }).run();
                }
            } catch (e) {
                console.error(e);
            }
        }

        document.getElementById('checkForm').onsubmit = async (e) => {
            e.preventDefault();
            const resDiv = document.getElementById('checkResult');
            resDiv.innerHTML = '<span style="color:#94a3b8;">Evaluating...</span>';
            const body = {
                object: document.getElementById('obj').value,
                relation: document.getElementById('rel').value,
                subject: document.getElementById('subj').value,
            };
            const res = await fetch('/api/check', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(body)
            });
            const data = await res.json();
            if (data.allowed) {
                resDiv.innerHTML = '<span style="color:#10b981;">✔ ALLOWED</span> (Permission granted via Zanzibar graph)';
                if (cy) {
                    cy.elements().removeClass('highlighted');
                    const objNode = cy.getElementById(body.object);
                    const subNode = cy.getElementById(body.subject);
                    if (objNode.length && subNode.length) {
                        const aStar = cy.elements().aStar({ root: objNode, goal: subNode });
                        if (aStar.found) {
                            aStar.path.addClass('highlighted');
                        }
                    }
                }
            } else {
                resDiv.innerHTML = '<span style="color:#ef4444;">✖ DENIED</span> (No path found in Zanzibar graph)';
                if (cy) cy.elements().removeClass('highlighted');
            }
        };

        window.onload = () => {
            initCy();
            loadData();
            setInterval(loadData, 4000);
        };
    </script>
</body>
</html>`
	w.Write([]byte(html))
}
