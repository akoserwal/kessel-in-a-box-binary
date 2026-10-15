package rebac

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type ObjectRef struct {
	Namespace string `json:"namespace"`
	Type      string `json:"type"`
	ID        string `json:"id"`
}

func (o ObjectRef) String() string {
	if o.Namespace != "" {
		return fmt.Sprintf("%s/%s:%s", o.Namespace, o.Type, o.ID)
	}
	return fmt.Sprintf("%s:%s", o.Type, o.ID)
}

func ParseObjectRef(s string) ObjectRef {
	var ns, typ, id string
	parts := strings.Split(s, ":")
	if len(parts) == 2 {
		id = parts[1]
		slashParts := strings.Split(parts[0], "/")
		if len(slashParts) == 2 {
			ns = slashParts[0]
			typ = slashParts[1]
		} else {
			typ = parts[0]
		}
	} else {
		id = s
	}
	return ObjectRef{Namespace: ns, Type: typ, ID: id}
}

type SubjectRef struct {
	ObjectRef
	Relation string `json:"relation,omitempty"`
}

func (s SubjectRef) String() string {
	base := s.ObjectRef.String()
	if s.Relation != "" {
		return fmt.Sprintf("%s#%s", base, s.Relation)
	}
	return base
}

func ParseSubjectRef(s string) SubjectRef {
	relParts := strings.Split(s, "#")
	obj := ParseObjectRef(relParts[0])
	var rel string
	if len(relParts) > 1 {
		rel = relParts[1]
	}
	return SubjectRef{ObjectRef: obj, Relation: rel}
}

type Tuple struct {
	Object          ObjectRef  `json:"object"`
	Relation        string     `json:"relation"`
	Subject         SubjectRef `json:"subject"`
	WrittenAtNanos  int64      `json:"written_at_nanos,omitempty"`
}

func (t Tuple) Key() string {
	return fmt.Sprintf("%s#%s@%s", t.Object.String(), t.Relation, t.Subject.String())
}

type RolesConfigFile struct {
	Roles           map[string][]string `json:"roles"`
	MacroExpansions map[string][]string `json:"macro_expansions,omitempty"`
}

type Engine struct {
	mu sync.RWMutex

	// Primary store: "object#rel@subject" -> Tuple
	tuples map[string]Tuple

	// Secondary index: object.String() -> map[tupleKey]Tuple
	byObject map[string]map[string]Tuple

	// Secondary index: subject.String() -> map[tupleKey]Tuple
	bySubject map[string]map[string]Tuple

	// Role permissions mapping: role -> set of allowed verbs
	rolePermissions map[string]map[string]bool

	// Macro expansions: verb -> set of equivalent verbs (e.g., view -> [read, list, get])
	macroExpansions map[string][]string

	// Monotonic commit timestamp
	lastCommittedNanos int64
}

func NewEngine() *Engine {
	e := &Engine{
		tuples:             make(map[string]Tuple),
		byObject:           make(map[string]map[string]Tuple),
		bySubject:          make(map[string]map[string]Tuple),
		rolePermissions:    make(map[string]map[string]bool),
		macroExpansions:    make(map[string][]string),
		lastCommittedNanos: time.Now().UnixNano(),
	}
	e.seedDefaultRoles()
	return e
}

func (e *Engine) seedDefaultRoles() {
	// Standard Red Hat / Kessel roles
	e.rolePermissions["admin"] = map[string]bool{
		"view": true, "edit": true, "delete": true, "admin": true, "*": true,
	}
	e.rolePermissions["editor"] = map[string]bool{
		"view": true, "edit": true,
	}
	e.rolePermissions["viewer"] = map[string]bool{
		"view": true,
	}

	// Default verb macro expansions
	e.macroExpansions["view"] = []string{"read", "view", "get", "list", "check"}
	e.macroExpansions["edit"] = []string{"write", "update", "edit", "patch", "modify"}
	e.macroExpansions["delete"] = []string{"delete", "remove", "destroy"}
}

// LoadRoleConfigFile loads custom role-permission definitions and macro expansions from a JSON file
func (e *Engine) LoadRoleConfigFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read roles config file: %w", err)
	}

	var cfg RolesConfigFile
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("failed to parse roles config JSON: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	for role, perms := range cfg.Roles {
		if e.rolePermissions[role] == nil {
			e.rolePermissions[role] = make(map[string]bool)
		}
		for _, p := range perms {
			e.rolePermissions[role][p] = true
		}
	}

	for verb, expansions := range cfg.MacroExpansions {
		e.macroExpansions[verb] = expansions
	}

	return nil
}

func (e *Engine) SetRolePermissions(role string, perms []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.rolePermissions[role] == nil {
		e.rolePermissions[role] = make(map[string]bool)
	}
	for _, p := range perms {
		e.rolePermissions[role][p] = true
	}
}

func (e *Engine) SetMacroExpansions(verb string, expansions []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.macroExpansions[verb] = expansions
}

func (e *Engine) GetLastCommittedNanos() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.lastCommittedNanos
}

func (e *Engine) GetLatestToken() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return fmt.Sprintf("ktn_%d_%s", e.lastCommittedNanos, uuid.NewString()[:8])
}

func (e *Engine) WriteTuple(tuple Tuple) {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now().UnixNano()
	if now <= e.lastCommittedNanos {
		now = e.lastCommittedNanos + 1
	}
	e.lastCommittedNanos = now
	tuple.WrittenAtNanos = now

	key := tuple.Key()
	e.tuples[key] = tuple

	// Index by Object
	objKey := tuple.Object.String()
	if e.byObject[objKey] == nil {
		e.byObject[objKey] = make(map[string]Tuple)
	}
	e.byObject[objKey][key] = tuple

	// Index by Subject
	subKey := tuple.Subject.String()
	if e.bySubject[subKey] == nil {
		e.bySubject[subKey] = make(map[string]Tuple)
	}
	e.bySubject[subKey][key] = tuple
}

func (e *Engine) DeleteTuple(tuple Tuple) {
	e.mu.Lock()
	defer e.mu.Unlock()

	key := tuple.Key()
	if _, ok := e.tuples[key]; ok {
		delete(e.tuples, key)

		objKey := tuple.Object.String()
		if m, ok := e.byObject[objKey]; ok {
			delete(m, key)
			if len(m) == 0 {
				delete(e.byObject, objKey)
			}
		}

		subKey := tuple.Subject.String()
		if m, ok := e.bySubject[subKey]; ok {
			delete(m, key)
			if len(m) == 0 {
				delete(e.bySubject, subKey)
			}
		}

		e.lastCommittedNanos = time.Now().UnixNano()
	}
}

func (e *Engine) DeleteTuplesForResource(resourceType, resourceID string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	prefix := fmt.Sprintf("%s:%s", resourceType, resourceID)
	for key, t := range e.tuples {
		if t.Object.String() == prefix || strings.HasPrefix(t.Object.String(), prefix) {
			delete(e.tuples, key)

			objKey := t.Object.String()
			if m, ok := e.byObject[objKey]; ok {
				delete(m, key)
				if len(m) == 0 {
					delete(e.byObject, objKey)
				}
			}

			subKey := t.Subject.String()
			if m, ok := e.bySubject[subKey]; ok {
				delete(m, key)
				if len(m) == 0 {
					delete(e.bySubject, subKey)
				}
			}
		}
	}
	e.lastCommittedNanos = time.Now().UnixNano()
}

// DeleteTuplesByFilter removes tuples matching filter fields and returns number of deleted tuples
func (e *Engine) DeleteTuplesByFilter(
	resNs, resType, resID, rel *string,
	subNs, subType, subID, subRel *string,
) int {
	e.mu.Lock()
	defer e.mu.Unlock()

	deleted := 0
	for key, t := range e.tuples {
		if resNs != nil && *resNs != "" && t.Object.Namespace != *resNs {
			continue
		}
		if resType != nil && *resType != "" && t.Object.Type != *resType {
			continue
		}
		if resID != nil && *resID != "" && t.Object.ID != *resID {
			continue
		}
		if rel != nil && *rel != "" && t.Relation != *rel {
			continue
		}
		if subNs != nil && *subNs != "" && t.Subject.Namespace != *subNs {
			continue
		}
		if subType != nil && *subType != "" && t.Subject.Type != *subType {
			continue
		}
		if subID != nil && *subID != "" && t.Subject.ID != *subID {
			continue
		}
		if subRel != nil && *subRel != "" && t.Subject.Relation != *subRel {
			continue
		}

		delete(e.tuples, key)

		objKey := t.Object.String()
		if m, ok := e.byObject[objKey]; ok {
			delete(m, key)
			if len(m) == 0 {
				delete(e.byObject, objKey)
			}
		}

		subKey := t.Subject.String()
		if m, ok := e.bySubject[subKey]; ok {
			delete(m, key)
			if len(m) == 0 {
				delete(e.bySubject, subKey)
			}
		}

		deleted++
	}

	if deleted > 0 {
		e.lastCommittedNanos = time.Now().UnixNano()
	}
	return deleted
}

func (e *Engine) ListAllTuples() []Tuple {
	e.mu.RLock()
	defer e.mu.RUnlock()
	res := make([]Tuple, 0, len(e.tuples))
	for _, t := range e.tuples {
		res = append(res, t)
	}
	return res
}

// CheckPermission evaluates Zanzibar ReBAC reachability:
// Does subject have 'relation' on 'object'?
func (e *Engine) CheckPermission(ctx context.Context, object ObjectRef, relation string, subject SubjectRef) (bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	visited := make(map[string]bool)
	return e.checkInternal(object, relation, subject, visited)
}

func (e *Engine) checkInternal(object ObjectRef, relation string, subject SubjectRef, visited map[string]bool) (bool, error) {
	stateKey := fmt.Sprintf("%s#%s@%s", object.String(), relation, subject.String())
	if visited[stateKey] {
		return false, nil // avoid infinite cycles
	}
	visited[stateKey] = true

	// 1. Direct Tuple Match via Map Key
	directKey := stateKey
	if _, ok := e.tuples[directKey]; ok {
		return true, nil
	}

	// 2. Direct match with subject relation wildcard or matching using byObject index (O(deg(object)))
	objKey := object.String()
	objTuples := e.byObject[objKey]
	for _, t := range objTuples {
		if t.Relation == relation || t.Relation == "*" {
			if t.Subject.String() == subject.String() {
				return true, nil
			}
			// Subject Set Indirection (e.g. subject is a role_binding#subject)
			if t.Subject.Relation != "" {
				subObj := t.Subject.ObjectRef
				ok, _ := e.checkInternal(subObj, t.Subject.Relation, subject, visited)
				if ok {
					return true, nil
				}
			}
		}
	}

	// 3. Workspace Scoping & Downward Inheritance Traversal
	// If object has relation "workspace" or "parent" -> find target workspaces
	var parentWorkspaces []ObjectRef
	for _, t := range objTuples {
		if t.Relation == "workspace" || t.Relation == "parent" {
			parentWorkspaces = append(parentWorkspaces, t.Subject.ObjectRef)
		}
	}

	// Evaluate permissions on parent workspaces
	for _, ws := range parentWorkspaces {
		// Check if subject has role on this workspace granting 'relation'
		ok, _ := e.checkWorkspaceGrants(ws, object.Type, relation, subject, visited)
		if ok {
			return true, nil
		}
		// Recurse up the workspace hierarchy (parent chain)
		ok, _ = e.checkInternal(ws, relation, subject, visited)
		if ok {
			return true, nil
		}
	}

	return false, nil
}

func (e *Engine) checkWorkspaceGrants(workspace ObjectRef, resourceType, relation string, subject SubjectRef, visited map[string]bool) (bool, error) {
	wsKey := workspace.String()
	wsTuples := e.byObject[wsKey]

	// Look for 3-tuple atomic role binding on this workspace:
	// workspace -> user_grant -> role_binding
	for _, t := range wsTuples {
		if t.Relation == "user_grant" || t.Relation == "role_grant" {
			roleBinding := t.Subject.ObjectRef

			// Verify role_binding -> subject -> principal
			if e.bindingMatchesSubject(roleBinding, subject) {
				// Verify role_binding -> granted -> role
				roles := e.getRolesForBinding(roleBinding)
				for _, r := range roles {
					roleName := r.ID
					if r.Type != "" && roleName == "" {
						roleName = r.Type
					}
					// Does this role grant the verb?
					if e.roleGrantsPermission(roleName, resourceType, relation) {
						return true, nil
					}
				}
			}
		}
	}

	// Also traverse workspace -> parent
	for _, t := range wsTuples {
		if t.Relation == "parent" {
			parentWS := t.Subject.ObjectRef
			ok, _ := e.checkWorkspaceGrants(parentWS, resourceType, relation, subject, visited)
			if ok {
				return true, nil
			}
		}
	}

	return false, nil
}

// roleGrantsPermission evaluates whether a role grants a relation verb on resourceType
// Supports exact match, wildcards ("*"), app prefixes ("inventory:*"), and verb macro expansions
func (e *Engine) roleGrantsPermission(roleName, resourceType, relation string) bool {
	perms, exists := e.rolePermissions[roleName]
	if !exists {
		return false
	}

	// 1. Direct match or global wildcard
	if perms["*"] || perms[relation] {
		return true
	}

	// 2. Resource-scoped match: e.g. "inventory:hosts:read" or "hosts:view"
	scopedRelation := fmt.Sprintf("%s:%s", resourceType, relation)
	if perms[scopedRelation] {
		return true
	}

	// 3. Prefix wildcard match: e.g. "inventory:*" or "host:*"
	for p := range perms {
		if strings.HasSuffix(p, ":*") {
			prefix := strings.TrimSuffix(p, ":*")
			if strings.HasPrefix(scopedRelation, prefix+":") || strings.HasPrefix(relation, prefix+":") || relation == prefix {
				return true
			}
		}
	}

	// 4. Macro expansions: e.g. check expanded equivalents for "view" -> [read, list, get]
	for p := range perms {
		if expansions, ok := e.macroExpansions[p]; ok {
			for _, exp := range expansions {
				if exp == relation || exp == scopedRelation {
					return true
				}
			}
		}
	}

	return false
}

func (e *Engine) bindingMatchesSubject(binding ObjectRef, targetSubject SubjectRef) bool {
	bTuples := e.byObject[binding.String()]
	for _, t := range bTuples {
		if t.Relation == "subject" {
			if t.Subject.String() == targetSubject.String() {
				return true
			}
			// Red Hat principal normalization (e.g. principal:redhat/alice vs redhat/alice)
			if strings.TrimPrefix(t.Subject.String(), "rbac/") == strings.TrimPrefix(targetSubject.String(), "rbac/") {
				return true
			}
			// Compare just IDs if type is principal/user
			if (t.Subject.Type == "principal" || t.Subject.Type == "user") &&
				(targetSubject.Type == "principal" || targetSubject.Type == "user") &&
				t.Subject.ID == targetSubject.ID {
				return true
			}
		}
	}
	return false
}

func (e *Engine) getRolesForBinding(binding ObjectRef) []ObjectRef {
	var roles []ObjectRef
	bTuples := e.byObject[binding.String()]
	for _, t := range bTuples {
		if t.Relation == "granted" || t.Relation == "role" {
			roles = append(roles, t.Subject.ObjectRef)
		}
	}
	return roles
}

// LookupResources returns all resource IDs of resourceType accessible to subject with relation
func (e *Engine) LookupResources(ctx context.Context, resourceType, relation string, subject SubjectRef) ([]string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var matchedIDs []string
	seen := make(map[string]bool)

	for _, t := range e.tuples {
		if t.Object.Type == resourceType {
			id := t.Object.ID
			if !seen[id] {
				visited := make(map[string]bool)
				ok, _ := e.checkInternal(t.Object, relation, subject, visited)
				if ok {
					seen[id] = true
					matchedIDs = append(matchedIDs, id)
				}
			}
		}
	}

	return matchedIDs, nil
}

// LookupSubjects returns all subjects who have relation on object
func (e *Engine) LookupSubjects(ctx context.Context, object ObjectRef, relation string, subjectType string) ([]SubjectRef, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var matchedSubjects []SubjectRef
	seen := make(map[string]bool)

	// Collect candidate subjects from all tuples
	for _, t := range e.tuples {
		subj := t.Subject
		if subjectType != "" && subj.Type != subjectType {
			continue
		}
		sKey := subj.String()
		if seen[sKey] {
			continue
		}

		visited := make(map[string]bool)
		ok, _ := e.checkInternal(object, relation, subj, visited)
		if ok {
			seen[sKey] = true
			matchedSubjects = append(matchedSubjects, subj)
		}
	}

	return matchedSubjects, nil
}
