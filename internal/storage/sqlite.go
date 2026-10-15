package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

type ResourceRecord struct {
	ID            string     `json:"id"`
	Type          string     `json:"type"`
	CommonVersion int64      `json:"common_version"`
	KTN           string     `json:"ktn"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
	DeletedAt     *time.Time `json:"deleted_at,omitempty"`
}

type ReporterResourceRecord struct {
	ID                    string     `json:"id"`
	ResourceID            string     `json:"resource_id"`
	ReporterType          string     `json:"reporter_type"`
	LocalResourceID       string     `json:"local_resource_id"`
	ReporterInstanceID    string     `json:"reporter_instance_id"`
	ResourceType          string     `json:"resource_type"`
	ApiHref               string     `json:"api_href,omitempty"`
	ConsoleHref           string     `json:"console_href,omitempty"`
	ReporterVersion       string     `json:"reporter_version,omitempty"`
	RepresentationVersion int64      `json:"representation_version"`
	Generation            int64      `json:"generation"`
	Tombstone             bool       `json:"tombstone"`
	CreatedAt             time.Time  `json:"created_at"`
	UpdatedAt             time.Time  `json:"updated_at"`
	DeletedAt             *time.Time `json:"deleted_at,omitempty"`
}

type CommonRepresentationRecord struct {
	ID            string    `json:"id"`
	ResourceID    string    `json:"resource_id"`
	Generation    int64     `json:"generation"`
	Version       int64     `json:"version"`
	WorkspaceID   string    `json:"workspace_id"`
	TransactionID string    `json:"transaction_id"`
	Payload       string    `json:"payload"`
	CreatedAt     time.Time `json:"created_at"`
}

type ReporterRepresentationRecord struct {
	ID                 string    `json:"id"`
	ReporterResourceID string    `json:"reporter_resource_id"`
	Generation         int64     `json:"generation"`
	Version            int64     `json:"version"`
	CommonVersion      *int64    `json:"common_version,omitempty"`
	ReporterVersion    string    `json:"reporter_version,omitempty"`
	Tombstone          bool      `json:"tombstone"`
	Payload            string    `json:"payload"`
	CreatedAt          time.Time `json:"created_at"`
}

type ResourceMetadata struct {
	ApiHref         string
	ConsoleHref     string
	ReporterVersion string
}

type SQLiteStorage struct {
	db *sql.DB
	mu sync.RWMutex
}

func NewSQLiteStorage(dbPath string) (*SQLiteStorage, error) {
	dsn := dbPath
	if dbPath == ":memory:" {
		dsn = "file::memory:?cache=shared"
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite db at %s: %w", dbPath, err)
	}

	// Optimize SQLite pragmas for single-process speed
	pragmas := []string{
		"PRAGMA foreign_keys = ON;",
		"PRAGMA journal_mode = WAL;",
		"PRAGMA synchronous = NORMAL;",
	}
	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			// ignore pragma error if memory mode
		}
	}

	s := &SQLiteStorage{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to run sqlite migrations: %w", err)
	}

	return s, nil
}

func (s *SQLiteStorage) Close() error {
	return s.db.Close()
}

func (s *SQLiteStorage) migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS resources (
		id TEXT PRIMARY KEY,
		type TEXT NOT NULL,
		common_version INTEGER NOT NULL DEFAULT 1,
		ktn TEXT NOT NULL DEFAULT '',
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL,
		deleted_at DATETIME
	);

	CREATE TABLE IF NOT EXISTS reporter_resources (
		id TEXT PRIMARY KEY,
		resource_id TEXT NOT NULL,
		reporter_type TEXT NOT NULL,
		local_resource_id TEXT NOT NULL,
		reporter_instance_id TEXT NOT NULL,
		resource_type TEXT NOT NULL,
		api_href TEXT NOT NULL DEFAULT '',
		console_href TEXT NOT NULL DEFAULT '',
		reporter_version TEXT NOT NULL DEFAULT '',
		representation_version INTEGER NOT NULL DEFAULT 1,
		generation INTEGER NOT NULL DEFAULT 1,
		tombstone BOOLEAN NOT NULL DEFAULT 0,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL,
		deleted_at DATETIME,
		FOREIGN KEY (resource_id) REFERENCES resources(id) ON DELETE CASCADE,
		UNIQUE (reporter_type, local_resource_id, resource_type, reporter_instance_id)
	);

	CREATE TABLE IF NOT EXISTS common_representations (
		id TEXT PRIMARY KEY,
		resource_id TEXT NOT NULL,
		generation INTEGER NOT NULL,
		version INTEGER NOT NULL,
		workspace_id TEXT NOT NULL,
		transaction_id TEXT,
		payload TEXT NOT NULL,
		created_at DATETIME NOT NULL,
		FOREIGN KEY (resource_id) REFERENCES resources(id) ON DELETE CASCADE
	);

	CREATE UNIQUE INDEX IF NOT EXISTS ux_common_reps_txid_nn 
	ON common_representations (transaction_id) 
	WHERE transaction_id IS NOT NULL AND transaction_id != '';

	CREATE TABLE IF NOT EXISTS reporter_representations (
		id TEXT PRIMARY KEY,
		reporter_resource_id TEXT NOT NULL,
		generation INTEGER NOT NULL,
		version INTEGER NOT NULL,
		common_version INTEGER,
		reporter_version TEXT NOT NULL DEFAULT '',
		tombstone BOOLEAN NOT NULL DEFAULT 0,
		payload TEXT NOT NULL,
		created_at DATETIME NOT NULL,
		FOREIGN KEY (reporter_resource_id) REFERENCES reporter_resources(id) ON DELETE CASCADE
	);
	`
	if _, err := s.db.Exec(schema); err != nil {
		return err
	}

	// Safe idempotent column additions for existing databases
	_, _ = s.db.Exec(`ALTER TABLE resources ADD COLUMN common_version INTEGER NOT NULL DEFAULT 1`)
	_, _ = s.db.Exec(`ALTER TABLE resources ADD COLUMN ktn TEXT NOT NULL DEFAULT ''`)
	_, _ = s.db.Exec(`ALTER TABLE reporter_resources ADD COLUMN api_href TEXT NOT NULL DEFAULT ''`)
	_, _ = s.db.Exec(`ALTER TABLE reporter_resources ADD COLUMN console_href TEXT NOT NULL DEFAULT ''`)
	_, _ = s.db.Exec(`ALTER TABLE reporter_resources ADD COLUMN reporter_version TEXT NOT NULL DEFAULT ''`)
	_, _ = s.db.Exec(`ALTER TABLE reporter_resources ADD COLUMN representation_version INTEGER NOT NULL DEFAULT 1`)
	_, _ = s.db.Exec(`ALTER TABLE reporter_resources ADD COLUMN generation INTEGER NOT NULL DEFAULT 1`)
	_, _ = s.db.Exec(`ALTER TABLE reporter_resources ADD COLUMN tombstone BOOLEAN NOT NULL DEFAULT 0`)
	_, _ = s.db.Exec(`ALTER TABLE reporter_representations ADD COLUMN common_version INTEGER`)
	_, _ = s.db.Exec(`ALTER TABLE reporter_representations ADD COLUMN reporter_version TEXT NOT NULL DEFAULT ''`)
	_, _ = s.db.Exec(`ALTER TABLE reporter_representations ADD COLUMN tombstone BOOLEAN NOT NULL DEFAULT 0`)

	return nil
}

func (s *SQLiteStorage) UpsertResource(
	resourceType, reporterType, localResourceID, reporterInstanceID string,
	workspaceID, transactionID string,
	commonPayload, reporterPayload map[string]interface{},
	metadata ...ResourceMetadata,
) (string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return "", "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()

	var meta ResourceMetadata
	if len(metadata) > 0 {
		meta = metadata[0]
	}

	// 1. Idempotency Check on transaction_id
	if transactionID != "" {
		var existingResourceID, existingKTN string
		err := tx.QueryRow(
			`SELECT c.resource_id, COALESCE(r.ktn, '') 
			 FROM common_representations c 
			 JOIN resources r ON c.resource_id = r.id 
			 WHERE c.transaction_id = ? LIMIT 1`,
			transactionID,
		).Scan(&existingResourceID, &existingKTN)
		if err == nil && existingResourceID != "" {
			if existingKTN == "" {
				existingKTN = fmt.Sprintf("ktn_%s", transactionID)
			}
			return existingResourceID, existingKTN, nil
		}
	}

	// 2. Resolve or create reporter_resource by natural key
	var (
		reporterResourceID string
		resourceID         string
		deletedAt          *time.Time
		repVersion         int64
		repGen             int64
	)

	err = tx.QueryRow(`
		SELECT id, resource_id, deleted_at, representation_version, generation 
		FROM reporter_resources 
		WHERE reporter_type = ? AND local_resource_id = ? AND resource_type = ? AND reporter_instance_id = ?
		LIMIT 1`,
		reporterType, localResourceID, resourceType, reporterInstanceID,
	).Scan(&reporterResourceID, &resourceID, &deletedAt, &repVersion, &repGen)

	token := fmt.Sprintf("ktn_%d_%s", now.UnixNano(), uuid.NewString()[:8])

	if err == sql.ErrNoRows {
		// New resource
		resourceID = uuid.NewString()
		reporterResourceID = uuid.NewString()

		_, err = tx.Exec(`
			INSERT INTO resources (id, type, common_version, ktn, created_at, updated_at) 
			VALUES (?, ?, 1, ?, ?, ?)`,
			resourceID, resourceType, token, now, now,
		)
		if err != nil {
			return "", "", fmt.Errorf("failed to insert resource: %w", err)
		}

		_, err = tx.Exec(`
			INSERT INTO reporter_resources (
				id, resource_id, reporter_type, local_resource_id, reporter_instance_id, resource_type,
				api_href, console_href, reporter_version, representation_version, generation, tombstone,
				created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, 0, ?, ?)`,
			reporterResourceID, resourceID, reporterType, localResourceID, reporterInstanceID, resourceType,
			meta.ApiHref, meta.ConsoleHref, meta.ReporterVersion, now, now,
		)
		if err != nil {
			return "", "", fmt.Errorf("failed to insert reporter_resource: %w", err)
		}
	} else if err != nil {
		return "", "", fmt.Errorf("failed to query reporter_resource: %w", err)
	} else {
		// Existing resource: touch updated_at, clear deleted_at / tombstone
		_, err = tx.Exec(`
			UPDATE reporter_resources 
			SET updated_at = ?, deleted_at = NULL, tombstone = 0,
			    representation_version = representation_version + 1,
			    api_href = CASE WHEN ? != '' THEN ? ELSE api_href END,
			    console_href = CASE WHEN ? != '' THEN ? ELSE console_href END,
			    reporter_version = CASE WHEN ? != '' THEN ? ELSE reporter_version END
			WHERE id = ?`,
			now, meta.ApiHref, meta.ApiHref, meta.ConsoleHref, meta.ConsoleHref, meta.ReporterVersion, meta.ReporterVersion, reporterResourceID,
		)
		if err != nil {
			return "", "", fmt.Errorf("failed to update reporter_resource: %w", err)
		}
	}

	// 3. Compute monotonic generation and version for common_representations
	var maxCommonGen, maxCommonVer int64
	_ = tx.QueryRow(`SELECT COALESCE(MAX(generation), 0), COALESCE(MAX(version), 0) FROM common_representations WHERE resource_id = ?`, resourceID).
		Scan(&maxCommonGen, &maxCommonVer)

	newCommonVer := maxCommonVer + 1
	newCommonGen := maxCommonGen + 1

	// Update resources common_version and persistent ktn
	_, err = tx.Exec(`UPDATE resources SET updated_at = ?, deleted_at = NULL, common_version = ?, ktn = ? WHERE id = ?`, now, newCommonVer, token, resourceID)
	if err != nil {
		return "", "", fmt.Errorf("failed to update resource version: %w", err)
	}

	commonJSON, _ := json.Marshal(commonPayload)
	commonRepID := uuid.NewString()
	_, err = tx.Exec(`
		INSERT INTO common_representations (id, resource_id, generation, version, workspace_id, transaction_id, payload, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		commonRepID, resourceID, newCommonGen, newCommonVer, workspaceID, transactionID, string(commonJSON), now,
	)
	if err != nil {
		return "", "", fmt.Errorf("failed to insert common_representation: %w", err)
	}

	// 4. Compute monotonic generation and version for reporter_representations
	var maxRepGen, maxRepVer int64
	_ = tx.QueryRow(`SELECT COALESCE(MAX(generation), 0), COALESCE(MAX(version), 0) FROM reporter_representations WHERE reporter_resource_id = ?`, reporterResourceID).
		Scan(&maxRepGen, &maxRepVer)

	reporterJSON, _ := json.Marshal(reporterPayload)
	repRepID := uuid.NewString()
	_, err = tx.Exec(`
		INSERT INTO reporter_representations (id, reporter_resource_id, generation, version, common_version, reporter_version, tombstone, payload, created_at)
		VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)`,
		repRepID, reporterResourceID, maxRepGen+1, maxRepVer+1, newCommonVer, meta.ReporterVersion, string(reporterJSON), now,
	)
	if err != nil {
		return "", "", fmt.Errorf("failed to insert reporter_representation: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return "", "", fmt.Errorf("failed to commit transaction: %w", err)
	}

	return resourceID, token, nil
}

func (s *SQLiteStorage) SoftDeleteResource(reporterType, localResourceID, resourceType, reporterInstanceID string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	var reporterResourceID, resourceID string

	query := `
		SELECT id, resource_id FROM reporter_resources 
		WHERE reporter_type = ? AND local_resource_id = ? AND resource_type = ? AND (reporter_instance_id = ? OR ? = '') AND deleted_at IS NULL
		LIMIT 1`
	err := s.db.QueryRow(query, reporterType, localResourceID, resourceType, reporterInstanceID, reporterInstanceID).
		Scan(&reporterResourceID, &resourceID)

	if err == sql.ErrNoRows {
		return "", fmt.Errorf("resource not found")
	} else if err != nil {
		return "", err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	// Soft delete / tombstone
	_, err = tx.Exec(`UPDATE reporter_resources SET deleted_at = ?, updated_at = ?, tombstone = 1 WHERE id = ?`, now, now, reporterResourceID)
	if err != nil {
		return "", err
	}

	_, err = tx.Exec(`UPDATE resources SET deleted_at = ?, updated_at = ? WHERE id = ?`, now, now, resourceID)
	if err != nil {
		return "", err
	}

	_, err = tx.Exec(`UPDATE reporter_representations SET tombstone = 1 WHERE reporter_resource_id = ?`, reporterResourceID)
	if err != nil {
		return "", err
	}

	if err := tx.Commit(); err != nil {
		return "", err
	}

	return resourceID, nil
}

// GetResourceKTN returns the stored consistency token for a resource
func (s *SQLiteStorage) GetResourceKTN(resourceType, localResourceID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var ktn string
	query := `
		SELECT r.ktn 
		FROM resources r
		JOIN reporter_resources rr ON r.id = rr.resource_id
		WHERE rr.resource_type = ? AND rr.local_resource_id = ? AND r.deleted_at IS NULL
		LIMIT 1`
	err := s.db.QueryRow(query, resourceType, localResourceID).Scan(&ktn)
	if err != nil {
		return "", err
	}
	return ktn, nil
}

func (s *SQLiteStorage) ListResources(limit int) ([]ResourceRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.Query(`SELECT id, type, common_version, ktn, created_at, updated_at, deleted_at FROM resources WHERE deleted_at IS NULL ORDER BY created_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ResourceRecord
	for rows.Next() {
		var r ResourceRecord
		var delAt sql.NullTime
		if err := rows.Scan(&r.ID, &r.Type, &r.CommonVersion, &r.KTN, &r.CreatedAt, &r.UpdatedAt, &delAt); err != nil {
			return nil, err
		}
		if delAt.Valid {
			r.DeletedAt = &delAt.Time
		}
		result = append(result, r)
	}
	return result, nil
}

// ListResourcesByWorkspace returns non-deleted resources belonging to a given workspace
func (s *SQLiteStorage) ListResourcesByWorkspace(workspaceID string, limit int) ([]ResourceRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if limit <= 0 {
		limit = 100
	}

	query := `
		SELECT DISTINCT r.id, r.type, r.common_version, r.ktn, r.created_at, r.updated_at, r.deleted_at 
		FROM resources r
		JOIN common_representations c ON r.id = c.resource_id
		WHERE r.deleted_at IS NULL AND c.workspace_id = ?
		ORDER BY r.created_at DESC 
		LIMIT ?`
	rows, err := s.db.Query(query, workspaceID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list resources by workspace: %w", err)
	}
	defer rows.Close()

	var result []ResourceRecord
	for rows.Next() {
		var r ResourceRecord
		var delAt sql.NullTime
		if err := rows.Scan(&r.ID, &r.Type, &r.CommonVersion, &r.KTN, &r.CreatedAt, &r.UpdatedAt, &delAt); err != nil {
			return nil, err
		}
		if delAt.Valid {
			r.DeletedAt = &delAt.Time
		}
		result = append(result, r)
	}
	return result, nil
}

type ExportBundle struct {
	Version                 string                         `json:"version"`
	ExportedAt              time.Time                      `json:"exported_at"`
	Resources               []ResourceRecord               `json:"resources"`
	ReporterResources       []ReporterResourceRecord       `json:"reporter_resources"`
	CommonRepresentations   []CommonRepresentationRecord   `json:"common_representations"`
	ReporterRepresentations []ReporterRepresentationRecord `json:"reporter_representations"`
}

func (s *SQLiteStorage) ExportData() (*ExportBundle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bundle := &ExportBundle{
		Version:    "1.0.0",
		ExportedAt: time.Now().UTC(),
	}

	// 1. Resources
	resRows, err := s.db.Query(`SELECT id, type, common_version, ktn, created_at, updated_at, deleted_at FROM resources ORDER BY created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("failed to query resources: %w", err)
	}
	defer resRows.Close()
	for resRows.Next() {
		var r ResourceRecord
		var delAt sql.NullTime
		if err := resRows.Scan(&r.ID, &r.Type, &r.CommonVersion, &r.KTN, &r.CreatedAt, &r.UpdatedAt, &delAt); err != nil {
			return nil, err
		}
		if delAt.Valid {
			r.DeletedAt = &delAt.Time
		}
		bundle.Resources = append(bundle.Resources, r)
	}

	// 2. Reporter Resources
	repRows, err := s.db.Query(`
		SELECT id, resource_id, reporter_type, local_resource_id, reporter_instance_id, resource_type,
		       api_href, console_href, reporter_version, representation_version, generation, tombstone,
		       created_at, updated_at, deleted_at 
		FROM reporter_resources ORDER BY created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("failed to query reporter_resources: %w", err)
	}
	defer repRows.Close()
	for repRows.Next() {
		var r ReporterResourceRecord
		var delAt sql.NullTime
		if err := repRows.Scan(
			&r.ID, &r.ResourceID, &r.ReporterType, &r.LocalResourceID, &r.ReporterInstanceID, &r.ResourceType,
			&r.ApiHref, &r.ConsoleHref, &r.ReporterVersion, &r.RepresentationVersion, &r.Generation, &r.Tombstone,
			&r.CreatedAt, &r.UpdatedAt, &delAt,
		); err != nil {
			return nil, err
		}
		if delAt.Valid {
			r.DeletedAt = &delAt.Time
		}
		bundle.ReporterResources = append(bundle.ReporterResources, r)
	}

	// 3. Common Representations
	comRows, err := s.db.Query(`SELECT id, resource_id, generation, version, workspace_id, COALESCE(transaction_id, ''), payload, created_at FROM common_representations ORDER BY created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("failed to query common_representations: %w", err)
	}
	defer comRows.Close()
	for comRows.Next() {
		var c CommonRepresentationRecord
		if err := comRows.Scan(&c.ID, &c.ResourceID, &c.Generation, &c.Version, &c.WorkspaceID, &c.TransactionID, &c.Payload, &c.CreatedAt); err != nil {
			return nil, err
		}
		bundle.CommonRepresentations = append(bundle.CommonRepresentations, c)
	}

	// 4. Reporter Representations
	repRepRows, err := s.db.Query(`
		SELECT id, reporter_resource_id, generation, version, common_version, reporter_version, tombstone, payload, created_at 
		FROM reporter_representations ORDER BY created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("failed to query reporter_representations: %w", err)
	}
	defer repRepRows.Close()
	for repRepRows.Next() {
		var r ReporterRepresentationRecord
		if err := repRepRows.Scan(&r.ID, &r.ReporterResourceID, &r.Generation, &r.Version, &r.CommonVersion, &r.ReporterVersion, &r.Tombstone, &r.Payload, &r.CreatedAt); err != nil {
			return nil, err
		}
		bundle.ReporterRepresentations = append(bundle.ReporterRepresentations, r)
	}

	return bundle, nil
}

func (s *SQLiteStorage) ImportData(bundle *ExportBundle) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, r := range bundle.Resources {
		_, err := tx.Exec(`
			INSERT OR REPLACE INTO resources (id, type, common_version, ktn, created_at, updated_at, deleted_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			r.ID, r.Type, r.CommonVersion, r.KTN, r.CreatedAt, r.UpdatedAt, r.DeletedAt,
		)
		if err != nil {
			return err
		}
	}

	for _, rr := range bundle.ReporterResources {
		_, err := tx.Exec(`
			INSERT OR REPLACE INTO reporter_resources (
				id, resource_id, reporter_type, local_resource_id, reporter_instance_id, resource_type,
				api_href, console_href, reporter_version, representation_version, generation, tombstone,
				created_at, updated_at, deleted_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			rr.ID, rr.ResourceID, rr.ReporterType, rr.LocalResourceID, rr.ReporterInstanceID, rr.ResourceType,
			rr.ApiHref, rr.ConsoleHref, rr.ReporterVersion, rr.RepresentationVersion, rr.Generation, rr.Tombstone,
			rr.CreatedAt, rr.UpdatedAt, rr.DeletedAt,
		)
		if err != nil {
			return err
		}
	}

	for _, cr := range bundle.CommonRepresentations {
		_, err := tx.Exec(`
			INSERT OR REPLACE INTO common_representations (id, resource_id, generation, version, workspace_id, transaction_id, payload, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			cr.ID, cr.ResourceID, cr.Generation, cr.Version, cr.WorkspaceID, cr.TransactionID, cr.Payload, cr.CreatedAt,
		)
		if err != nil {
			return err
		}
	}

	for _, rep := range bundle.ReporterRepresentations {
		_, err := tx.Exec(`
			INSERT OR REPLACE INTO reporter_representations (id, reporter_resource_id, generation, version, common_version, reporter_version, tombstone, payload, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			rep.ID, rep.ReporterResourceID, rep.Generation, rep.Version, rep.CommonVersion, rep.ReporterVersion, rep.Tombstone, rep.Payload, rep.CreatedAt,
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}
