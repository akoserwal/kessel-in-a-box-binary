package schema

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/xeipuuv/gojsonschema"
)

//go:embed resources/*
var embeddedSchemas embed.FS

type Validator struct {
	mu      sync.RWMutex
	schemas map[string]*gojsonschema.Schema
}

func NewValidator() (*Validator, error) {
	return NewValidatorWithDirs()
}

func NewValidatorWithDirs(customDirs ...string) (*Validator, error) {
	v := &Validator{
		schemas: make(map[string]*gojsonschema.Schema),
	}
	if err := v.loadEmbeddedSchemas(); err != nil {
		return nil, fmt.Errorf("failed to load embedded schemas: %w", err)
	}
	for _, dir := range customDirs {
		if dir != "" {
			if err := v.LoadDirectory(dir); err != nil {
				return nil, fmt.Errorf("failed to load custom schema directory %s: %w", dir, err)
			}
		}
	}
	return v, nil
}

func (v *Validator) LoadDirectory(dirPath string) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	return filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".json" {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			loader := gojsonschema.NewBytesLoader(data)
			schema, err := gojsonschema.NewSchema(loader)
			if err == nil {
				rel, err := filepath.Rel(dirPath, path)
				if err == nil {
					cleanRel := filepath.ToSlash(filepath.Clean(rel))
					// Strip any leading resources/
					cleanRel = strings.TrimPrefix(cleanRel, "resources/")
					
					// Register standard canonical key
					v.schemas["resources/"+cleanRel] = schema
					v.schemas[cleanRel] = schema

					// If filename is <resource_type>.json at top level, map to common_representation
					if !strings.Contains(cleanRel, "/") {
						resType := strings.TrimSuffix(cleanRel, ".json")
						v.schemas[fmt.Sprintf("resources/%s/common_representation.json", resType)] = schema
					}
				}
			}
		}
		return nil
	})
}

func (v *Validator) loadEmbeddedSchemas() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	return fs.WalkDir(embeddedSchemas, "resources", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(path) == ".json" {
			data, err := embeddedSchemas.ReadFile(path)
			if err != nil {
				return err
			}
			loader := gojsonschema.NewBytesLoader(data)
			schema, err := gojsonschema.NewSchema(loader)
			if err == nil {
				// Normalize key e.g. "host/common" or "host/reporters/hbi"
				cleanKey := filepath.ToSlash(path)
				v.schemas[cleanKey] = schema
			}
		}
		return nil
	})
}

// ValidateCommon validates common representation payload against resource schema
func (v *Validator) ValidateCommon(resourceType string, payload map[string]interface{}) error {
	v.mu.RLock()
	defer v.mu.RUnlock()

	schemaPath := fmt.Sprintf("resources/%s/common_representation.json", resourceType)
	schema, ok := v.schemas[schemaPath]
	if !ok {
		// Schema not defined for this resource type, allow through
		return nil
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	loader := gojsonschema.NewBytesLoader(payloadJSON)
	res, err := schema.Validate(loader)
	if err != nil {
		return err
	}
	if !res.Valid() {
		var errMsgs []string
		for _, desc := range res.Errors() {
			errMsgs = append(errMsgs, desc.String())
		}
		return fmt.Errorf("schema validation error for %s: %v", resourceType, errMsgs)
	}
	return nil
}

// ValidateReporter validates reporter-specific representation payload
func (v *Validator) ValidateReporter(resourceType, reporterType string, payload map[string]interface{}) error {
	v.mu.RLock()
	defer v.mu.RUnlock()

	schemaPath := fmt.Sprintf("resources/%s/reporters/%s/%s.json", resourceType, reporterType, resourceType)
	schema, ok := v.schemas[schemaPath]
	if !ok {
		return nil
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	loader := gojsonschema.NewBytesLoader(payloadJSON)
	res, err := schema.Validate(loader)
	if err != nil {
		return err
	}
	if !res.Valid() {
		var errMsgs []string
		for _, desc := range res.Errors() {
			errMsgs = append(errMsgs, desc.String())
		}
		return fmt.Errorf("reporter schema validation error for %s/%s: %v", resourceType, reporterType, errMsgs)
	}
	return nil
}
