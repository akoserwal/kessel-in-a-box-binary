package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/project-kessel/kessel-in-a-box-binary/internal/bootstrapper"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/inventory"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/rebac"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/relations"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/schema"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/server"
	"github.com/project-kessel/kessel-in-a-box-binary/internal/storage"
	"github.com/spf13/cobra"
)

var (
	grpcPort         int
	httpPort         int
	dbPath           string
	schemasDir       string
	rolesConfig      string
	simulateCDCDelay time.Duration
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "kessel-box",
		Short: "kessel-box: All-in-one Project Kessel local development & testing binary",
	}

	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Start the Kessel-in-a-Box gRPC and HTTP services",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("================================================================")
			fmt.Println("📦 Starting Kessel-in-a-Box (All-in-One Kessel Platform Runner)")
			fmt.Println("================================================================")

			// 1. Initialize SQLite storage
			db, err := storage.NewSQLiteStorage(dbPath)
			if err != nil {
				return fmt.Errorf("failed to init sqlite: %w", err)
			}
			defer db.Close()
			fmt.Printf("✔ SQLite storage initialized (%s)\n", dbPath)

			// 2. Initialize ReBAC engine
			engine := rebac.NewEngine()
			if rolesConfig != "" {
				if err := engine.LoadRoleConfigFile(rolesConfig); err != nil {
					return fmt.Errorf("failed to load roles config: %w", err)
				}
				fmt.Printf("✔ Custom roles & permissions loaded from: %s\n", rolesConfig)
			}
			fmt.Println("✔ Embedded Zanzibar ReBAC engine initialized")

			// 3. Initialize Schema Validator (supports custom schema directory)
			validator, err := schema.NewValidatorWithDirs(schemasDir)
			if err != nil {
				return fmt.Errorf("failed to init schema validator: %w", err)
			}
			if schemasDir != "" {
				fmt.Printf("✔ Schema validator loaded custom schemas from: %s\n", schemasDir)
			} else {
				fmt.Println("✔ Embedded JSON Schema Draft-07 validator initialized")
			}

			// 4. Initialize Bootstrapper
			boot := bootstrapper.NewBootstrapper(engine)
			fmt.Println("✔ Tenant workspace and role bootstrapper seeded")

			// 5. Initialize Services
			invSvc := inventory.NewService(db, engine, validator, boot)
			if simulateCDCDelay > 0 {
				invSvc.SetCDCDelay(simulateCDCDelay)
				fmt.Printf("✔ Simulated CDC async delay enabled: %v\n", simulateCDCDelay)
			}
			relSvc := relations.NewService(engine)

			// 6. Start Server
			srv := server.NewServer(grpcPort, httpPort, db, engine, invSvc, relSvc)
			if err := srv.Start(); err != nil {
				return err
			}

			fmt.Println("----------------------------------------------------------------")
			fmt.Printf("👉 gRPC Inventory & Relations Endpoint: localhost:%d\n", grpcPort)
			fmt.Printf("👉 HTTP Developer UI & Playground:      http://localhost:%d\n", httpPort)
			fmt.Println("----------------------------------------------------------------")
			fmt.Println("Press Ctrl+C to stop.")

			// Wait for termination signal
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
			<-sigCh

			fmt.Println("\nStopping Kessel-in-a-Box...")
			srv.Stop()
			fmt.Println("Gracefully shut down. Goodbye!")
			return nil
		},
	}

	startCmd.Flags().IntVar(&grpcPort, "grpc-port", 9000, "gRPC server port")
	startCmd.Flags().IntVar(&httpPort, "http-port", 8088, "HTTP server & Web UI port")
	startCmd.Flags().StringVar(&dbPath, "db-path", ":memory:", "SQLite database file path or :memory:")
	startCmd.Flags().StringVar(&schemasDir, "schemas-dir", "", "Optional path to custom JSON schema directory")
	startCmd.Flags().StringVar(&rolesConfig, "roles-config", "", "Optional path to custom roles JSON configuration file")
	startCmd.Flags().DurationVar(&simulateCDCDelay, "simulate-cdc-delay", 0, "Optional simulated CDC propagation delay (e.g. 100ms) for async write visibility")

	checkCmd := &cobra.Command{
		Use:   "check",
		Short: "Perform an offline ReBAC permission check",
		RunE: func(cmd *cobra.Command, args []string) error {
			objStr, _ := cmd.Flags().GetString("object")
			rel, _ := cmd.Flags().GetString("relation")
			subjStr, _ := cmd.Flags().GetString("subject")

			engine := rebac.NewEngine()
			bootstrapper.NewBootstrapper(engine)

			obj := rebac.ParseObjectRef(objStr)
			subj := rebac.ParseSubjectRef(subjStr)

			allowed, err := engine.CheckPermission(context.Background(), obj, rel, subj)
			if err != nil {
				return err
			}

			if allowed {
				fmt.Printf("✔ ALLOWED: Subject [%s] HAS [%s] on Object [%s]\n", subjStr, rel, objStr)
			} else {
				fmt.Printf("✖ DENIED: Subject [%s] DOES NOT HAVE [%s] on Object [%s]\n", subjStr, rel, objStr)
			}
			return nil
		},
	}
	checkCmd.Flags().String("object", "rbac/workspace:default_12345", "Object to check")
	checkCmd.Flags().String("relation", "view", "Relation / permission to check")
	checkCmd.Flags().String("subject", "rbac/principal:redhat/test-admin", "Subject to check")

	seedCmd := &cobra.Command{
		Use:   "seed",
		Short: "Seed demo resources (hosts, clusters, role bindings) into database",
		RunE: func(cmd *cobra.Command, args []string) error {
			tenant, _ := cmd.Flags().GetString("tenant")
			profile, _ := cmd.Flags().GetString("profile")
			path, _ := cmd.Flags().GetString("db-path")

			db, err := storage.NewSQLiteStorage(path)
			if err != nil {
				return err
			}
			defer db.Close()

			engine := rebac.NewEngine()
			boot := bootstrapper.NewBootstrapper(engine)
			if err := boot.SeedProfile(profile, tenant, db); err != nil {
				return fmt.Errorf("seeding failed: %w", err)
			}
			fmt.Printf("✔ Successfully seeded profile [%s] for tenant [%s] into %s\n", profile, tenant, path)
			return nil
		},
	}
	seedCmd.Flags().String("tenant", "org-12345", "Tenant organization ID")
	seedCmd.Flags().String("profile", "hbi-demo", "Seeding profile: hbi-demo, k8s-demo")
	seedCmd.Flags().String("db-path", "kessel.db", "SQLite database file path")

	exportCmd := &cobra.Command{
		Use:   "export",
		Short: "Export database resources and representations to a JSON bundle",
		RunE: func(cmd *cobra.Command, args []string) error {
			outPath, _ := cmd.Flags().GetString("out")
			path, _ := cmd.Flags().GetString("db-path")

			db, err := storage.NewSQLiteStorage(path)
			if err != nil {
				return err
			}
			defer db.Close()

			bundle, err := db.ExportData()
			if err != nil {
				return err
			}
			data, err := json.MarshalIndent(bundle, "", "  ")
			if err != nil {
				return err
			}
			if err := os.WriteFile(outPath, data, 0644); err != nil {
				return err
			}
			fmt.Printf("✔ Successfully exported snapshot (%d resources) to %s\n", len(bundle.Resources), outPath)
			return nil
		},
	}
	exportCmd.Flags().String("out", "snapshot.json", "Output JSON file path")
	exportCmd.Flags().String("db-path", "kessel.db", "SQLite database file path")

	importCmd := &cobra.Command{
		Use:   "import",
		Short: "Import database state from a JSON bundle",
		RunE: func(cmd *cobra.Command, args []string) error {
			inPath, _ := cmd.Flags().GetString("in")
			path, _ := cmd.Flags().GetString("db-path")

			data, err := os.ReadFile(inPath)
			if err != nil {
				return err
			}
			var bundle storage.ExportBundle
			if err := json.Unmarshal(data, &bundle); err != nil {
				return err
			}

			db, err := storage.NewSQLiteStorage(path)
			if err != nil {
				return err
			}
			defer db.Close()

			if err := db.ImportData(&bundle); err != nil {
				return err
			}
			fmt.Printf("✔ Successfully imported %d resources from %s into %s\n", len(bundle.Resources), inPath, path)
			return nil
		},
	}
	importCmd.Flags().String("in", "snapshot.json", "Input JSON file path to import")
	importCmd.Flags().String("db-path", "kessel.db", "SQLite database file path")

	rootCmd.AddCommand(startCmd, checkCmd, seedCmd, exportCmd, importCmd)
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
