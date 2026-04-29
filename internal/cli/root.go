package cli

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ValentinKolb/rsql/internal/app"
	"github.com/ValentinKolb/rsql/internal/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const version = "0.1.0"

type serveRunner func(context.Context, config.Config) error

// NewRootCmd creates the root command for the rsql CLI.
func NewRootCmd() *cobra.Command {
	return newRootCmd(app.RunServe)
}

func newRootCmd(run serveRunner) *cobra.Command {
	v := config.NewViper()
	cobra.OnInitialize(func() { config.InitEnv(v) })

	rootCmd := &cobra.Command{
		Use:   "rsql",
		Short: "Multi-tenant SQLite database server",
	}

	rootCmd.AddCommand(newServeCmd(v, run))
	rootCmd.AddCommand(newVersionCmd())
	rootCmd.AddCommand(newConfigCmd(v))

	return rootCmd
}

func newServeCmd(v *viper.Viper, run serveRunner) *cobra.Command {
	var cfg config.Config

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the rsql server",
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			if err := v.BindPFlags(cmd.Flags()); err != nil {
				return err
			}
			loaded, err := config.Load(v)
			if err != nil {
				return err
			}
			if err := loaded.Validate(); err != nil {
				return err
			}
			cfg = loaded
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd.Context(), cfg)
		},
	}

	config.BindServeFlags(cmd.Flags())
	return cmd
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print rsql version",
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "rsql v%s\n", version)
			return err
		},
	}
}

func newConfigCmd(v *viper.Viper) *cobra.Command {
	configCmd := &cobra.Command{
		Use:   "config",
		Short: "Inspect runtime configuration",
	}

	printCmd := &cobra.Command{
		Use:   "print",
		Short: "Print the effective configuration",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := v.BindPFlags(cmd.Flags()); err != nil {
				return err
			}

			cfg, err := config.Load(v)
			if err != nil {
				return err
			}

			format, _ := cmd.Flags().GetString("format")
			if format != "json" {
				return fmt.Errorf("unsupported format %q", format)
			}

			enc := json.NewEncoder(cmd.OutOrStdout())
			enc.SetIndent("", "  ")
			return enc.Encode(cfg.Redacted())
		},
	}

	printCmd.Flags().String("format", "json", "Output format")
	config.BindServeFlags(printCmd.Flags())
	configCmd.AddCommand(printCmd)

	return configCmd
}
