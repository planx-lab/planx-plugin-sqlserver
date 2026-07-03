package main

import (
	"context"

	"github.com/planx-lab/planx-plugin-sqlserver/internal/sink"
	"github.com/planx-lab/planx-plugin-sqlserver/internal/source"
	"github.com/planx-lab/planx-sdk-go/sdk"
)

func main() {
	sdk.Serve(sdk.Plugin{
		ID:          "sqlserver",
		Version:     "1.0.0",
		DisplayName: "SQL Server Connector",
		Description: "Read from and write to SQL Server (source + sink).",
		Components: []sdk.ComponentSpec{
			{
				ID: "source", Kind: sdk.KindSource, DisplayName: "SQL Server Source", Source: source.New,
				DiscoverSchema: func(ctx context.Context, config []byte) (*sdk.SchemaDiscovery, error) {
					return source.DiscoverSchema(ctx, config, func(cfg source.Config) (source.Querier, error) {
						return source.ConnectQuerier(ctx, cfg)
					})
				},
				ConfigSchema: sdk.Schema(
					sdk.StringField("host", sdk.Required(), sdk.WithDescription("SQL Server host")),
					sdk.IntegerField("port", sdk.WithDefault(sdk.IntValue(1433)), sdk.WithDescription("SQL Server port")),
					sdk.StringField("database", sdk.Required(), sdk.WithDescription("Database name")),
					sdk.StringField("user", sdk.Required(), sdk.WithDescription("DB user")),
					sdk.SecretField("password", sdk.Required(), sdk.WithDescription("DB password")),
					sdk.StringField("table", sdk.Required(), sdk.WithDescription("Source table (schema.table, e.g. dbo.users_src)")),
					sdk.StringField("columns", sdk.WithDescription("Columns to read (comma-separated; empty = all)")),
					sdk.IntegerField("batchRows", sdk.WithDefault(sdk.IntValue(1000)), sdk.WithDescription("Rows per batch")),
					sdk.EnumField("encrypt", []string{"disable", "false", "true"}, sdk.WithDefault(sdk.StringValue("disable")), sdk.WithDescription("Encrypt connection (TLS)")),
				),
			},
			{
				ID: "sink", Kind: sdk.KindSink, DisplayName: "SQL Server Sink", Sink: sink.New,
				ConfigSchema: sdk.Schema(
					sdk.StringField("host", sdk.Required()),
					sdk.IntegerField("port", sdk.WithDefault(sdk.IntValue(1433))),
					sdk.StringField("database", sdk.Required()),
					sdk.StringField("user", sdk.Required()),
					sdk.SecretField("password", sdk.Required(), sdk.WithDescription("DB password")),
					sdk.StringField("table", sdk.Required(), sdk.WithDescription("Target table (e.g. dbo.users)")),
					sdk.StringField("columns", sdk.WithDescription("Comma-separated column list; if empty, uses batch column schema")),
					sdk.IntegerField("batchRows", sdk.WithDefault(sdk.IntValue(1000)), sdk.WithDescription("Rows per INSERT transaction")),
					sdk.EnumField("encrypt", []string{"disable", "false", "true"}, sdk.WithDefault(sdk.StringValue("disable"))),
				),
			},
		},
	})
}
