package db_query_tagger

import (
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/processors"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

const sampleConfig = `
  ## Database type (Driver Name): one of {sqlite3, mssql, mysql, postgres}
  database_type = "sqlite3"

  ## Sqlite3 Database file
  database = "/var/lib/grafana/grafana.db"
  
  ## The query to execute, including parameters written as "?"
  query = "select something1, something2 from tablename where value = ?"

  ## The tag to take the parameter's value from
  query_params_tags = ["existing_tag_as_query_parameter"]

  ## The tag to which the result should be put
  query_results_tags = ["new_tag_for_something1", "new_tag_for_something2"]

  ## Remove original parameters tags, so they're replaced by the results tags?
  remove_query_params_tags = false
`

type DBQueryTagger struct {
	DatabaseDriverName string `toml:"database_type"`
	Database string `toml:"database"`
	Query string `toml:"query"`
	QueryParamsTags []string `toml:"query_params_tags"`
	QueryResultsTags []string `toml:"query_results_tags"`
	RemoveQueryParamsTags bool `toml:"remove_query_params_tags"`
	Log telegraf.Logger `toml:"-"`
}

func (r *DBQueryTagger) SampleConfig() string {
	return sampleConfig
}

func (r *DBQueryTagger) Description() string {
	return "Tag metrics that pass through this filter according to db (sqlite3, mssql, mysql or postgres) query results."
}

func (r *DBQueryTagger) Apply(in ...telegraf.Metric) []telegraf.Metric {
	db, err := sql.Open(r.DatabaseDriverName, r.Database)
	if err != nil {
		r.Log.Errorf(err.Error())
	}
	defer db.Close()

	stmt, err := db.Prepare(r.Query)
	if err != nil {
		r.Log.Errorf(err.Error())
	}
	defer stmt.Close()

	for _, point := range in {
		queryParams := make([]interface{}, len(r.QueryParamsTags))
		for i, paramTag := range r.QueryParamsTags {
			if queryParam, ok := point.GetTag(paramTag); ok {
				queryParams[i] = queryParam
			}
		}

		results := make([]interface{}, len(r.QueryResultsTags))
		resultsPointers := make([]interface{}, len(r.QueryResultsTags))
		for i := range results {
			resultsPointers[i] = &results[i]
		}
		row := stmt.QueryRow(queryParams...)

		switch err := row.Scan(resultsPointers...); err {
		case sql.ErrNoRows:
			continue
		case nil:
			if r.RemoveQueryParamsTags {
				for _, tag := range r.QueryParamsTags {
					point.RemoveTag(tag)
				}
			}
			for i, resultTag := range r.QueryResultsTags {
				point.AddTag(resultTag, fmt.Sprintf("%v", results[i]))
			}
		default:
			r.Log.Errorf(err.Error())
		}
	}

	return in
}

func init() {
	processors.Add("db_query_tagger", func() telegraf.Processor {
		return &DBQueryTagger{}
	})
}
