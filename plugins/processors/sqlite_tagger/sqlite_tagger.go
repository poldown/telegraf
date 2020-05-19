package sqlite_tagger

import (
	"telegraf"
	"telegraf/plugins/processors"
	"database/sql"
	"fmt"
	"go-sqlite3"
	"log"
)

const sampleConfig = `
`

type SqlLiteTagger struct {
	Database string `toml:"database"`
	Query string `toml:"query"`
	QueryParamTag string `toml:"query_param_tag"`
	QueryResultTag string `toml:"query_result_tag"`
}

func (r *SqlLiteTagger) SampleConfig() string {
	return sampleConfig
}

func (r *SqlLiteTagger) Description() string {
	return "Tag metrics that pass through this filter according to SqlLite3 query results."
}

func (r *SqlLiteTagger) Apply(in ...telegraf.Metric) []telegraf.Metric {
	db, err := sql.Open("sqlite3", r.Database)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	for _, point := range in {
		stmt, err = db.Prepare(r.Query)
		if err != nil {
			log.Fatal(err)
		}
		defer stmt.Close()

		var result := ""
		if r.QueryParamTag != "" {
			if queryParam, ok := point.GetTag(r.QueryParamTag); ok {
				err = stmt.QueryRow(queryParam).Scan(&result)
				if err != nil {
					log.Fatal(err)
				}
			} else {
				err = stmt.QueryRow().Scan(&result)
				if err != nil {
					log.Fatal(err)
				}
			}
		}

		if r.QueryResultTag != "" {
			if _, ok := point.GetTag(r.QueryResultTag); ok {
				point.RemoveTag(r.QueryResultTag)
			}
			point.AddTag(r.QueryResultTag, result)
		}
	}

	return in
}

func init() {
	processors.Add("rename", func() telegraf.Processor {
		return &Rename{}
	})
}
