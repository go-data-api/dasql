// +build awsintegration

package dasql_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
	"github.com/go-data-api/dasql"
	_ "github.com/go-sql-driver/mysql"
)

func TestLocalMySQL(t *testing.T) {
	cfg := make(map[string]string)
	cfgd, _ := ioutil.ReadFile("integrate_conf.json")
	json.Unmarshal(cfgd, &cfg)
	if cfg["local_mysql_port"] == "" {
		t.Skipf("skipping, no local mysql conf: %+v", cfg)
	}

	dsn := fmt.Sprintf("%s:%s@(127.0.0.1:%s)/",
		cfg["local_mysql_user"],
		cfg["local_mysql_pass"],
		cfg["local_mysql_port"],
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("got: %v", err)
	}

	if err = db.Ping(); err != nil {
		t.Fatalf("got: %v", err)
	}

	adb := dasql.Adapt(db)
	_ = adb
}

func TestRealDataAPI(t *testing.T) {
	cfg := make(map[string]string)
	cfgd, _ := ioutil.ReadFile("integrate_conf.json")
	json.Unmarshal(cfgd, &cfg)
	if cfg["test_dapi_resource_arn"] == "" {
		t.Skipf("skipping, no real data api in conf: %+v", cfg)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*90)
	defer cancel()

	// @TODO add a retryer

	sess := session.Must(session.NewSession())
	db := dasql.New(rdsdataservice.New(sess),
		cfg["test_dapi_resource_arn"],
		cfg["test_dapi_secret_arn"])

	// query
	t.Run("query", func(t *testing.T) {
		rows, err := db.Query(ctx, `
			SELECT table_schema 
			FROM information_schema.columns 
			WHERE table_schema = :name`, sql.Named("name", "sys"))
		if err != nil {
			t.Fatalf("got: %v", err)
		}

		defer rows.Close()
		var name string
		for rows.Next() {
			if err = rows.Scan(&name); err != nil {
				t.Fatalf("got: %v", err)
			}

			if name != "sys" {
				t.Fatalf("got: %v", name)
			}
		}
	})

	// exec
	t.Run("exec", func(t *testing.T) {
		rows, err := db.Query(ctx, `
			SELECT table_schema 
			FROM information_schema.columns 
			WHERE table_schema = :name`, sql.Named("name", "sys"))
		if err != nil {
			t.Fatalf("got: %v", err)
		}

		defer rows.Close()
		var name string
		for rows.Next() {
			if err = rows.Scan(&name); err != nil {
				t.Fatalf("got: %v", err)
			}

			if name != "sys" {
				t.Fatalf("got: %v", name)
			}
		}
	})

}
