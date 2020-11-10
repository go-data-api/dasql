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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
	"github.com/go-data-api/dasql"
	_ "github.com/go-sql-driver/mysql"
)

type DBTX interface {
	Exec(ctx context.Context, q string, args ...interface{}) (dasql.Result, error)
	Query(ctx context.Context, q string, args ...interface{}) (dasql.Rows, error)
}

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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*90)
	defer cancel()

	if err = db.PingContext(ctx); err != nil {
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

	acfg := aws.Config{Region: aws.String(cfg["test_dapi_region"]), Retryer: dasql.Retryer{
		DefaultRetryer: client.DefaultRetryer{
			NumMaxRetries: 10,
		}},
	}

	sess := session.Must(session.NewSessionWithOptions(session.Options{Config: acfg}))
	db := dasql.New(rdsdataservice.New(sess),
		cfg["test_dapi_resource_arn"],
		cfg["test_dapi_secret_arn"])

	// query
	t.Run("query", func(t *testing.T) {
		rows := QueryInformationSchema(ctx, t, db)
		defer rows.Close()

		var name string
		for rows.Next() {
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("got: %v", err)
			}

			if name != "sys" {
				t.Fatalf("got: %v", name)
			}
		}
	})

	// exec
	t.Run("exec", func(t *testing.T) {
		if _, err := db.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS dasql_test`); err != nil {
			t.Fatalf("got: %v", err)
		}

		if _, err := db.Exec(ctx, `CREATE TABLE IF NOT EXISTS dasql_test.t1(
			id INT AUTO_INCREMENT PRIMARY KEY,
			data JSON
		)`); err != nil {
			t.Fatalf("got: %v", err)
		}

		// should insert the "0" id
		if _, err := db.Exec(ctx, `INSERT INTO dasql_test.t1 (data) VALUES ('{}')`); err != nil {
			t.Fatalf("got: %v", err)
		}

		res, err := db.Exec(ctx, `INSERT INTO dasql_test.t1 (data) VALUES (:d)`, sql.Named("d", "{}"))
		if err != nil {
			t.Fatalf("got: %v", err)
		}

		raf, err := res.RowsAffected()
		if err != nil || raf != 1 {
			t.Fatalf("got: %v %v", raf, err)
		}

		lid, err := res.LastInsertId()
		if err != nil || lid < 1 {
			t.Fatalf("got: %v %v", lid, err)
		}
	})
}

func QueryInformationSchema(ctx context.Context, tb testing.TB, db DBTX) dasql.Rows {
	rows, err := db.Query(ctx, `
			SELECT table_schema 
			FROM information_schema.columns 
			WHERE table_schema = :name`, sql.Named("name", "sys"))
	if err != nil {
		tb.Fatalf("got: %v", err)
	}

	return rows
}
