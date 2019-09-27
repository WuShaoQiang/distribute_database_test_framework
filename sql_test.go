package distribute_database_test_framework

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"
)

func (suite *DMLTestSuite) exec(query string, args ...interface{}) (res sql.Result, err error) {
	for i := 0; i < 10; i++ {
		ctx, _ := context.WithTimeout(context.Background(),2*time.Second)
		server := getOneServerRandomly()
		server.db.ExecContext(ctx,"use test_dml")
		res, err = server.db.ExecContext(ctx,query, args...)
		if isRetryableError(err) {
			// retry
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}
	if err != nil {
		log.Printf("exec failed : %s", err)
	}

	if isDuplicateError(err) {
		return res, nil
	}
	return
}

func (suite *DMLTestSuite) query(query string, args ...interface{}) (res *sql.Rows, err error) {
	for i := 0; i < 10; i++ {
		server := getOneServerRandomly()
		ctx, _ := context.WithTimeout(context.Background(),2*time.Second)
		server.db.QueryContext(ctx,"use test_dml")
		res, err = server.db.QueryContext(ctx,query, args...)
		if isRetryableError(err){
			// retry
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}
	if err != nil {
		log.Printf("query failed : %s", err)
	}

	if isDuplicateError(err) {
		return res, nil
	}
	return
}

func (suite *DMLTestSuite) createTable(tableName string) {
	_, err := suite.exec(fmt.Sprintf("create table %s (c1 int, c2 int, primary key(c1))", tableName))
	suite.Require().NoError(err)
}

func (suite *DMLTestSuite) dropTable(tableName string) {
	_, err := suite.exec(fmt.Sprintf("drop table if exists %s", tableName))
	suite.NoError(err)
}

func (suite *DMLTestSuite) insertSampleValueInto(tableName string) {
	_, err := suite.exec(fmt.Sprintf("insert into %s values (1, 1)", tableName))
	suite.NoError(err)
}

func (suite *DMLTestSuite) selectFrom(fields []string, tableName string, limit int) (res *sql.Rows, err error) {
	return suite.query(fmt.Sprintf("select %s from %s limit %d", strings.Join(fields, " "), tableName, limit))
}
