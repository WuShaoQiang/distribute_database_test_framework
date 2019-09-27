package distribute_database_test_framework

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/suite"
)

const binDir = "bin/"
const workDir = "var/"

type tidbServer struct {
	*exec.Cmd
	port       int
	statusPort int
	addr       string
	path       string
	logFile    string
	db         *sql.DB
}

type tikvServer struct {
	*exec.Cmd
	pdEndpoints string
	addr        string
	dataDir     string
	logFile     string
}

type pdServer struct {
	*exec.Cmd
	name           string
	dataDir        string
	logFile        string
	initialCluster string
	// for pd server cluster
	peerAddr string
	// for tikv and tidb
	clientAddr string
}

type DMLTestSuite struct {
	suite.Suite
	host   string
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
	goroutineCount int
	operationCountEachGoroutine int
	randomRestartDuration time.Duration
	randomRestart bool

	tidbServerCount int
	tidbServers     []*tidbServer

	tikvServerCount int
	tikvServers     []*tikvServer

	pdServerCount int
	pdServers     []*pdServer
}

func TestDML(t *testing.T) {
	suite.Run(t, new(DMLTestSuite))
}

func (suite *DMLTestSuite) SetupSuite() {
	suite.host = "127.0.0.1"
	suite.tidbServerCount = 3
	suite.tikvServerCount = 3
	suite.pdServerCount = 1
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.goroutineCount = 5
	suite.operationCountEachGoroutine = 20
	suite.randomRestartDuration = 15*time.Second
	suite.randomRestart = false

	rand.Seed(time.Now().UnixNano())

	pdEndpoints := make([]string, 0, suite.pdServerCount)
	clientAddrs := make([]string, 0, suite.pdServerCount)
	for i := 0; i < suite.pdServerCount; i++ {
		pdEndpoints = append(pdEndpoints, fmt.Sprintf("%s:%d", suite.host, suite.getOnePort()))
		clientAddrs = append(clientAddrs, fmt.Sprintf("%s:%d", suite.host, suite.getOnePort()))
	}

	suite.startPDServers(pdEndpoints, clientAddrs)
	suite.startTiKVServers(clientAddrs)
	suite.startTiDBServers(clientAddrs)

	if suite.randomRestart {
		suite.wg.Add(1)
		go suite.restartTiDBRandomly()
	}
}

func (suite *DMLTestSuite) TearDownSuite() {
	log.Println("Clearing Test")
	suite.cancel()
	suite.wg.Wait()
	for _, server := range suite.tidbServers {
		suite.NoError(server.kill())
	}

	for _, server := range suite.tikvServers {
		suite.NoError(server.kill())
	}

	for _, server := range suite.pdServers {
		suite.NoError(server.kill())
	}

	suite.removeAllDir()
}

func (suite *DMLTestSuite) TestInsert() {
	tableName := "test_insert"
	suite.createTable(tableName)

	var wg sync.WaitGroup
	wg.Add(suite.goroutineCount)
	for i := 0; i < suite.goroutineCount; i++ {
		go func(n int) {
			defer wg.Done()
			for j := suite.operationCountEachGoroutine * n; j < suite.operationCountEachGoroutine*(n+1); j++ {
				time.Sleep(100 * time.Millisecond)
				_, err := suite.exec(fmt.Sprintf("insert into %s values (%d,%d)", tableName, j, j))
				if err != nil {
					suite.True(isDuplicateError(err))
				}
			}
		}(i)
	}
	wg.Wait()

	//verify
	m := make(map[int]int)
	rows, err := suite.query(fmt.Sprintf("SELECT c1,c2 FROM %s", tableName))
	suite.NoError(err)
	suite.NotNil(rows)
	for rows.Next() {
		var k, v int
		err := rows.Scan(&k, &v)
		suite.NoError(err)
		m[k] = v
	}
	suite.Len(m, suite.goroutineCount*suite.operationCountEachGoroutine)
	suite.dropTable(tableName)
}

func (suite *DMLTestSuite) TestUpdate() {
	tableName := "test_update"
	suite.createTable(tableName)

	var wg sync.WaitGroup
	wg.Add(suite.goroutineCount)
	for i := 0; i < suite.goroutineCount; i++ {
		go func(n int) {
			defer wg.Done()
			for j := suite.operationCountEachGoroutine * n; j < suite.operationCountEachGoroutine*(n+1); j++ {
				time.Sleep(50 * time.Millisecond)
				_, err := suite.exec(fmt.Sprintf("insert into %s values (%d,%d)", tableName, j, j))
				if err != nil {
					suite.True(isDuplicateError(err))
				}
			}
		}(i)
	}
	wg.Wait()

	wg.Add(suite.goroutineCount)
	for i := 0; i < suite.goroutineCount; i++ {
		go func(n int) {
			defer wg.Done()
			for j := suite.operationCountEachGoroutine * n; j < suite.operationCountEachGoroutine*(n+1); j++ {
				time.Sleep(50 * time.Millisecond)
				_, err := suite.exec(fmt.Sprintf("update %s set c2 = %d where c1 = %d", tableName, j+1, j))
				if err != nil {
					suite.True(isDuplicateError(err))
				}
			}
		}(i)
	}
	wg.Wait()

	m := make(map[int]int)

	rows, err := suite.query(fmt.Sprintf("select c1,c2 from %s", tableName))
	suite.NoError(err)
	suite.NotNil(rows)
	for rows.Next() {
		var k, v int
		err := rows.Scan(&k, &v)
		suite.NoError(err)
		m[k] = v
	}
	suite.Len(m, suite.goroutineCount*suite.operationCountEachGoroutine)
	for k, v := range m {
		suite.Equal(v, k+1)
	}
	suite.dropTable(tableName)
}

func (suite *DMLTestSuite) TestDelete() {
	tableName := "test_delete"
	suite.createTable(tableName)

	var wg sync.WaitGroup
	wg.Add(suite.goroutineCount)
	for i := 0; i < suite.goroutineCount; i++ {
		go func(n int) {
			defer wg.Done()
			for j := suite.operationCountEachGoroutine * n; j < suite.operationCountEachGoroutine*(n+1); j++ {
				time.Sleep(50 * time.Millisecond)
				_, err := suite.exec(fmt.Sprintf("insert into %s values (%d,%d)", tableName, j, j))
				if err != nil {
					suite.True(isDuplicateError(err))
				}
			}
		}(i)
	}
	wg.Wait()

	wg.Add(suite.goroutineCount)
	for i := 0; i < suite.goroutineCount; i++ {
		go func(n int) {
			defer wg.Done()
			for j := suite.operationCountEachGoroutine * n; j < suite.operationCountEachGoroutine*(n+1); j++ {
				time.Sleep(50 * time.Millisecond)
				_, err := suite.exec(fmt.Sprintf("delete from %s where c1 = %d", tableName, j))
				if err != nil {
					suite.True(isDuplicateError(err))
				}
			}
		}(i)
	}
	wg.Wait()

	m := make(map[int]int)

	rows, err := suite.query(fmt.Sprintf("select c1,c2 from %s", tableName))
	suite.NoError(err)
	suite.NotNil(rows)
	for rows.Next() {
		var k, v int
		err := rows.Scan(&k, &v)
		suite.NoError(err)
		m[k] = v
	}
	suite.Len(m, 0)
	suite.dropTable(tableName)
}

func (suite *DMLTestSuite) TestBankTransfer() {
	tableName := "test_bank_transfer"
	_, err := suite.exec(fmt.Sprintf("CREATE TABLE %s (id int,money int,primary key(id))", tableName))
	suite.NoError(err)
	var l sync.Mutex
	expected := make(map[int]int)

	var wg sync.WaitGroup
	wg.Add(suite.goroutineCount)
	for i := 0; i < suite.goroutineCount; i++ {
		go func(n int) {
			defer wg.Done()
			for j := suite.operationCountEachGoroutine * n; j < suite.operationCountEachGoroutine*(n+1); j++ {
				time.Sleep(50 * time.Millisecond)
				_, err := suite.exec(fmt.Sprintf("insert into %s values (%d,%d)", tableName, j, j+1))
				if err == nil || isDuplicateError(err) {
					l.Lock()
					expected[j]=j+1
					l.Unlock()
				}
			}
		}(i)
	}
	wg.Wait()

	wg.Add(suite.goroutineCount)
	for i := 0; i < suite.goroutineCount; i++ {
		go func(n int) {
			defer wg.Done()
			for j := suite.operationCountEachGoroutine * n; j < suite.operationCountEachGoroutine*(n+1)-1; j++ {
				time.Sleep(50 * time.Millisecond)
				err = suite.transfer(tableName, j, j+1, 1)
				if err == nil {
					l.Lock()
					expected[j]--
					expected[j+1]++
					l.Unlock()
				}
			}
		}(i)
	}
	wg.Wait()

	rows, err := suite.query(fmt.Sprintf("SELECT id,money FROM %s", tableName))
	suite.NoError(err)
	suite.Require().NotNil(rows)
	m := make(map[int]int)
	for rows.Next() {
		var id int
		var money int
		err := rows.Scan(&id, &money)
		suite.NoError(err)
		m[id] = money
	}

	for k, v := range m {
		suite.Equal(expected[k], v)
	}
}

func (suite *DMLTestSuite) getTransaction() (txn *sql.Tx, err error) {
	for i:=0;i<10;i++{
		server := suite.getOneServerRandomly()
		txn, err = server.db.Begin()
		if txn != nil && err == nil {
			return
		}
	}

	return nil, errors.New("loop run out")
}

func (suite *DMLTestSuite) transfer(tableName string, sender, receiver, count int) (err error) {
	txn, err := suite.getTransaction()
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			err := txn.Rollback()
			suite.NoError(err)
		}
	}()
	err = suite.doTransfer(txn, tableName, sender, receiver, count)
	if err != nil {
		return
	}

	return
}

func (suite *DMLTestSuite) doTransfer(txn *sql.Tx, tableName string, sender, receiver, count int) (err error) {
	row := txn.QueryRow(fmt.Sprintf("SELECT name FROM %s WHERE id = %d AND money >= %d", tableName, sender, count))
	var scanedName string
	err = row.Scan(&scanedName)
	if err != nil {
		return err
	}
	suite.Equal(sender, scanedName)
	_, err = txn.Exec(fmt.Sprintf("UPDATE %s SET money = money + %d WHERE id = %d", tableName, count, receiver))
	if err != nil {
		return err
	}
	_, err = txn.Exec(fmt.Sprintf("UPDATE %s SET money = money - %d WHERE id = %d", tableName, count, sender))
	if err != nil {
		return err
	}
	return txn.Commit()
}

func (suite *DMLTestSuite) getBinary() {
	resp, err := http.Get("https://download.pingcap.org/tidb-latest-linux-amd64.tar.gz")
	suite.Require().NoError(err)
	f, err := os.Create(filepath.Join(workDir, "bin.tar"))
	suite.Require().NoError(err)
	_, err = io.Copy(f, resp.Body)
	suite.NoError(err)
	suite.NoError(f.Close())
}

func (suite *DMLTestSuite) removeAllDir() {
	for i := 1; i <= suite.pdServerCount; i++ {
		suite.NoError(os.RemoveAll(filepath.Join(workDir, fmt.Sprintf("pd%d", i))))
	}

	for i := 1; i <= suite.tikvServerCount; i++ {
		suite.NoError(os.RemoveAll(filepath.Join(workDir, fmt.Sprintf("tikv%d", i))))
	}
}

func (suite *DMLTestSuite) getOnePort() int {
	ports, err := getUnusedPorts(1)
	suite.Require().NoError(err)
	return ports[0]
}
