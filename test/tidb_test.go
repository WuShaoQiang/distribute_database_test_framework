package test

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func (suite *DMLTestSuite) startTiDBServers(clientAddrs []string) {
	for i := 0; i < suite.tidbServerCount; i++ {
		tidb := &tidbServer{
			port:       suite.getOnePort(),
			statusPort: suite.getOnePort(),
			path:       strings.Join(clientAddrs, ","),
			logFile:    filepath.Join(workDir, fmt.Sprintf("tidb%d.log", i+1)),
		}
		tidb.addr = fmt.Sprintf("%s:%d", suite.host, tidb.port)
		suite.NoError(tidb.startServer())
		suite.tidbServers = append(suite.tidbServers, tidb)
	}
}

func (tidb *tidbServer) startServer() (err error) {
	err = tidb.start()
	if err != nil {
		return err
	}
	return tidb.connectToDatabase()
}

func (tidb *tidbServer) start() error {
	var cmd *exec.Cmd
	cmd = exec.Command(filepath.Join(binDir, "/tidb-server"),
		"--store=tikv",
		// "-L=debug",
		fmt.Sprintf("--log-file=%s", tidb.logFile),
		fmt.Sprintf("--path=%s", tidb.path),
		fmt.Sprintf("-P=%d", tidb.port),
		fmt.Sprintf("--status=%d", tidb.statusPort))

	tidb.Cmd = cmd
	err := cmd.Start()
	return err
}

func (tidb *tidbServer) kill() error {
	return killProcess(tidb.Process)
}

func (suite *DMLTestSuite) restartTiDBRandomly() {
	defer suite.wg.Done()
	for {
		select {
		case <-time.After(suite.randomRestartDuration):
			err := suite.getOneServerRandomly().restart()
			suite.NoError(err)
		case <-suite.ctx.Done():
			return
		}
	}
}

func (tidb *tidbServer) restart() (err error) {
	log.Printf("restarting tidb server %s",tidb.addr)
	err = tidb.kill()
	if err != nil {
		return
	}
	return tidb.startServer()
}

func (tidb *tidbServer) connectToDatabase() (err error) {
	for i := 0; i < 5; i++ {
		log.Printf("connecting to %s", tidb.addr)
		tidb.db, err = sql.Open("mysql", fmt.Sprintf("root@(%s)/", tidb.addr))
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		log.Println("Ping to database")
		err = tidb.db.Ping()
		if err == nil {
			break
		}
		log.Printf("Ping error : %v", err)
		time.Sleep(3 * time.Second)
	}
	if err != nil {
		return err
	}
	tidb.db.SetMaxOpenConns(10)

	_, err = tidb.db.Exec("create database if not exists test_dml")
	return err

}

func (suite *DMLTestSuite) getOneServerRandomly() *tidbServer {
	for i := 0; i < 10; i++ {
		i := rand.Intn(suite.tidbServerCount)
		log.Printf("get number %d server",i)
		if suite.tidbServers[i] != nil {
			return suite.tidbServers[i]
		}
	}

	log.Fatalf("try to get server too many times")
	return nil
}
