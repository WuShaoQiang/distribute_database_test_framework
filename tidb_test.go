package distribute_database_test_framework

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
	for i := 0; i < tidbServerCount; i++ {
		tidb := &tidbServer{
			port:       getOnePort(),
			statusPort: getOnePort(),
			path:       strings.Join(clientAddrs, ","),
			logFile:    filepath.Join(workDir, fmt.Sprintf("tidb%d.log", i+1)),
		}
		addr = fmt.Sprintf("%s:%d", host, port)
		suite.NoError(tidb.startServer())
		tidbServers = append(tidbServers, tidb)
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
		fmt.Sprintf("--log-file=%s", logFile),
		fmt.Sprintf("--path=%s", path),
		fmt.Sprintf("-P=%d", port),
		fmt.Sprintf("--status=%d", statusPort))

	Cmd = cmd
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
		case <-time.After(randomRestartDuration):
			err := suite.getOneServerRandomly().restart()
			suite.NoError(err)
		case <-ctx.Done():
			return
		}
	}
}

func (tidb *tidbServer) restart() (err error) {
	log.Printf("restarting tidb server %s", addr)
	err = tidb.kill()
	if err != nil {
		return
	}
	return tidb.startServer()
}

func (tidb *tidbServer) connectToDatabase() (err error) {
	for i := 0; i < 5; i++ {
		log.Printf("connecting to %s", addr)
		db, err = sql.Open("mysql", fmt.Sprintf("root@(%s)/", addr))
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
		i := rand.Intn(tidbServerCount)
		log.Printf("get number %d server",i)
		if tidbServers[i] != nil {
			return tidbServers[i]
		}
	}

	log.Fatalf("try to get server too many times")
	return nil
}
