package distribute_database_test_framework

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

func (suite *DMLTestSuite) startTiKVServers(clientAddrs []string) {
	for i := 0; i < tikvServerCount; i++ {
		tikv := tikvServer{
			pdEndpoints: strings.Join(clientAddrs, ","),
			addr:        fmt.Sprintf("%s:%d", "127.0.0.1", getOnePort()),
			dataDir:     filepath.Join(workDir, fmt.Sprintf("tikv%d", i+1)),
			logFile:     filepath.Join(workDir, fmt.Sprintf("tikv%d.log", i+1)),
		}
		err := tikv.start()
		suite.Require().NoError(err)
		tikvServers = append(tikvServers, &tikv)
	}
}

func (tikv *tikvServer) start() error {
	var cmd *exec.Cmd
	cmd = exec.Command(filepath.Join(binDir, "/tikv-server"),
		fmt.Sprintf("--pd-endpoints=%s", pdEndpoints),
		fmt.Sprintf("--addr=%s", addr),
		fmt.Sprintf("--data-dir=%s", dataDir),
		fmt.Sprintf("--log-file=%s", logFile))

	Cmd = cmd
	return cmd.Start()
}

func (tikv *tikvServer) kill() error {
	return killProcess(tikv.Process)
}
