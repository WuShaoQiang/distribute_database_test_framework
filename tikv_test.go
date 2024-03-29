package distribute_database_test_framework

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

func (suite *DMLTestSuite) startTiKVServers(clientAddrs []string) {
	for i := 0; i < suite.tikvServerCount; i++ {
		tikv := tikvServer{
			pdEndpoints: strings.Join(clientAddrs, ","),
			addr:        fmt.Sprintf("%s:%d", "127.0.0.1", suite.getOnePort()),
			dataDir:     filepath.Join(workDir, fmt.Sprintf("tikv%d", i+1)),
			logFile:     filepath.Join(workDir, fmt.Sprintf("tikv%d.log", i+1)),
		}
		err := tikv.start()
		suite.Require().NoError(err)
		suite.tikvServers = append(suite.tikvServers, &tikv)
	}
}

func (tikv *tikvServer) start() error {
	var cmd *exec.Cmd
	cmd = exec.Command(filepath.Join(binDir, "/tikv-server"),
		fmt.Sprintf("--pd-endpoints=%s", tikv.pdEndpoints),
		fmt.Sprintf("--addr=%s", tikv.addr),
		fmt.Sprintf("--data-dir=%s", tikv.dataDir),
		fmt.Sprintf("--log-file=%s", tikv.logFile))

	tikv.Cmd = cmd
	return cmd.Start()
}

func (tikv *tikvServer) kill() error {
	return killProcess(tikv.Process)
}
