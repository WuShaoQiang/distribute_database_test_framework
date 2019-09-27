package distribute_database_test_framework

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

func (suite *DMLTestSuite) startPDServers(pdEndpoints, clientAddrs []string) {
	var initialCluster []string
	for idx, pdEndpoint := range pdEndpoints {
		initialCluster = append(initialCluster, fmt.Sprintf("pd%d=http://%s", idx+1, pdEndpoint))
	}
	for i := 0; i < pdServerCount; i++ {
		name := fmt.Sprintf("pd%d", i+1)
		pd := pdServer{
			name:           name,
			dataDir:        filepath.Join(workDir, name),
			logFile:        filepath.Join(workDir, name+".log"),
			initialCluster: strings.Join(initialCluster, ","),
			peerAddr:       fmt.Sprintf("http://%s", pdEndpoints[i]),
			clientAddr:     fmt.Sprintf("http://%s", clientAddrs[i]),
		}
		err := pd.start()
		suite.Require().NoError(err)
		pdServers = append(pdServers, &pd)
	}
}

func (pd *pdServer) start() error {
	var cmd *exec.Cmd
	cmd = exec.Command(filepath.Join(binDir, "/pd-server"),
		fmt.Sprintf("--name=%s", name),
		fmt.Sprintf("--data-dir=%s", dataDir),
		fmt.Sprintf("--client-urls=%s", clientAddr),
		fmt.Sprintf("--peer-urls=%s", peerAddr),
		fmt.Sprintf("--initial-cluster=%s", initialCluster),
		fmt.Sprintf("--log-file=%s", logFile))

	Cmd = cmd
	return cmd.Start()

}

func (pd *pdServer) kill() error {
	return killProcess(pd.Process)
}
