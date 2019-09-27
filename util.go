package distribute_database_test_framework

import (
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func getUnusedPorts(count int) (ports []int, err error) {
	ports = make([]int, 0, count)
	for i := 0; i < count; i++ {
		l, err := net.Listen("tcp", ":")
		addr := l.Addr().String()
		err = l.Close()
		if err != nil {
			return nil, err
		}
		idx := strings.LastIndex(addr, ":")
		portStr := addr[idx+1:]
		p, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, err
		}
		ports = append(ports, p)
	}

	return ports, nil
}

func isCtxError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "context")
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	return isCtxError(err) || isConnectionError(err) || isNoDatabaseError(err)
}

func isDuplicateError(err error) bool {
	return err != nil &&
		(strings.Contains(err.Error(), "Duplicate") || strings.Contains(err.Error(), "exists"))
}

func isConnectionError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "connection")
}

func isNoDatabaseError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "No database selected")
}


func killProcess(proc *os.Process) error {
	log.Printf("killing pid %d", proc.Pid)
	err := proc.Kill()
	if err != nil {
		return err
	}
	_, err = proc.Wait()
	time.Sleep(1 * time.Second)
	return err
}