package util

import (
	"fmt"
	"net"
	"strings"
)

func GetIP() (string, error) {
	if conn, err := net.Dial("udp", "8.8.8.8:80"); err != nil {
		return "", fmt.Errorf("error getting outbound ip: %v", err)
	} else {
		defer conn.Close()
		localAddr := conn.LocalAddr().String()
		idx := strings.LastIndex(localAddr, ":")
		return localAddr[0:idx], nil
	}

}
