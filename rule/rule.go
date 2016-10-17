package rule

import (
	"net"
)

// Rule is an inferface specifying the rule of retriving node info
type Rule interface {
	getNodes() []string
	heartBeat(conn net.Conn) boolean
}
