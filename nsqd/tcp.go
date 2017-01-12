package nsqd

import (
	"io"
	"net"

	"github.com/nsqio/nsq/internal/protocol"
)

type tcpServer struct {
	ctx *context
}

// 因为考虑可能以后需要变更协议, 所以, tcpServer 再次做了一个封装, 不同的协议版本, 由 tcpServer交给 不同的 'Protocal实现' 进行处理
func (p *tcpServer) Handle(clientConn net.Conn) {
	p.ctx.nsqd.logf("TCP: new client(%s)", clientConn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	// 先读取4个字节 作为 协议版本号
	buf := make([]byte, 4)
	_, err := io.ReadFull(clientConn, buf)
	if err != nil {
		p.ctx.nsqd.logf("ERROR: failed to read protocol version - %s", err)
		return
	}
	protocolMagic := string(buf)

	p.ctx.nsqd.logf("CLIENT(%s): desired protocol magic '%s'",
		clientConn.RemoteAddr(), protocolMagic)

	var prot protocol.Protocol
	switch protocolMagic {
	case "  V2":
		// protocolV2 实现了 V2 这个版本的协议
		prot = &protocolV2{ctx: p.ctx}
	//假如有另外一个版本的协议
	//case "  V3":
	//	prot = &protocolV3{ctx: p.ctx}
	default:
		protocol.SendFramedResponse(clientConn, frameTypeError, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		p.ctx.nsqd.logf("ERROR: client(%s) bad protocol magic '%s'",
			clientConn.RemoteAddr(), protocolMagic)
		return
	}

	//交给具体实现处理
	err = prot.IOLoop(clientConn)
	if err != nil {
		p.ctx.nsqd.logf("ERROR: client(%s) - %s", clientConn.RemoteAddr(), err)
		return
	}
}
