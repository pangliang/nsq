package nsqlookupd

import (
	"io"
	"net"

	"github.com/nsqio/nsq/internal/protocol"
)

type tcpServer struct {
	ctx *Context
}

func (p *tcpServer) Handle(clientConn net.Conn) {
	p.ctx.nsqlookupd.logf("TCP: new client(%s)", clientConn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...

	//客户端连接后, 先发送四个字节是 协议版本号
	//这样设计方便的扩展协议成其他的方式, 当前使用的是 文本/换行分割 的格式
	buf := make([]byte, 4)
	_, err := io.ReadFull(clientConn, buf)
	if err != nil {
		p.ctx.nsqlookupd.logf("ERROR: failed to read protocol version - %s", err)
		return
	}
	protocolMagic := string(buf)

	p.ctx.nsqlookupd.logf("CLIENT(%s): desired protocol magic '%s'",
		clientConn.RemoteAddr(), protocolMagic)

	var prot protocol.Protocol
	switch protocolMagic {
	case "  V1":
		// 实现了 V1 版本的 Lookup 服务
		prot = &LookupProtocolV1{ctx: p.ctx}
	default:
		protocol.SendResponse(clientConn, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		p.ctx.nsqlookupd.logf("ERROR: client(%s) bad protocol magic '%s'",
			clientConn.RemoteAddr(), protocolMagic)
		return
	}
	// 进入循环处理
	err = prot.IOLoop(clientConn)
	if err != nil {
		p.ctx.nsqlookupd.logf("ERROR: client(%s) - %s", clientConn.RemoteAddr(), err)
		return
	}
}
