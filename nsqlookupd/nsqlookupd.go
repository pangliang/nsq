package nsqlookupd

import (
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/util"
	"github.com/nsqio/nsq/internal/version"
)

type NSQLookupd struct {
	sync.RWMutex
	opts         *Options
	tcpListener  net.Listener
	httpListener net.Listener
	waitGroup    util.WaitGroupWrapper
	DB           *RegistrationDB
}

func New(opts *Options) *NSQLookupd {
	n := &NSQLookupd{
		opts: opts,
		DB:   NewRegistrationDB(),
	}
	n.logf(version.String("nsqlookupd"))
	return n
}

func (l *NSQLookupd) logf(f string, args ...interface{}) {
	if l.opts.Logger == nil {
		return
	}
	l.opts.Logger.Output(2, fmt.Sprintf(f, args...))
}

func (l *NSQLookupd) Main() {
	ctx := &Context{l}

	tcpListener, err := net.Listen("tcp", l.opts.TCPAddress)
	if err != nil {
		l.logf("FATAL: listen (%s) failed - %s", l.opts.TCPAddress, err)
		os.Exit(1)
	}
	l.Lock()                //TODO  这里为什么要加锁?
	l.tcpListener = tcpListener
	l.Unlock()
	tcpServer := &tcpServer{ctx: ctx}
	l.waitGroup.Wrap(func() {
		//进入TCPServer 之前会 w.add(1) , 当收到系统信号量, program Stop 方法调用tcpListener.Close() 不在接收新请求
		//直到当前存在的连接退出之后, 整个程序才退出. 而不是强行断开
		protocol.TCPServer(tcpListener, tcpServer, l.opts.Logger)
	})

	httpListener, err := net.Listen("tcp", l.opts.HTTPAddress)
	if err != nil {
		l.logf("FATAL: listen (%s) failed - %s", l.opts.HTTPAddress, err)
		os.Exit(1)
	}
	l.Lock()
	l.httpListener = httpListener
	l.Unlock()
	httpServer := newHTTPServer(ctx)
	l.waitGroup.Wrap(func() {
		http_api.Serve(httpListener, httpServer, "HTTP", l.opts.Logger)
	})
}

func (l *NSQLookupd) RealTCPAddr() *net.TCPAddr {
	l.RLock()
	defer l.RUnlock()
	return l.tcpListener.Addr().(*net.TCPAddr)
}

func (l *NSQLookupd) RealHTTPAddr() *net.TCPAddr {
	l.RLock()
	defer l.RUnlock()
	return l.httpListener.Addr().(*net.TCPAddr)
}

func (l *NSQLookupd) Exit() {
	if l.tcpListener != nil {
		l.tcpListener.Close()
	}

	if l.httpListener != nil {
		l.httpListener.Close()
	}
	l.waitGroup.Wait()
}
