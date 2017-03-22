package session

import (
	"net"
	"bufio"
)

type Session struct {
	conn     *net.TCPConn
	br       *bufio.Reader
	bw       *bufio.Writer
	isClosed bool
}

func NewSession(conn *net.TCPConn) *Session {
	conn.SetKeepAlive(true);

	session := &Session{
		conn:conn,
		br:bufio.NewReaderSize(conn, 128),
		bw:bufio.NewWriterSize(conn, 128),
		isClosed:false,
	}
	return session;
}

func (this *Session) Read() {

}

func (this *Session) Write() {

}

func (this *Session) IsClosed() bool {
	return this.isClosed;
}

func (this *Session) Close() {
	if !this.isClosed {
		this.isClosed = true;
		this.conn.Close();
	}
}
