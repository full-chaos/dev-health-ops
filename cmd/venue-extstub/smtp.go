package main

import "net"

const smtpHost = "smtp"

// serveSMTP: skeleton, no behaviour yet.
func serveSMTP(ln net.Listener, log *hitLog) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		conn.Close()
	}
}
