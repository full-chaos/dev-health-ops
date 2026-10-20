// A capturing SMTP sink for ci/check_auth_web_proxy_compose.sh: accepts plain SMTP, appends every
// message body to the file named by argv[3], and writes its listening port to the file argv[2].
import net from "node:net";
import fs from "node:fs";

const [, , portFile, mailFile] = process.argv;

const server = net.createServer((socket) => {
    let inData = false;
    let buffer = "";
    const reply = (line) => socket.write(`${line}\r\n`);
    reply("220 sink ESMTP");
    socket.on("data", (chunk) => {
        buffer += chunk.toString("latin1");
        let index;
        while (inData ? (index = buffer.indexOf("\r\n.\r\n")) >= 0 : (index = buffer.indexOf("\r\n")) >= 0) {
            if (inData) {
                fs.appendFileSync(mailFile, `${buffer.slice(0, index)}\n---MESSAGE-END---\n`);
                buffer = buffer.slice(index + 5);
                inData = false;
                reply("250 queued");
                continue;
            }
            const line = buffer.slice(0, index).toUpperCase();
            buffer = buffer.slice(index + 2);
            if (line.startsWith("EHLO") || line.startsWith("HELO")) reply("250 sink");
            else if (line.startsWith("DATA")) {
                inData = true;
                reply("354 go ahead");
            } else if (line.startsWith("QUIT")) {
                reply("221 bye");
                socket.end();
            } else reply("250 ok");
        }
    });
    socket.on("error", () => {});
});

server.listen(0, "0.0.0.0", () => {
    fs.writeFileSync(portFile, String(server.address().port));
});
