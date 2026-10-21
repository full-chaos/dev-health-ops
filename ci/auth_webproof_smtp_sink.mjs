// A capturing SMTP sink for ci/check_auth_web_proxy_compose.sh: accepts plain SMTP, appends every
// message body to the file named by argv[3], and writes its listening port to the file argv[2].
import net from "node:net";
import fs from "node:fs";

const [, , portFile, mailFile] = process.argv;
const tokenFile = `${mailFile}.token`;

const decodeQuotedPrintable = (text) =>
    text.replace(/=\r?\n/g, "").replace(/=([0-9A-Fa-f]{2})/g, (_, hex) => String.fromCharCode(parseInt(hex, 16)));

const readableBody = (message) => {
    const split = message.search(/\r?\n\r?\n/);
    const head = split < 0 ? message : message.slice(0, split);
    const body = split < 0 ? "" : message.slice(split).trim();
    if (/^content-transfer-encoding:\s*base64/im.test(head)) {
        return Buffer.from(body.replace(/\s+/g, ""), "base64").toString("latin1");
    }
    return decodeQuotedPrintable(body);
};

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
                const message = buffer.slice(0, index);
                fs.appendFileSync(mailFile, `${message}\n---MESSAGE-END---\n`);
                const found = /verify\?token=([A-Za-z0-9._~%-]+)/.exec(readableBody(message));
                if (found) fs.writeFileSync(tokenFile, found[1]);
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
