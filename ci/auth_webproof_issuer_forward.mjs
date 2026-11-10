// A loopback TCP forwarder for ci/check_auth_web_proxy_compose.sh: listens on 127.0.0.1:argv[2] (the issuer URL's
// port, 8443) and pipes each connection to 127.0.0.1:argv[3] (the proof's ephemeral auth-issuer port). It writes
// "ready" to the file argv[4] once it listens. TLS passes through unchanged: the Web code verifies the issuer itself.
import net from "node:net";
import fs from "node:fs";

const [, , listenPort, targetPort, readyFile] = process.argv;

const server = net.createServer((client) => {
    const upstream = net.connect({ host: "127.0.0.1", port: Number(targetPort) });
    client.pipe(upstream);
    upstream.pipe(client);
    const close = () => {
        client.destroy();
        upstream.destroy();
    };
    client.on("error", close);
    upstream.on("error", close);
});

server.on("error", (err) => {
    process.stderr.write(`issuer forward: ${err.message}\n`);
    process.exit(1);
});
server.listen(Number(listenPort), "127.0.0.1", () => fs.writeFileSync(readyFile, "ready\n"));
