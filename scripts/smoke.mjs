// 冒烟脚本：对运行中的 chat_echo 例程做端到端验证。
// 用法：go run ./examples/chat_echo（另一终端）→ node scripts/smoke.mjs [port]
import net from "node:net";
import crypto from "node:crypto";

const port = Number(process.argv[2] ?? 7792);
const sock = net.connect({ host: "127.0.0.1", port });
const key = crypto.randomBytes(16).toString("base64");
let buf = Buffer.alloc(0);
let hs = false;
const msgs = [];

sock.on("connect", () =>
  sock.write(
    `GET / HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: ${key}\r\nSec-WebSocket-Version: 13\r\n\r\n`,
  ),
);
sock.on("data", (c) => {
  buf = Buffer.concat([buf, c]);
  if (!hs) {
    const i = buf.indexOf("\r\n\r\n");
    if (i < 0) return;
    if (!buf.subarray(0, i).toString().includes("101")) throw new Error("hs fail");
    buf = buf.subarray(i + 4);
    hs = true;
    const p = Buffer.from("hello");
    const m = crypto.randomBytes(4);
    const x = Buffer.from(p);
    for (let i = 0; i < x.length; i++) x[i] ^= m[i % 4];
    sock.write(Buffer.concat([Buffer.from([0x81, 0x80 | p.length]), m, x]));
    return;
  }
  for (;;) {
    if (buf.length < 2) return;
    const op = buf[0] & 0xf;
    let len = buf[1] & 0x7f;
    let off = 2;
    if (len === 126) {
      if (buf.length < 4) return;
      len = buf.readUInt16BE(2);
      off = 4;
    } else if (len === 127) {
      if (buf.length < 10) return;
      len = Number(buf.readBigUInt64BE(2));
      off = 10;
    }
    if (buf.length < off + len) return;
    const pl = buf.subarray(off, off + len).toString();
    buf = buf.subarray(off + len);
    if (op === 1) msgs.push(JSON.parse(pl));
  }
});

const dl = Date.now() + 5000;
while (Date.now() < dl && msgs.filter((m) => m.type === "event").length < 2) {
  await new Promise((r) => setTimeout(r, 50));
}
if (!msgs.some((m) => m.type === "echo" && m.text === "hello")) {
  throw new Error("echo missing: " + JSON.stringify(msgs));
}
if (msgs.filter((m) => m.type === "event" && m.topic === "beep").length < 1) {
  throw new Error("event missing");
}
console.log("[smoke] ok:", JSON.stringify(msgs));
const res = await fetch(`http://127.0.0.1:${port}/`);
const body = await res.text();
if (res.status !== 404 || !body.includes("troupe-gateway")) {
  throw new Error(`fallback: ${res.status} ${body}`);
}
console.log("[smoke] fallback ok");
sock.destroy();
process.exit(0);
