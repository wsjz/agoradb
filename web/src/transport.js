// Copyright 2025 The Oikome Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// The browser node's way to Full Nodes: one WebRTC data channel per node,
// dialed "WebRTC-direct" (architecture v3 §10). The node publishes its UDP
// address and DTLS fingerprint in an access record signed with its DID; the
// Rust core checks the signature and passes both here. The browser builds
// the node's SDP answer itself — no signaling server — and accepts only a
// DTLS certificate with that fingerprint. This is the
// `HostTransportAdapter` crates/oikome-wasm/src/transport.rs calls.
//
// Messages are split into frames of at most 16 KiB:
// `[u32 big-endian request id][u8 flags][payload]`, flag 1 on the last
// frame (crates/oikome-protocol/src/webrtc.rs).

const MAX_FRAME_BYTES = 16 * 1024;
const FRAME_HEADER_BYTES = 5;
const FLAG_LAST = 1;
const MAX_MESSAGE_BYTES = 256 * 1024 * 1024;
const OPEN_TIMEOUT_MS = 10_000;
const CALL_TIMEOUT_MS = 120_000;
const HIGH_WATER = 1 << 20;
const LOW_WATER = 256 << 10;

const IPV4 = /^(\d{1,3}(?:\.\d{1,3}){3}):(\d{1,5})$/;
const IPV6 = /^\[([0-9A-Fa-f:.]+)\]:(\d{1,5})$/;
const FINGERPRINT = /^sha-256 [0-9A-Fa-f]{2}(?::[0-9A-Fa-f]{2}){31}$/;

/** `ip:port` or `[ip6]:port` → { ip, port, family }; anything else throws. */
function parseAddr(addr) {
  const v4 = IPV4.exec(addr);
  const v6 = v4 ? null : IPV6.exec(addr);
  const match = v4 ?? v6;
  const port = Number(match?.[2]);
  if (!match || !(port > 0 && port < 65536)) {
    throw new Error(`not an ip:port address: ${addr}`);
  }
  return { ip: match[1], port, family: v4 ? "IP4" : "IP6" };
}

function randomUfrag() {
  const hex = Array.from(crypto.getRandomValues(new Uint8Array(16)), (b) =>
    b.toString(16).padStart(2, "0"),
  ).join("");
  // The node names the session by it; it doubles as the ICE password,
  // which must be at least 22 characters.
  return `oikome+v1/${hex}`;
}

/** The node's SDP answer: ICE-lite, DTLS passive, one negotiated channel. */
function answerSdp({ ip, port, family }, ufrag, fingerprint) {
  return [
    "v=0",
    `o=- 0 0 IN ${family} ${ip}`,
    "s=-",
    `c=IN ${family} ${ip}`,
    "t=0 0",
    "a=ice-lite",
    `m=application ${port} UDP/DTLS/SCTP webrtc-datachannel`,
    "a=mid:0",
    "a=setup:passive",
    `a=ice-ufrag:${ufrag}`,
    `a=ice-pwd:${ufrag}`,
    `a=fingerprint:${fingerprint}`,
    "a=sctp-port:5000",
    `a=max-message-size:${MAX_FRAME_BYTES}`,
    `a=candidate:1 1 UDP 2130706431 ${ip} ${port} typ host`,
    "a=end-of-candidates",
    "",
  ].join("\r\n");
}

function frames(id, message) {
  const chunk = MAX_FRAME_BYTES - FRAME_HEADER_BYTES;
  const count = Math.max(1, Math.ceil(message.length / chunk));
  const out = [];
  for (let i = 0; i < count; i++) {
    const part = message.subarray(i * chunk, (i + 1) * chunk);
    const frame = new Uint8Array(FRAME_HEADER_BYTES + part.length);
    new DataView(frame.buffer).setUint32(0, id);
    frame[4] = i === count - 1 ? FLAG_LAST : 0;
    frame.set(part, FRAME_HEADER_BYTES);
    out.push(frame);
  }
  return out;
}

function concat(parts) {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

/** An open data channel to one node, carrying concurrent calls. */
class Connection {
  constructor(pc, channel, addr) {
    this.pc = pc;
    this.channel = channel;
    this.addr = addr;
    this.nextId = 1;
    this.pending = new Map();
    this.closed = null;
    channel.bufferedAmountLowThreshold = LOW_WATER;
    channel.onmessage = (event) => this.receive(new Uint8Array(event.data));
    channel.onclose = () => this.close(new Error(`the channel to ${addr} closed`));
    pc.onconnectionstatechange = () => {
      if (pc.connectionState === "failed" || pc.connectionState === "closed") {
        this.close(new Error(`the connection to ${addr} ${pc.connectionState}`));
      }
    };
  }

  receive(frame) {
    if (frame.length < FRAME_HEADER_BYTES) {
      this.close(new Error(`invalid frame from ${this.addr}`));
      return;
    }
    const id = new DataView(frame.buffer, frame.byteOffset).getUint32(0);
    const call = this.pending.get(id);
    if (!call) return;
    call.parts.push(frame.subarray(FRAME_HEADER_BYTES));
    call.size += frame.length - FRAME_HEADER_BYTES;
    if (call.size > MAX_MESSAGE_BYTES) {
      this.finish(id, new Error(`reply from ${this.addr} too large`));
    } else if (frame[4] & FLAG_LAST) {
      this.finish(id, null, concat(call.parts));
    }
  }

  finish(id, error, reply) {
    const call = this.pending.get(id);
    if (!call) return;
    this.pending.delete(id);
    clearTimeout(call.timer);
    if (error) call.reject(error);
    else call.resolve(reply);
  }

  async call(message) {
    if (this.closed) throw this.closed;
    const id = this.nextId;
    this.nextId = (this.nextId % 0xffffffff) + 1;
    const reply = new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => this.finish(id, new Error(`no reply from ${this.addr}`)),
        CALL_TIMEOUT_MS,
      );
      this.pending.set(id, { resolve, reject, timer, parts: [], size: 0 });
    });
    try {
      for (const frame of frames(id, message)) {
        if (this.channel.bufferedAmount > HIGH_WATER) {
          await new Promise((resolve) => {
            this.channel.addEventListener("bufferedamountlow", resolve, { once: true });
            this.channel.addEventListener("close", resolve, { once: true });
          });
        }
        if (this.closed) throw this.closed;
        this.channel.send(frame);
      }
    } catch (e) {
      // A channel that cannot be written to is done; this rejects `reply`.
      this.close(e);
    }
    return reply;
  }

  close(error) {
    if (this.closed) return;
    this.closed = error;
    for (const id of [...this.pending.keys()]) this.finish(id, error);
    this.channel.close();
    this.pc.close();
  }
}

async function connect(addr, fingerprint) {
  if (!FINGERPRINT.test(fingerprint)) {
    throw new Error(`not a sha-256 fingerprint: ${fingerprint}`);
  }
  const endpoint = parseAddr(addr);
  const pc = new RTCPeerConnection();
  const channel = pc.createDataChannel("", { negotiated: true, id: 0, ordered: true });
  channel.binaryType = "arraybuffer";
  try {
    const ufrag = randomUfrag();
    const offer = await pc.createOffer();
    const sdp = offer.sdp
      .replace(/a=ice-ufrag:[^\r\n]*/, `a=ice-ufrag:${ufrag}`)
      .replace(/a=ice-pwd:[^\r\n]*/, `a=ice-pwd:${ufrag}`);
    await pc.setLocalDescription({ type: "offer", sdp });
    await pc.setRemoteDescription({ type: "answer", sdp: answerSdp(endpoint, ufrag, fingerprint) });
    await new Promise((resolve, reject) => {
      const fail = (why) =>
        reject(new Error(`could not open a WebRTC channel to ${addr}: ${why}`));
      const timer = setTimeout(() => fail(`timed out (${pc.connectionState})`), OPEN_TIMEOUT_MS);
      channel.onopen = () => {
        clearTimeout(timer);
        resolve();
      };
      pc.onconnectionstatechange = () => {
        if (pc.connectionState === "failed") {
          clearTimeout(timer);
          fail("the connection failed (wrong fingerprint or unreachable node)");
        }
      };
    });
  } catch (e) {
    pc.close();
    throw e;
  }
  return new Connection(pc, channel, addr);
}

/** The transport: connections are opened on first use and reused. */
export function createTransport() {
  const connections = new Map();
  return {
    async call(addr, fingerprint, message) {
      const key = `${addr} ${fingerprint}`;
      let connection = connections.get(key);
      if (!connection) {
        connection = connect(addr, fingerprint);
        connections.set(key, connection);
      }
      let open;
      try {
        open = await connection;
        return await open.call(message);
      } catch (e) {
        // Dial again next time.
        if (!open || open.closed) connections.delete(key);
        throw e;
      }
    },
  };
}
