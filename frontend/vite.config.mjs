import { defineConfig } from 'vite';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { createRequire } from 'module';
import http from 'http';
import crypto from 'crypto';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const require = createRequire(import.meta.url);
const utilPolyfillPath = require.resolve('util/');

/**
 * Embedded Zero-Dependency Local Signaling & Room Server on port 8080
 * Runs completely isolated from Vite HMR on its own dedicated port.
 */
let standaloneServerInstance = null;

function ensureLocalSignalingServer() {
    if (standaloneServerInstance) {
        return;
    }

    const rooms = new Map();
    const roomSockets = new Map();
    const wsSockets = new Set();

    function encodeWsFrame(data) {
        const payload = Buffer.from(typeof data === 'string' ? data : JSON.stringify(data));
        const length = payload.length;
        let header;
        if (length < 126) {
            header = Buffer.from([0x81, length]);
        } else if (length <= 0xffff) {
            header = Buffer.from([0x81, 126, (length >> 8) & 0xff, length & 0xff]);
        } else {
            header = Buffer.alloc(10);
            header[0] = 0x81;
            header[1] = 127;
            header.writeBigUInt64BE(BigInt(length), 2);
        }
        return Buffer.concat([header, payload]);
    }

    function decodeWsFrames(buffer) {
        const frames = [];
        let offset = 0;
        while (offset < buffer.length) {
            if (offset + 2 > buffer.length) break;
            const firstByte = buffer[offset];
            const secondByte = buffer[offset + 1];
            const opcode = firstByte & 0x0f;
            const isMasked = (secondByte & 0x80) !== 0;
            let payloadLength = secondByte & 0x7f;
            let headerLength = 2;

            if (payloadLength === 126) {
                if (offset + 4 > buffer.length) break;
                payloadLength = buffer.readUInt16BE(offset + 2);
                headerLength = 4;
            } else if (payloadLength === 127) {
                if (offset + 10 > buffer.length) break;
                payloadLength = Number(buffer.readBigUInt64BE(offset + 2));
                headerLength = 10;
            }

            const maskKeyLength = isMasked ? 4 : 0;
            const totalFrameLength = headerLength + maskKeyLength + payloadLength;
            if (offset + totalFrameLength > buffer.length) break;

            const maskKey = isMasked ? buffer.subarray(offset + headerLength, offset + headerLength + 4) : null;
            const payloadOffset = offset + headerLength + maskKeyLength;
            const payload = Buffer.from(buffer.subarray(payloadOffset, payloadOffset + payloadLength));

            if (isMasked && maskKey) {
                for (let i = 0; i < payload.length; i++) {
                    payload[i] ^= maskKey[i % 4];
                }
            }

            frames.push({ opcode, payload });
            offset += totalFrameLength;
        }
        return { frames, remainder: buffer.subarray(offset) };
    }

    function sendWs(client, data) {
        if (client.socket && !client.socket.destroyed) {
            client.socket.write(encodeWsFrame(data));
        }
    }

    function handleWsMessage(client, text) {
        let msg;
        try {
            msg = JSON.parse(text);
        } catch {
            return;
        }

        switch (msg.type) {
            case 'register-details':
                client.name = msg.name || client.name;
                sendWs(client, { type: 'registered', id: client.id });
                break;

            case 'attach-room': {
                const code = (msg.roomCode || msg.flightCode || '').toUpperCase();
                client.roomCode = code;
                client.participantId = msg.participantId || client.id;

                if (!roomSockets.has(code)) {
                    roomSockets.set(code, new Set());
                }
                const subscribers = roomSockets.get(code);
                subscribers.add(client);

                const room = rooms.get(code);
                const isHost = room && room.host && (room.host.participantId === client.participantId || room.host.name === client.name);
                const role = isHost ? 'host' : 'peer';

                sendWs(client, { type: 'room-attached', flightCode: code, role });

                if (subscribers.size >= 2) {
                    const clientList = Array.from(subscribers);
                    const first = clientList[0];
                    const second = clientList[1];

                    sendWs(first, {
                        type: 'peer-joined',
                        flightCode: code,
                        connectionType: 'lan',
                        peer: {
                            name: second.name,
                            id: second.participantId,
                            participantId: second.participantId,
                            role: 'peer',
                        },
                    });

                    sendWs(second, {
                        type: 'peer-joined',
                        flightCode: code,
                        connectionType: 'lan',
                        peer: {
                            name: first.name,
                            id: first.participantId,
                            participantId: first.participantId,
                            role: 'host',
                        },
                    });
                }
                break;
            }

            case 'signal': {
                if (!client.roomCode) return;
                const subscribers = roomSockets.get(client.roomCode);
                if (!subscribers) return;
                for (const peer of subscribers) {
                    if (peer !== client) {
                        sendWs(peer, { type: 'signal', data: msg.data });
                    }
                }
                break;
            }
        }
    }

    function handleWsDisconnect(client) {
        if (!client.roomCode) return;
        const subscribers = roomSockets.get(client.roomCode);
        if (!subscribers) return;
        subscribers.delete(client);
        for (const peer of subscribers) {
            sendWs(peer, { type: 'peer-left' });
        }
        if (subscribers.size === 0) {
            roomSockets.delete(client.roomCode);
        }
    }

    function readBody(req) {
        return new Promise((resolve) => {
            let data = '';
            req.on('data', (chunk) => { data += chunk; });
            req.on('end', () => {
                try {
                    resolve(JSON.parse(data || '{}'));
                } catch {
                    resolve({});
                }
            });
        });
    }

    function sendJson(res, statusCode, data) {
        res.writeHead(statusCode, {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Methods': '*',
        });
        res.end(JSON.stringify(data));
    }

    function generateRoomCode() {
        const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
        let code = '';
        for (let i = 0; i < 6; i++) {
            code += chars[Math.floor(Math.random() * chars.length)];
        }
        return code;
    }

    const server = http.createServer(async (req, res) => {
        if (req.method === 'OPTIONS') {
            res.writeHead(204, {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Methods': '*',
            });
            return res.end();
        }

        const urlObj = new URL(req.url, 'http://localhost:8080');
        const pathname = urlObj.pathname;

        if (pathname === '/api/status') {
            return sendJson(res, 200, {
                status: 'operational',
                timestamp: new Date().toISOString(),
                uptime: Math.round(process.uptime()),
                uptimeFormatted: '1d 0h 0m 0s',
                version: '3.2.3',
                services: {
                    websocket: { status: 'operational', activeConnections: wsSockets.size },
                    signaling: { status: 'operational', activeFlights: rooms.size },
                    turn: { status: 'operational' },
                },
                stats: {
                    totalConnections: wsSockets.size,
                    totalDisconnections: 0,
                    totalFlightsCreated: rooms.size,
                    totalFlightsJoined: rooms.size,
                },
                memory: {
                    heapTotalMB: 24,
                    heapUsedMB: 18,
                    rssMB: 65,
                },
            });
        }

        if (pathname === '/api/turn-credentials') {
            return sendJson(res, 200, {
                iceServers: [
                    { urls: ['stun:stun.l.google.com:19302', 'stun:stun.cloudflare.com:3478'] },
                ],
            });
        }

        if (req.method === 'POST' && pathname === '/api/rooms') {
            const body = await readBody(req);
            const code = generateRoomCode();
            const hostId = 'p_' + Math.random().toString(36).substring(2, 9);
            const room = {
                code,
                host: {
                    participantId: hostId,
                    name: body.name || 'Anonymous',
                    role: 'host',
                    ready: false,
                },
                peer: null,
            };
            rooms.set(code, room);
            return sendJson(res, 200, {
                roomCode: code,
                status: 'waiting',
                self: room.host,
                peer: null,
                shouldConnect: false,
            });
        }

        const joinMatch = pathname.match(/^\/api\/rooms\/([^/]+)\/join$/i);
        if (req.method === 'POST' && joinMatch) {
            const code = joinMatch[1].toUpperCase();
            let room = rooms.get(code);
            if (!room) {
                const hostId = 'p_' + Math.random().toString(36).substring(2, 9);
                room = {
                    code,
                    host: { participantId: hostId, name: 'Host', role: 'host', ready: false },
                    peer: null,
                };
                rooms.set(code, room);
            }
            const body = await readBody(req);
            const peerId = 'p_' + Math.random().toString(36).substring(2, 9);
            room.peer = {
                participantId: peerId,
                name: body.name || 'Peer',
                role: 'peer',
                ready: false,
            };
            return sendJson(res, 200, {
                roomCode: code,
                status: 'connected',
                self: room.peer,
                peer: room.host,
                shouldConnect: true,
            });
        }

        const roomMatch = pathname.match(/^\/api\/rooms\/([^/]+)$/i);
        if (req.method === 'GET' && roomMatch) {
            const code = roomMatch[1].toUpperCase();
            const room = rooms.get(code);
            if (!room) {
                return sendJson(res, 404, { error: 'Flight not found' });
            }
            const participantId = urlObj.searchParams.get('participantId');
            const isHost = room.host.participantId === participantId;
            const self = isHost ? room.host : (room.peer || { participantId, name: 'Guest', role: 'peer' });
            const peer = isHost ? room.peer : room.host;
            return sendJson(res, 200, {
                roomCode: code,
                status: peer ? 'connected' : 'waiting',
                self,
                peer,
                shouldConnect: Boolean(peer),
            });
        }

        if (req.method === 'POST' && pathname.includes('/ready')) {
            return sendJson(res, 200, { ok: true, shouldConnect: true });
        }

        if (req.method === 'POST' && (pathname.includes('/chat') || pathname.includes('/screen-share'))) {
            return sendJson(res, 200, { ok: true, active: true });
        }

        sendJson(res, 404, { error: 'Not found' });
    });

    server.on('upgrade', (req, socket, head) => {
        const key = req.headers['sec-websocket-key'];
        if (!key) return;

        const acceptKey = crypto
            .createHash('sha1')
            .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
            .digest('base64');

        socket.write(
            'HTTP/1.1 101 Switching Protocols\r\n' +
            'Upgrade: websocket\r\n' +
            'Connection: Upgrade\r\n' +
            `Sec-WebSocket-Accept: ${acceptKey}\r\n\r\n`
        );

        const client = {
            id: 'ws_' + Math.random().toString(36).substring(2, 9),
            name: 'Guest',
            roomCode: null,
            participantId: null,
            socket,
            buffer: Buffer.alloc(0),
        };
        wsSockets.add(client);

        socket.on('data', (chunk) => {
            client.buffer = Buffer.concat([client.buffer, chunk]);
            const { frames, remainder } = decodeWsFrames(client.buffer);
            client.buffer = remainder;

            for (const frame of frames) {
                if (frame.opcode === 0x8) {
                    socket.end();
                    return;
                }
                if (frame.opcode === 0x9) {
                    socket.write(Buffer.from([0x8a, 0x00]));
                    continue;
                }
                if (frame.opcode === 0x1) {
                    handleWsMessage(client, frame.payload.toString('utf8'));
                }
            }
        });

        socket.on('close', () => {
            wsSockets.delete(client);
            handleWsDisconnect(client);
        });

        socket.on('error', () => {
            wsSockets.delete(client);
            handleWsDisconnect(client);
        });
    });

    server.on('error', (err) => {
        if (err.code === 'EADDRINUSE') {
            console.log('ℹ️ [DropSilk] Port 8080 already in use, reusing existing server.');
        } else {
            console.error('Signaling server error:', err);
        }
    });

    server.listen(8080, () => {
        console.log('🚀 [DropSilk] Dedicated local signaling & room server running on http://localhost:8080 and ws://localhost:8080');
    });

    standaloneServerInstance = server;
}

export default defineConfig({
    plugins: [
        {
            name: 'local-standalone-signaling',
            configureServer() {
                ensureLocalSignalingServer();
            },
        },
    ],
    resolve: {
        alias: [
            { find: /^util$/, replacement: utilPolyfillPath },
        ],
    },
    build: {
        chunkSizeWarningLimit: 2000,
        rollupOptions: {
            input: {
                main: resolve(__dirname, 'index.html'),
                not_found: resolve(__dirname, '404.html'),
                status: resolve(__dirname, 'status.html'),
            },
        },
    },
    publicDir: 'public',
    server: {
        port: 5173,
        host: true,
        proxy: {
            '/api': {
                target: 'http://localhost:8080',
                changeOrigin: true,
            },
        },
    },
    preview: {
        mode: 'development',
        port: 4173,
        host: true,
    },
});
