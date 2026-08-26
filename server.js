'use strict';
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');

const app = express();
app.use(cors()); app.use(express.json());
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const rooms = new Map(); // roomId -> Set<ws>
const history = new Map(); // roomId -> Array<msg>

function broadcast(roomId, data, excludeWs = null) {
  const room = rooms.get(roomId);
  if (!room) return;
  const msg = JSON.stringify(data);
  room.forEach(ws => { if (ws !== excludeWs && ws.readyState === WebSocket.OPEN) ws.send(msg); });
}

wss.on('connection', (ws) => {
  let roomId = null; let username = null;
  ws.send(JSON.stringify({ type: 'connected', message: 'WebSocket conectado! Envie {"type":"join","room":"sala","username":"nome"}' }));

  ws.on('message', (raw) => {
    try {
      const data = JSON.parse(raw.toString());
      if (data.type === 'join') {
        roomId = data.room || 'geral'; username = data.username || 'Anónimo';
        if (!rooms.has(roomId)) { rooms.set(roomId, new Set()); history.set(roomId, []); }
        rooms.get(roomId).add(ws);
        const hist = history.get(roomId).slice(-50);
        ws.send(JSON.stringify({ type: 'history', room: roomId, messages: hist }));
        broadcast(roomId, { type: 'user_joined', room: roomId, username, timestamp: new Date().toISOString() }, ws);
      } else if (data.type === 'message' && roomId) {
        const msg = { type: 'message', room: roomId, username, text: String(data.text || '').slice(0, 500), timestamp: new Date().toISOString() };
        history.get(roomId).push(msg);
        broadcast(roomId, msg);
      } else if (data.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong', timestamp: new Date().toISOString() }));
      }
    } catch {}
  });

  ws.on('close', () => {
    if (roomId && rooms.has(roomId)) {
      rooms.get(roomId).delete(ws);
      broadcast(roomId, { type: 'user_left', username, timestamp: new Date().toISOString() });
    }
  });
});

app.get('/', (req, res) => res.json({ service: 'WebSocket Notifications API', rooms: [...rooms.keys()], connections: wss.clients.size }));
app.get('/rooms', (req, res) => res.json({ rooms: [...rooms.entries()].map(([id, clients]) => ({ id, users: clients.size })) }));

server.listen(3000, () => console.log('⚡ WebSocket Server em ws://localhost:3000'));
