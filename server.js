const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT) || 3000;
const ROOT = __dirname;
const questions = JSON.parse(fs.readFileSync(path.join(ROOT, 'questions.json'), 'utf8'));
const rooms = new Map();
const MAX_PLAYERS = 8;
const TOTAL_ROUNDS = 5;
const WRITE_MS = 60_000;
const VOTE_MS = 30_000;
const REVEAL_MS = 15_000;

const server = http.createServer((req, res) => {
  let requested = decodeURIComponent((req.url || '/').split('?')[0]);
  if (requested === '/') requested = '/index.html';
  const filename = path.resolve(ROOT, 'public', `.${requested}`);
  if (!filename.startsWith(path.resolve(ROOT, 'public') + path.sep)) {
    res.writeHead(403).end('Forbidden'); return;
  }
  fs.readFile(filename, (err, data) => {
    if (err) { res.writeHead(404).end('Not found'); return; }
    const ext = path.extname(filename);
    const types = { '.html': 'text/html; charset=utf-8', '.css': 'text/css', '.js': 'text/javascript', '.json': 'application/json' };
    res.writeHead(200, { 'Content-Type': types[ext] || 'application/octet-stream', 'Cache-Control': 'no-store' }).end(data);
  });
});
const wss = new WebSocketServer({ server });

function code() {
  const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let value;
  do { value = Array.from({ length: 5 }, () => alphabet[crypto.randomInt(alphabet.length)]).join(''); } while (rooms.has(value));
  return value;
}
function cleanName(value) { return String(value || '').trim().replace(/\s+/g, ' ').slice(0, 18); }
function cleanAnswer(value) { return String(value || '').trim().replace(/\s+/g, ' ').slice(0, 100); }
function normalized(value) { return cleanAnswer(value).trim().toLocaleLowerCase(); }
function send(socket, payload) { if (socket && socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(payload)); }
function playerFor(room, id) { return room?.players.find(p => p.id === id); }
function clearTimer(room) { if (room.timer) clearTimeout(room.timer); room.timer = null; }
function publicRoom(room, viewerId) {
  const players = room.players.map(p => ({ id: p.id, name: p.name, score: p.score, connected: !!p.socket }));
  const state = { roomCode: room.code, hostId: room.hostId, phase: room.phase, round: room.round, totalRounds: TOTAL_ROUNDS, players,
    endsAt: room.endsAt || null, isHost: viewerId === room.hostId };
  if (room.phase === 'lobby') state.canStart = room.players.length >= 2;
  if (room.phase === 'writing') {
    state.question = room.currentQuestion.question;
    state.submitted = room.players.filter(p => room.submissions.has(p.id)).length;
    state.youSubmitted = room.submissions.has(viewerId);
  }
  if (room.phase === 'voting') {
    state.question = room.currentQuestion.question;
    state.choices = room.choices.map(c => ({ id: c.id, text: c.text }));
    state.voted = room.votes.size;
    state.youVoted = room.votes.has(viewerId);
    state.eligible = room.voters.includes(viewerId) && !room.votes.has(viewerId);
    state.truthAlreadyFound = room.truthFound.includes(viewerId);
    state.ownChoiceIds = room.choices.filter(c => c.authorIds.includes(viewerId)).map(c => c.id);
  }
  if (room.phase === 'reveal' || room.phase === 'finished') {
    state.question = room.currentQuestion.question;
    state.answer = room.currentQuestion.answer;
    state.choices = room.choices.map(c => ({ id: c.id, text: c.text, authors: c.authorIds.map(id => playerFor(room, id)?.name || 'Player'), voteNames: c.voteIds.map(id => playerFor(room, id)?.name || 'Player'), real: c.real }));
    state.truthFoundNames = room.truthFound.map(id => playerFor(room, id)?.name || 'Player');
    state.lastRoundScores = room.lastRoundScores || [];
    if (room.phase === 'reveal') state.nextIn = Math.max(0, Math.ceil((room.endsAt - Date.now()) / 1000));
  }
  return state;
}
function broadcast(room) { for (const p of room.players) if (p.socket) send(p.socket, { type: 'state', state: publicRoom(room, p.id) }); }
function notify(room, message) { for (const p of room.players) send(p.socket, { type: 'notice', message }); }
function schedule(room, ms, fn) { clearTimer(room); room.endsAt = Date.now() + ms; room.timer = setTimeout(fn, ms); }

function startGame(room) {
  if (room.players.length < 2 || room.players.length > MAX_PLAYERS || room.phase !== 'lobby') return;
  room.deck = [...questions];
  for (let i = room.deck.length - 1; i > 0; i--) { const j = crypto.randomInt(i + 1); [room.deck[i], room.deck[j]] = [room.deck[j], room.deck[i]]; }
  room.round = 0;
  nextRound(room);
}
function nextRound(room) {
  clearTimer(room);
  if (room.round >= TOTAL_ROUNDS) { room.phase = 'finished'; room.endsAt = null; broadcast(room); return; }
  room.round++;
  room.currentQuestion = room.deck[room.round - 1];
  room.submissions = new Map(); room.votes = new Map(); room.choices = []; room.truthFound = []; room.voters = [];
  room.lastRoundScores = [];
  room.phase = 'writing';
  schedule(room, WRITE_MS, () => beginVoting(room));
  broadcast(room);
}
function beginVoting(room) {
  if (room.phase !== 'writing') return;
  const groups = new Map();
  room.truthFound = [];
  for (const [playerId, answer] of room.submissions) {
    if (normalized(answer) === normalized(room.currentQuestion.answer)) {
      room.truthFound.push(playerId);
      const p = playerFor(room, playerId); if (p) p.score += 3;
      continue;
    }
    const key = normalized(answer);
    if (!groups.has(key)) groups.set(key, { id: crypto.randomUUID(), text: answer, authorIds: [], voteIds: [], real: false });
    groups.get(key).authorIds.push(playerId);
  }
  room.choices = [...groups.values(), { id: crypto.randomUUID(), text: room.currentQuestion.answer, authorIds: [], voteIds: [], real: true }];
  for (let i = room.choices.length - 1; i > 0; i--) { const j = crypto.randomInt(i + 1); [room.choices[i], room.choices[j]] = [room.choices[j], room.choices[i]]; }
  room.voters = room.players.filter(p => !room.truthFound.includes(p.id)).map(p => p.id);
  room.phase = 'voting';
  if (room.voters.length === 0) { resolveRound(room); return; }
  schedule(room, VOTE_MS, () => resolveRound(room));
  broadcast(room);
}
function resolveRound(room) {
  if (room.phase !== 'voting') return;
  clearTimer(room);
  const startScores = new Map(room.players.map(p => [p.id, p.score]));
  for (const [voterId, choiceId] of room.votes) {
    const choice = room.choices.find(c => c.id === choiceId);
    if (!choice) continue;
    choice.voteIds.push(voterId);
    if (choice.real) { const voter = playerFor(room, voterId); if (voter) voter.score += 3; }
    else for (const authorId of choice.authorIds) { const author = playerFor(room, authorId); if (author) author.score += 2; }
  }
  room.lastRoundScores = room.players.map(p => ({ id: p.id, name: p.name, gained: p.score - startScores.get(p.id), score: p.score }));
  room.phase = 'reveal';
  schedule(room, REVEAL_MS, () => nextRound(room));
  broadcast(room);
}
function removePlayer(room, player) {
  room.players = room.players.filter(p => p.id !== player.id);
  if (!room.players.length) { clearTimer(room); rooms.delete(room.code); return; }
  if (room.hostId === player.id) room.hostId = room.players[0].id;
  if (room.phase === 'writing' && room.players.every(p => room.submissions.has(p.id))) beginVoting(room);
  else if (room.phase === 'voting' && room.voters.every(id => room.votes.has(id) || !playerFor(room, id))) resolveRound(room);
  broadcast(room);
}
function getSocketPlayer(socket) { return socket.playerRef ? { room: socket.playerRef.room, player: socket.playerRef.player } : null; }

wss.on('connection', socket => {
  socket.on('message', raw => {
    let msg; try { msg = JSON.parse(raw.toString()); } catch { send(socket, { type: 'error', message: 'That message was not valid.' }); return; }
    if (msg.type === 'create') {
      const name = cleanName(msg.name); if (!name) return send(socket, { type: 'error', message: 'Enter a name first.' });
      const roomCode = code(), id = crypto.randomUUID(), token = crypto.randomUUID();
      const room = { code: roomCode, hostId: id, players: [{ id, token, name, score: 0, socket }], phase: 'lobby', round: 0, timer: null };
      socket.playerRef = { room, player: room.players[0] }; rooms.set(roomCode, room);
      send(socket, { type: 'joined', token, roomCode }); broadcast(room); return;
    }
    if (msg.type === 'join') {
      const room = rooms.get(String(msg.roomCode || '').toUpperCase());
      const name = cleanName(msg.name); if (!room) return send(socket, { type: 'error', message: 'Room not found. Check the code and try again.' });
      const returning = room.players.find(p => p.token === msg.token);
      if (returning) {
        if (returning.socket && returning.socket !== socket) returning.socket.close();
        returning.socket = socket; socket.playerRef = { room, player: returning };
        send(socket, { type: 'joined', token: returning.token, roomCode: room.code }); broadcast(room); return;
      }
      if (room.phase !== 'lobby') return send(socket, { type: 'error', message: 'This game has already started.' });
      if (!name) return send(socket, { type: 'error', message: 'Enter a name first.' });
      if (room.players.length >= MAX_PLAYERS) return send(socket, { type: 'error', message: 'This room is full (8 players).' });
      if (room.players.some(p => p.name.toLowerCase() === name.toLowerCase())) return send(socket, { type: 'error', message: 'That name is already in this room.' });
      const player = { id: crypto.randomUUID(), token: crypto.randomUUID(), name, score: 0, socket };
      room.players.push(player); socket.playerRef = { room, player };
      send(socket, { type: 'joined', token: player.token, roomCode: room.code }); broadcast(room); return;
    }
    const ref = getSocketPlayer(socket); if (!ref) return send(socket, { type: 'error', message: 'Join a room first.' });
    const { room, player } = ref;
    if (msg.type === 'start') {
      if (room.hostId !== player.id) return send(socket, { type: 'error', message: 'Only the host can start the game.' });
      if (room.players.length < 2) return send(socket, { type: 'error', message: 'At least two players are needed.' });
      startGame(room);
    } else if (msg.type === 'submit') {
      if (room.phase !== 'writing' || room.submissions.has(player.id)) return;
      const answer = cleanAnswer(msg.answer); if (!answer) return send(socket, { type: 'error', message: 'Write an answer first.' });
      room.submissions.set(player.id, answer);
      if (room.players.every(p => room.submissions.has(p.id))) beginVoting(room); else broadcast(room);
    } else if (msg.type === 'vote') {
      if (room.phase !== 'voting' || !room.voters.includes(player.id) || room.votes.has(player.id)) return;
      const choice = room.choices.find(c => c.id === msg.choiceId);
      if (!choice || choice.authorIds.includes(player.id)) return send(socket, { type: 'error', message: 'Choose an eligible answer.' });
      room.votes.set(player.id, choice.id);
      if (room.voters.every(id => room.votes.has(id))) resolveRound(room); else broadcast(room);
    } else if (msg.type === 'next') {
      if (room.hostId !== player.id || room.phase !== 'reveal') return send(socket, { type: 'error', message: 'Only the host can advance after a round.' });
      nextRound(room);
    } else if (msg.type === 'leave') {
      socket.playerRef = null; removePlayer(room, player);
    }
  });
  socket.on('close', () => {
    const ref = getSocketPlayer(socket); if (!ref) return;
    if (ref.player.socket === socket) { ref.player.socket = null; broadcast(ref.room); }
  });
});

server.listen(PORT, '0.0.0.0', () => console.log(`Fact or Fiction listening on http://localhost:${PORT}`));
