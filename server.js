import { Server } from 'socket.io';
import { randomUUID, scryptSync, randomBytes, timingSafeEqual } from 'crypto';

const PORT = process.env.PORT || 3001;
const origins = process.env.CLIENT_ORIGIN ? process.env.CLIENT_ORIGIN.split(',') : '*';
const io = new Server(PORT, { cors: { origin: origins } });

const rooms = new Map();

const makeHash = (pw, salt) => scryptSync(pw, salt, 32);
const checkPw = (r, pw) => timingSafeEqual(makeHash(pw, r.salt), r.passHash);

const getRoom = (id) => {
  if (!rooms.has(id)) {
    rooms.set(id, {
      queue: [], index: -1, playing: false, position: 0, updatedAt: Date.now(),
      members: new Map(), passHash: null, salt: null,
    });
  }
  return rooms.get(id);
};

const livePosition = (r) =>
  r.playing ? r.position + (Date.now() - r.updatedAt) / 1000 : r.position;

const publicState = (r) => ({
  queue: r.queue, index: r.index, playing: r.playing, position: livePosition(r),
});

const broadcast = (id) => {
  const r = rooms.get(id);
  if (r) io.to(id).emit('state', publicState(r));
};

const broadcastPresence = (id) => {
  const r = rooms.get(id);
  if (r) io.to(id).emit('presence', [...r.members.values()]);
};

function parseYouTubeId(input) {
  try {
    const url = new URL(input);
    if (url.hostname === 'youtu.be') return url.pathname.slice(1);
    if (url.pathname.startsWith('/shorts/')) return url.pathname.split('/')[2];
    return url.searchParams.get('v');
  } catch {
    return null;
  }
}

async function fetchTitle(videoId) {
  try {
    const res = await fetch(
      `https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v=${videoId}&format=json`
    );
    if (!res.ok) return null;
    return (await res.json()).title;
  } catch {
    return null;
  }
}

// Start playing the item at index i from the beginning
function goTo(r, i) {
  r.index = i;
  r.position = 0;
  r.playing = i >= 0;
  r.updatedAt = Date.now();
}

io.on('connection', (socket) => {
  // Only sockets that joined a room may send commands to that room
  socket.use(([event, payload], next) => {
    if (event === 'join') return next();
    if (payload?.roomId && payload.roomId === socket.data.roomId) return next();
    // otherwise silently dropped
  });

  socket.on('join', ({ roomId, name, password }, ack) => {
    if (!roomId || typeof roomId !== 'string') return ack?.({ error: 'Invalid room' });
    const isNew = !rooms.has(roomId);
    const r = getRoom(roomId);

    // First person into a new room sets its password (if they provide one)
    if (isNew && password) {
      r.salt = randomBytes(16);
      r.passHash = makeHash(password, r.salt);
    }

    if (r.passHash && !(password && checkPw(r, password))) {
      return ack?.({ error: password ? 'Wrong password' : 'This room needs a password' });
    }

    socket.data.roomId = roomId;
    socket.join(roomId);
    r.members.set(socket.id, { name: (name || 'Someone').slice(0, 30) });
    ack?.({ ok: true });
    broadcastPresence(roomId);
  });

  // Client asks for current state once its player is ready
  socket.on('sync', ({ roomId }) => {
    const r = rooms.get(roomId);
    if (!r) return;
    socket.emit('state', publicState(r));
    socket.emit('presence', [...r.members.values()]);
  });

  socket.on('add', async ({ roomId, url }, ack) => {
    const videoId = parseYouTubeId(url);
    if (!videoId) return ack?.({ error: 'Invalid YouTube link' });
    const title = await fetchTitle(videoId);
    if (!title) return ack?.({ error: 'Video not found or not embeddable' });
    const r = getRoom(roomId);
    r.queue.push({
      id: randomUUID(),
      videoId,
      title,
      addedBy: r.members.get(socket.id)?.name ?? 'Someone',
    });
    if (r.index === -1) goTo(r, 0); // first song starts automatically
    broadcast(roomId);
    ack?.({ ok: true });
  });

  socket.on('remove', ({ roomId, itemId }) => {
    const r = getRoom(roomId);
    const i = r.queue.findIndex((q) => q.id === itemId);
    if (i === -1) return;
    r.queue.splice(i, 1);
    if (r.queue.length === 0) goTo(r, -1);
    else if (i < r.index) r.index--;
    else if (i === r.index) goTo(r, Math.min(i, r.queue.length - 1)); // next song takes over
    broadcast(roomId);
  });

  socket.on('reorder', ({ roomId, itemId, toIndex }) => {
    const r = getRoom(roomId);
    const from = r.queue.findIndex((q) => q.id === itemId);
    if (from === -1 || toIndex < 0 || toIndex >= r.queue.length) return;
    const currentId = r.queue[r.index]?.id;
    const [item] = r.queue.splice(from, 1);
    r.queue.splice(toIndex, 0, item);
    r.index = r.queue.findIndex((q) => q.id === currentId); // current song stays current
    broadcast(roomId);
  });

  socket.on('play', ({ roomId }) => {
    const r = getRoom(roomId);
    if (r.index === -1) return;
    r.playing = true;
    r.updatedAt = Date.now();
    broadcast(roomId);
  });

  socket.on('pause', ({ roomId }) => {
    const r = getRoom(roomId);
    r.position = livePosition(r);
    r.playing = false;
    r.updatedAt = Date.now();
    broadcast(roomId);
  });

  socket.on('skip', ({ roomId, delta }) => {
    const r = getRoom(roomId);
    if (r.index === -1 || typeof delta !== 'number') return;
    r.position = Math.max(0, livePosition(r) + delta);
    r.updatedAt = Date.now();
    broadcast(roomId);
  });
 socket.on('seek', ({ roomId, position }) => {
  const r = getRoom(roomId);
  if (r.index === -1 || typeof position !== 'number' || !isFinite(position)) return;
  r.position = Math.max(0, position);
  r.updatedAt = Date.now();
  broadcast(roomId);
});
  socket.on('next', ({ roomId }) => {
    const r = getRoom(roomId);
    if (r.index < r.queue.length - 1) goTo(r, r.index + 1);
    broadcast(roomId);
  });

  socket.on('prev', ({ roomId }) => {
    const r = getRoom(roomId);
    if (r.index > 0 && livePosition(r) < 3) goTo(r, r.index - 1);
    else if (r.index >= 0) goTo(r, r.index); // restart current song
    broadcast(roomId);
  });

  socket.on('select', ({ roomId, itemId }) => {
    const r = getRoom(roomId);
    const i = r.queue.findIndex((q) => q.id === itemId);
    if (i !== -1) goTo(r, i);
    broadcast(roomId);
  });

  // Every client reports "ended"; only the first report for the current song counts
  socket.on('ended', ({ roomId, itemId }) => {
    const r = getRoom(roomId);
    if (r.queue[r.index]?.id !== itemId) return;
    if (r.index < r.queue.length - 1) goTo(r, r.index + 1);
    else { r.playing = false; r.position = 0; r.updatedAt = Date.now(); }
    broadcast(roomId);
  });

  socket.on('disconnect', () => {
    const id = socket.data.roomId;
    if (!id) return;
    rooms.get(id)?.members.delete(socket.id);
    broadcastPresence(id);
  });
});

console.log(`Server listening on port ${PORT}`);