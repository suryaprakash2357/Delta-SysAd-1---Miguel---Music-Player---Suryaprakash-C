import socket
import threading
import json
import struct
import os
import time
import subprocess
from mutagen.mp3 import MP3
from mutagen.flac import FLAC
from mutagen.oggvorbis import OggVorbis
from database import init_database, fetch_all, fetch_one, execute_query
from auth import register_user, authenticate_user

STREAM_BITRATE = 128_000
BYTES_PER_SECOND = STREAM_BITRATE // 8
CHUNK_SIZE = 8192
CHUNK_DURATION = CHUNK_SIZE / BYTES_PER_SECOND

class MusicServer:
    def __init__(self):
        init_database()
        self.server = None
        self.running = False
        self.sessions = {}
        self.lock = threading.Lock()

    def start(self):
        self.scan_library()

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(('0.0.0.0', 9000))
        self.server.listen(10)
        self.running = True

        print("Server running on port 9000")

        scanner = threading.Thread(target=self.auto_scanner, daemon=True)
        scanner.start()

        while self.running:
            try:
                client, addr = self.server.accept()
                print(f"Connected: {addr[0]}")
                thread = threading.Thread(target=self.handle_client, args=(client, addr))
                thread.daemon = True
                thread.start()
            except:
                break

    def get_duration(self, filepath):
        try:
            ext = os.path.splitext(filepath)[1].lower()
            if ext == '.mp3':
                return MP3(filepath).info.length
            elif ext == '.flac':
                return FLAC(filepath).info.length
            elif ext == '.ogg':
                return OggVorbis(filepath).info.length
        except:
            pass
        return 0.0

    def scan_library(self):
        folder = os.path.join(os.path.dirname(__file__), 'music_library')
        if not os.path.exists(folder):
            os.makedirs(folder)
            return
        for filename in os.listdir(folder):
            if filename.lower().endswith(('.mp3', '.flac', '.ogg', '.wav', '.m4a')):
                filepath = os.path.join(folder, filename).replace('\\', '/')
                exists = fetch_one("SELECT id FROM tracks WHERE filepath=?", (filepath,))
                if not exists:
                    title = os.path.splitext(filename)[0]
                    duration = self.get_duration(filepath)
                    execute_query(
                        "INSERT INTO tracks (filepath, title, artist, album, genre, duration) VALUES (?, ?, ?, ?, ?, ?)",
                        (filepath, title, 'Unknown', 'Unknown', 'Unknown', duration)
                    )
                    print(f"Added: {title} ({duration:.1f}s)")

    def auto_scanner(self):
        while self.running:
            time.sleep(30)
            self.scan_library()

    def send_message(self, sock, data):
        try:
            msg = json.dumps(data, default=str).encode('utf-8')
            header = struct.pack('!I', len(msg))
            sock.sendall(header + msg)
        except:
            pass

    def recv_message(self, sock):
        try:
            header = sock.recv(4)
            if not header:
                return None
            length = struct.unpack('!I', header)[0]
            data = b''
            while len(data) < length:
                chunk = sock.recv(length - len(data))
                if not chunk:
                    return None
                data += chunk
            return json.loads(data.decode('utf-8'))
        except:
            return None

    def stop_ffmpeg(self, session_id):
        if session_id and session_id in self.sessions:
            session = self.sessions[session_id]
            proc = session.get('ffmpeg_proc')
            if proc:
                try:
                    proc.stdout.close()
                except:
                    pass
                try:
                    proc.terminate()
                    proc.wait(timeout=2)
                except:
                    pass
                session['ffmpeg_proc'] = None

            stream_thread = session.get('stream_thread')
            if stream_thread and stream_thread.is_alive():
                stream_thread.join(timeout=2)
                session['stream_thread'] = None

    def stream_with_ffmpeg(self, sock, filepath, start_time, stop_event, session_id):
        cmd = [
            'ffmpeg',
            '-ss', str(start_time),
            '-i', filepath,
            '-f', 'mp3',
            '-acodec', 'libmp3lame',
            '-b:a', f'{STREAM_BITRATE // 1000}k',
            '-'
        ]
        proc = None
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if session_id and session_id in self.sessions:
                self.sessions[session_id]['ffmpeg_proc'] = proc

            while not stop_event.is_set():
                chunk = proc.stdout.read(CHUNK_SIZE)
                if not chunk:
                    break
                header = struct.pack('!I', len(chunk))
                try:
                    sock.sendall(header + chunk)
                except:
                    break

                if session_id in self.sessions:
                    with self.sessions[session_id]['lock']:
                        self.sessions[session_id]['bytes_sent'] += len(chunk)

                time.sleep(CHUNK_DURATION)

            if not stop_event.is_set():
                self.send_message(sock, {"type": "stream_end"})

        except Exception as e:
            print(f"Stream error: {e}")
        finally:
            if proc:
                try:
                    proc.stdout.close()
                except:
                    pass
                try:
                    proc.terminate()
                except:
                    pass
            if session_id and session_id in self.sessions:
                if self.sessions[session_id].get('ffmpeg_proc') == proc:
                    self.sessions[session_id]['ffmpeg_proc'] = None

    def handle_client(self, sock, addr):
        session_id = None
        user_id = None

        try:
            while True:
                msg = self.recv_message(sock)
                if not msg:
                    break

                msg_type = msg.get('type')

                if msg_type == 'register':
                    result = register_user(msg['username'], msg['password'])
                    result['type'] = 'register_result'
                    self.send_message(sock, result)

                elif msg_type == 'login':
                    result = authenticate_user(msg['username'], msg['password'])
                    result['type'] = 'login_result'
                    if result['status'] == 'ok':
                        session_id = result['session_id']
                        user_id = result['user_id']
                        with self.lock:
                            self.sessions[session_id] = {
                                'user_id': user_id,
                                'queue': [],
                                'current_track': None,
                                'ffmpeg_proc': None,
                                'bytes_sent': 0,
                                'stop_event': None,
                                'stream_thread': None,
                                'lock': threading.Lock()
                            }
                    self.send_message(sock, result)

                elif msg_type == 'get_library':
                    tracks = fetch_all("SELECT id, title, artist, album, duration FROM tracks ORDER BY title")
                    track_list = [{'id': t['id'], 'title': t['title'], 'artist': t['artist'],
                                   'album': t['album'], 'duration': t['duration']} for t in tracks] if tracks else []
                    self.send_message(sock, {"type": "library", "tracks": track_list})

                elif msg_type == 'search':
                    query = f"%{msg.get('query', '')}%"
                    tracks = fetch_all(
                        "SELECT id, title, artist, album, duration FROM tracks WHERE title LIKE ? OR artist LIKE ? ORDER BY title",
                        (query, query)
                    )
                    track_list = [{'id': t['id'], 'title': t['title'], 'artist': t['artist'],
                                   'album': t['album'], 'duration': t['duration']} for t in tracks] if tracks else []
                    self.send_message(sock, {"type": "search_results", "tracks": track_list})

                elif msg_type == 'play':
                    track_id = msg.get('track_id')
                    track = fetch_one("SELECT * FROM tracks WHERE id=?", (track_id,))
                    if track and os.path.exists(track['filepath']):
                        self.stop_ffmpeg(session_id)
                        if session_id in self.sessions:
                            if self.sessions[session_id].get('stop_event'):
                                self.sessions[session_id]['stop_event'].set()

                        self.send_message(sock, {"type": "stop_audio"})
                        time.sleep(0.1)

                        track_info = {'id': track['id'], 'title': track['title'],
                                      'artist': track['artist'], 'album': track['album'],
                                      'duration': track['duration']}
                        self.send_message(sock, {"type": "now_playing", "track": track_info})

                        if session_id in self.sessions:
                            with self.sessions[session_id]['lock']:
                                self.sessions[session_id]['bytes_sent'] = 0
                                self.sessions[session_id]['current_track'] = track_info

                        stop_event = threading.Event()
                        stream_thread = threading.Thread(
                            target=self.stream_with_ffmpeg,
                            args=(sock, track['filepath'], 0.0, stop_event, session_id)
                        )
                        stream_thread.daemon = True
                        stream_thread.start()

                        if session_id in self.sessions:
                            self.sessions[session_id]['stop_event'] = stop_event
                            self.sessions[session_id]['stream_thread'] = stream_thread

                        if user_id:
                            execute_query("INSERT INTO history (user_id, track_id) VALUES (?, ?)", (user_id, track_id))
                    else:
                        self.send_message(sock, {"type": "error", "message": "Track not found"})

                elif msg_type == 'pause':
                    if session_id in self.sessions:
                        if self.sessions[session_id].get('stop_event'):
                            self.sessions[session_id]['stop_event'].set()
                        self.stop_ffmpeg(session_id)
                    self.send_message(sock, {"type": "paused"})

                elif msg_type == 'resume':
                    if session_id in self.sessions:
                        session = self.sessions[session_id]
                        track = session.get('current_track')
                        if track:
                            track_full = fetch_one("SELECT * FROM tracks WHERE id=?", (track['id'],))
                            if track_full and os.path.exists(track_full['filepath']):
                                with session['lock']:
                                    bytes_sent = session['bytes_sent']
                                seek_time = bytes_sent / BYTES_PER_SECOND

                                self.send_message(sock, {"type": "stop_audio"})
                                time.sleep(0.1)
                                self.send_message(sock, {"type": "resumed"})

                                stop_event = threading.Event()
                                stream_thread = threading.Thread(
                                    target=self.stream_with_ffmpeg,
                                    args=(sock, track_full['filepath'], seek_time, stop_event, session_id)
                                )
                                stream_thread.daemon = True
                                stream_thread.start()

                                session['stop_event'] = stop_event
                                session['stream_thread'] = stream_thread

                elif msg_type == 'next':
                    self.stop_ffmpeg(session_id)
                    if session_id in self.sessions:
                        if self.sessions[session_id].get('stop_event'):
                            self.sessions[session_id]['stop_event'].set()

                    if session_id and session_id in self.sessions:
                        queue = self.sessions[session_id].get('queue', [])
                        if queue:
                            next_track_id = queue.pop(0)
                            track = fetch_one("SELECT * FROM tracks WHERE id=?", (next_track_id,))
                            if track and os.path.exists(track['filepath']):
                                track_info = {'id': track['id'], 'title': track['title'],
                                              'artist': track['artist'], 'album': track['album'],
                                              'duration': track['duration']}

                                self.send_message(sock, {"type": "stop_audio"})
                                time.sleep(0.1)
                                self.send_message(sock, {"type": "now_playing", "track": track_info})
                                self.send_message(sock, {"type": "queue_updated", "queue": queue})

                                if session_id in self.sessions:
                                    with self.sessions[session_id]['lock']:
                                        self.sessions[session_id]['bytes_sent'] = 0
                                        self.sessions[session_id]['current_track'] = track_info

                                stop_event = threading.Event()
                                stream_thread = threading.Thread(
                                    target=self.stream_with_ffmpeg,
                                    args=(sock, track['filepath'], 0.0, stop_event, session_id)
                                )
                                stream_thread.daemon = True
                                stream_thread.start()

                                if session_id in self.sessions:
                                    self.sessions[session_id]['stop_event'] = stop_event
                                    self.sessions[session_id]['stream_thread'] = stream_thread

                                if user_id:
                                    execute_query("INSERT INTO history (user_id, track_id) VALUES (?, ?)", (user_id, next_track_id))
                                continue
                    self.send_message(sock, {"type": "queue_empty"})

                elif msg_type == 'add_to_queue':
                    track_id = msg.get('track_id')
                    if session_id and session_id in self.sessions:
                        queue = self.sessions[session_id].get('queue', [])
                        if track_id not in queue:
                            queue.append(track_id)
                        self.send_message(sock, {"type": "queue_updated", "queue": queue})

                elif msg_type == 'remove_from_queue':
                    track_id = msg.get('track_id')
                    if session_id and session_id in self.sessions:
                        queue = self.sessions[session_id].get('queue', [])
                        if track_id in queue:
                            queue.remove(track_id)
                        self.send_message(sock, {"type": "queue_updated", "queue": queue})

                elif msg_type == 'get_queue':
                    queue = self.sessions.get(session_id, {}).get('queue', []) if session_id else []
                    self.send_message(sock, {"type": "queue", "queue": queue})

                elif msg_type == 'get_playlists':
                    playlists = []
                    if user_id:
                        pls = fetch_all("SELECT id, name FROM playlists WHERE user_id=?", (user_id,))
                        if pls:
                            playlists = [{'id': p['id'], 'name': p['name']} for p in pls]
                    self.send_message(sock, {"type": "playlists", "playlists": playlists})

                elif msg_type == 'create_playlist':
                    if user_id:
                        name = msg.get('name', 'New Playlist')
                        execute_query("INSERT INTO playlists (user_id, name) VALUES (?, ?)", (user_id, name))
                        result = fetch_one("SELECT last_insert_rowid() as id")
                        playlist_id = result['id'] if result else 0
                        self.send_message(sock, {"type": "playlist_created", "playlist_id": playlist_id, "name": name})

                elif msg_type == 'get_playlist_tracks':
                    playlist_id = msg.get('playlist_id')
                    tracks = fetch_all(
                        "SELECT t.id, t.title, t.artist, t.album, t.duration FROM playlist_items pi JOIN tracks t ON pi.track_id = t.id WHERE pi.playlist_id = ? ORDER BY pi.position",
                        (playlist_id,)
                    )
                    track_list = [{'id': t['id'], 'title': t['title'], 'artist': t['artist'],
                                   'album': t['album'], 'duration': t['duration']} for t in tracks] if tracks else []
                    self.send_message(sock, {"type": "playlist_tracks", "tracks": track_list})

                elif msg_type == 'add_to_playlist':
                    playlist_id = msg.get('playlist_id')
                    track_id = msg.get('track_id')
                    if playlist_id and track_id:
                        result = fetch_one("SELECT COALESCE(MAX(position), -1) + 1 as next_pos FROM playlist_items WHERE playlist_id=?", (playlist_id,))
                        next_pos = result['next_pos'] if result else 0
                        execute_query("INSERT INTO playlist_items (playlist_id, track_id, position) VALUES (?, ?, ?)", (playlist_id, track_id, next_pos))
                        self.send_message(sock, {"type": "track_added"})

                elif msg_type == 'get_history':
                    history_list = []
                    if user_id:
                        tracks = fetch_all(
                            "SELECT t.id, t.title, t.artist, t.album, t.duration FROM history h JOIN tracks t ON h.track_id = t.id WHERE h.user_id = ? ORDER BY h.listened_at DESC LIMIT 20",
                            (user_id,)
                        )
                        if tracks:
                            history_list = [{'id': t['id'], 'title': t['title'], 'artist': t['artist'],
                                             'album': t['album'], 'duration': t['duration']} for t in tracks]
                    self.send_message(sock, {"type": "history", "tracks": history_list})

        except Exception as e:
            print(f"Error: {e}")
        finally:
            self.stop_ffmpeg(session_id)
            if session_id:
                with self.lock:
                    if session_id in self.sessions:
                        if self.sessions[session_id].get('stop_event'):
                            self.sessions[session_id]['stop_event'].set()
                        del self.sessions[session_id]
            sock.close()
            print(f"Disconnected: {addr[0]}")

if __name__ == '__main__':
    server = MusicServer()
    try:
        server.start()
    except KeyboardInterrupt:
        print("\nServer stopped")
