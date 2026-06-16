import socket
import struct
import json
import os
import threading
import time
import subprocess
from queue import Queue, Empty

class MusicClient:
    def __init__(self):
        self.sock = None
        self.connected = False
        self.session_id = None
        self.user_id = None
        self.current_track = None
        self.is_playing = False
        self.is_paused = False
        self.queue = []
        self.msg_queue = Queue()
        self.audio_queue = Queue()
        self.ffplay = None
        self.stream_ended = False

    def connect(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect(('localhost', 9000))
            self.connected = True
            threading.Thread(target=self.receiver, daemon=True).start()
            threading.Thread(target=self.audio_player, daemon=True).start()
            return True
        except Exception as e:
            print(f"Cannot connect: {e}")
            return False

    def send(self, data):
        try:
            msg = json.dumps(data).encode('utf-8')
            header = struct.pack('!I', len(msg))
            self.sock.sendall(header + msg)
        except:
            self.connected = False

    def stop_ffplay(self):
        if self.ffplay:
            try:
                if self.ffplay.stdin:
                    self.ffplay.stdin.close()
                self.ffplay.terminate()
                self.ffplay.wait(timeout=2)
            except:
                pass
            self.ffplay = None

    def clear_audio_queue(self):
        while not self.audio_queue.empty():
            try:
                self.audio_queue.get_nowait()
            except:
                pass

    def receiver(self):
        buffer = b''
        while self.connected:
            try:
                data = self.sock.recv(65536)
                if not data:
                    break
                buffer += data
                while len(buffer) >= 4:
                    length = struct.unpack('!I', buffer[:4])[0]
                    if len(buffer) >= 4 + length:
                        chunk = buffer[4:4+length]
                        buffer = buffer[4+length:]
                        try:
                            msg = json.loads(chunk.decode('utf-8'))
                            self.handle_server_message(msg)
                        except:
                            if self.is_playing and not self.is_paused:
                                self.audio_queue.put(chunk)
                    else:
                        break
            except:
                break
        self.connected = False

    def handle_server_message(self, msg):
        msg_type = msg.get('type', '')

        if msg_type == 'stop_audio':
            self.stop_ffplay()
            self.clear_audio_queue()
            self.stream_ended = False

        elif msg_type == 'stream_end':
            self.stream_ended = True
            if self.ffplay and self.ffplay.stdin:
                try:
                    self.ffplay.stdin.close()
                except:
                    pass

        elif msg_type == 'queue_empty':
            self.is_playing = False
            self.is_paused = False
            self.current_track = None
            self.queue = []
            self.stop_ffplay()

        elif msg_type == 'queue_updated':
            self.queue = msg.get('queue', [])

        elif msg_type == 'now_playing':
            self.current_track = msg.get('track', {})
            self.is_playing = True
            self.is_paused = False
            self.stream_ended = False

        elif msg_type == 'paused':
            self.is_paused = True
            self.stop_ffplay()
            self.clear_audio_queue()

        elif msg_type == 'resumed':
            self.is_paused = False

        self.msg_queue.put(msg)

    def audio_player(self):
        while self.connected:
            if not self.is_playing or self.is_paused:
                time.sleep(0.1)
                continue
            try:
                chunk = self.audio_queue.get(timeout=1)

                if self.ffplay is None or self.ffplay.poll() is not None:
                    if self.ffplay:
                        self.stop_ffplay()
                    try:
                        self.ffplay = subprocess.Popen(
                            ['ffplay', '-nodisp', '-autoexit', '-infbuf', '-f', 'mp3', '-i', 'pipe:0'],
                            stdin=subprocess.PIPE,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL
                        )
                    except FileNotFoundError:
                        print("\nffplay not found. Install FFmpeg.")
                        self.is_playing = False
                        break

                if self.ffplay and self.ffplay.poll() is None:
                    try:
                        self.ffplay.stdin.write(chunk)
                    except:
                        pass

                if self.stream_ended and self.ffplay and self.ffplay.poll() is not None:
                    self.is_playing = False
                    self.send({"type": "next"})
                    self.stream_ended = False

            except Empty:
                if self.stream_ended and self.ffplay and self.ffplay.poll() is not None:
                    self.is_playing = False
                    self.send({"type": "next"})
                    self.stream_ended = False
                continue

    def wait_for_type(self, msg_type, timeout=5):
        start = time.time()
        while time.time() - start < timeout:
            try:
                msg = self.msg_queue.get(timeout=1)
                if msg.get('type') == msg_type:
                    return msg
            except Empty:
                continue
        return None

    def login(self, username, password):
        self.send({"type": "login", "username": username, "password": password})
        response = self.wait_for_type('login_result', 5)
        if response and response.get('status') == 'ok':
            self.session_id = response['session_id']
            self.user_id = response['user_id']
            return True
        return False

    def register(self, username, password):
        self.send({"type": "register", "username": username, "password": password})
        return self.wait_for_type('register_result', 5)

    def get_library(self):
        self.send({"type": "get_library"})
        return self.wait_for_type('library', 5)

    def search(self, query):
        self.send({"type": "search", "query": query})
        return self.wait_for_type('search_results', 5)

    def play(self, track_id, track_info=None):
        self.stop_ffplay()
        self.clear_audio_queue()
        self.is_playing = True
        self.is_paused = False
        self.stream_ended = False
        if track_info:
            self.current_track = track_info
        self.send({"type": "play", "track_id": track_id})

    def toggle_pause(self):
        if self.is_paused:
            self.is_paused = False
            self.send({"type": "resume"})
        else:
            self.is_paused = True
            self.stop_ffplay()
            self.clear_audio_queue()
            self.send({"type": "pause"})

    def skip(self):
        self.stop_ffplay()
        self.clear_audio_queue()
        self.is_paused = False
        self.stream_ended = False
        self.send({"type": "next"})

    def add_to_queue(self, track_id):
        self.send({"type": "add_to_queue", "track_id": track_id})

    def remove_from_queue(self, track_id):
        self.send({"type": "remove_from_queue", "track_id": track_id})

    def get_queue(self):
        self.send({"type": "get_queue"})
        return self.wait_for_type('queue', 5)

    def get_playlists(self):
        self.send({"type": "get_playlists"})
        return self.wait_for_type('playlists', 5)

    def create_playlist(self, name):
        self.send({"type": "create_playlist", "name": name})
        return self.wait_for_type('playlist_created', 5)

    def get_playlist_tracks(self, playlist_id):
        self.send({"type": "get_playlist_tracks", "playlist_id": playlist_id})
        return self.wait_for_type('playlist_tracks', 5)

    def add_to_playlist(self, playlist_id, track_id):
        self.send({"type": "add_to_playlist", "playlist_id": playlist_id, "track_id": track_id})

    def play_playlist(self, playlist_id):
        result = self.get_playlist_tracks(playlist_id)
        if not result:
            print("Playlist is empty or not found.")
            return
        tracks = result.get('tracks', [])
        if not tracks:
            print("Playlist is empty.")
            return
        for t in tracks:
            if t['id'] not in self.queue:
                self.queue.append(t['id'])
        self.send({"type": "queue_updated", "queue": self.queue})
        first = tracks[0]
        self.play(first['id'], track_info={
            'id': first['id'],
            'title': first['title'],
            'artist': first['artist'],
            'album': first.get('album', 'Unknown'),
            'duration': first.get('duration', 0)
        })
        print(f"Playing playlist: {len(tracks)} tracks added to queue.")

    def get_history(self):
        self.send({"type": "get_history"})
        return self.wait_for_type('history', 5)

    def run(self):
        if not self.connect():
            return
        print("\nMiguel - Music Player")
        while True:
            print("MAIN MENU")
            print("1. Login  2. Register  3. Exit")
            choice = input("\nChoose: ").strip()
            if choice == '1':
                u = input("Username: ").strip()
                p = input("Password: ").strip()
                if self.login(u, p):
                    print("\nLogin successful!")
                    self.main_menu()
                    break
                else:
                    print("\nLogin failed.")
            elif choice == '2':
                u = input("Username: ").strip()
                p = input("Password: ").strip()
                r = self.register(u, p)
                if r:
                    print(f"\n{r.get('message', 'Done')}")
            elif choice == '3':
                break
        self.connected = False
        self.stop_ffplay()
        if self.sock:
            self.sock.close()

    def main_menu(self):
        while self.connected:
            if self.current_track:
                title = self.current_track.get('title', 'Unknown')
                status = "PAUSED" if self.is_paused else "PLAYING"
                print(f"Now Playing: {title}")
                print(f"Status: {status}")
            else:
                print("No track playing")
            if self.queue:
                print(f"Queue: {len(self.queue)} song(s)")
            print("1. Search  2. Library  3. Playlists")
            print("4. History 5. Queue    7. Pause/Play")
            print("8. Next    0. Logout")
            cmd = input("\nCommand: ").strip()
            if cmd == '1':
                q = input("Search: ").strip()
                if q:
                    r = self.search(q)
                    if r:
                        self.display_tracks(r.get('tracks', []))
            elif cmd == '2':
                r = self.get_library()
                if r:
                    self.display_tracks(r.get('tracks', []))
            elif cmd == '3':
                self.playlist_menu()
            elif cmd == '4':
                r = self.get_history()
                if r:
                    for t in r.get('tracks', []):
                        print(f"{t['title']} - {t['artist']}")
            elif cmd == '5':
                self.queue_menu()
            elif cmd == '7':
                self.toggle_pause()
                print("Paused" if self.is_paused else "Playing")
            elif cmd == '8':
                self.skip()
            elif cmd == '0':
                self.stop_ffplay()
                break

    def display_tracks(self, tracks):
        if not tracks:
            print("No tracks.")
            return
        for i, t in enumerate(tracks):
            print(f"{i+1}. {t['title']} - {t['artist']} (ID: {t['id']})")
        c = input("\nPlay number (0 to cancel): ").strip()
        if c.isdigit():
            idx = int(c) - 1
            if 0 <= idx < len(tracks):
                t = tracks[idx]
                self.play(t['id'], track_info={'id': t['id'], 'title': t['title'], 'artist': t['artist']})

    def playlist_menu(self):
        while True:
            print("\n1. View  2. Create  3. View Tracks  4. Add  5. Play Playlist  0. Back")
            c = input("Select: ").strip()
            if c == '1':
                r = self.get_playlists()
                if r:
                    for p in r.get('playlists', []):
                        print(f"ID: {p['id']} - {p['name']}")
            elif c == '2':
                self.create_playlist(input("Name: ").strip())
            elif c == '3':
                pid = input("Playlist ID: ").strip()
                if pid.isdigit():
                    r = self.get_playlist_tracks(int(pid))
                    if r:
                        for t in r.get('tracks', []):
                            print(f"{t['title']} - {t['artist']}")
            elif c == '4':
                pid = input("Playlist ID: ").strip()
                tid = input("Track ID: ").strip()
                if pid.isdigit() and tid.isdigit():
                    self.add_to_playlist(int(pid), int(tid))
            elif c == '5':
                pid = input("Playlist ID: ").strip()
                if pid.isdigit():
                    self.play_playlist(int(pid))
            elif c == '0':
                break

    def queue_menu(self):
        r = self.get_queue()
        if r:
            self.queue = r.get('queue', [])
        print(f"Queue: {self.queue}")
        print("1. Add  2. Remove  0. Back")
        c = input("Select: ").strip()
        if c == '1':
            tid = input("Track ID: ").strip()
            if tid.isdigit():
                self.add_to_queue(int(tid))
        elif c == '2':
            tid = input("Track ID: ").strip()
            if tid.isdigit():
                self.remove_from_queue(int(tid))

if __name__ == '__main__':
    client = MusicClient()
    client.run()
