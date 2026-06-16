import bcrypt
import uuid
from datetime import datetime
from database import fetch_one, execute_query

def register_user(username, password):
    existing = fetch_one("SELECT id FROM users WHERE username=?", (username,))
    if existing:
        return {"status": "error", "message": "Username already exists"}

    try:
        hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        execute_query("INSERT INTO users (username, password_hash) VALUES (?, ?)", (username, hashed))
        return {"status": "ok", "message": "Registration successful"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def authenticate_user(username, password):
    user = fetch_one("SELECT id, password_hash FROM users WHERE username=?", (username,))
    if not user:
        return {"status": "error", "message": "Invalid credentials"}

    if not bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
        return {"status": "error", "message": "Invalid credentials"}

    session_id = str(uuid.uuid4())
    now = datetime.now().isoformat()
    execute_query("INSERT INTO sessions (session_id, user_id, last_seen) VALUES (?, ?, ?)",
                  (session_id, user['id'], now))

    return {
        "status": "ok",
        "session_id": session_id,
        "user_id": user['id']
    }
