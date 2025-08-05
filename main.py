from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict
from datetime import datetime, timedelta
import pandas as pd
import threading
import time

app = FastAPI()

session_store: Dict[str, Dict[str, any]] = {}
lock = threading.Lock()
DATA_LIFETIME_SECONDS = 20 * 60

class AddValuesRequest(BaseModel):
    session_id: str
    values: List[float]

@app.post("/add")
def add_values(req: AddValuesRequest):
    with lock:
        now = datetime.utcnow()
        if req.session_id not in session_store:
            session_store[req.session_id] = {
                "values": pd.Series([], dtype="float64"),
                "last_updated": now,
            }
        session_store[req.session_id]["values"] = pd.concat([
            session_store[req.session_id]["values"],
            pd.Series(req.values)
        ])
        session_store[req.session_id]["last_updated"] = now
    return {"status": "ok", "count": len(session_store[req.session_id]["values"])}

@app.get("/median")
def get_median(session_id: str = Query(...)):
    with lock:
        if session_id not in session_store:
            raise HTTPException(status_code=404, detail="Session not found")
        values = session_store[session_id]["values"]
        if values.empty:
            raise HTTPException(status_code=400, detail="No data in session")
        median = float(values.median())
    return {"median": median}

@app.post("/clear")
def clear_session(session_id: str = Query(...)):
    with lock:
        if session_id in session_store:
            del session_store[session_id]
    return {"status": "cleared"}

def cleanup_expired_sessions():
    while True:
        time.sleep(60)
        now = datetime.utcnow()
        with lock:
            expired = [
                sid for sid, data in session_store.items()
                if now - data["last_updated"] > timedelta(seconds=DATA_LIFETIME_SECONDS)
            ]
            for sid in expired:
                del session_store[sid]

threading.Thread(target=cleanup_expired_sessions, daemon=True).start()
