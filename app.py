from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import pandas as pd
import os
import threading
import time
from datetime import datetime, timedelta
from typing import List

app = FastAPI()
DATA_DIR = "data"
LOCK = threading.Lock()
EXPIRATION_MINUTES = 20

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

class Batch(BaseModel):
    values: List[float]

def get_file_path(seed_id: str) -> str:
    return os.path.join(DATA_DIR, f"{seed_id}.csv")

def update_access_time(path: str):
    os.utime(path, None)

def append_values(seed_id: str, new_values: List[float]):
    file_path = get_file_path(seed_id)
    with LOCK:
        df_new = pd.DataFrame({'value': new_values})
        if not os.path.exists(file_path):
            df_new.to_csv(file_path, index=False)
        else:
            df_new.to_csv(file_path, mode='a', header=False, index=False)
        update_access_time(file_path)

def read_data(seed_id: str) -> pd.DataFrame:
    file_path = get_file_path(seed_id)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Seed not found")
    with LOCK:
        update_access_time(file_path)
        return pd.read_csv(file_path)

@app.post("/append")
async def append(batch: Batch, seed_id: str = Query(..., description="Unique session identifier")):
    if not batch.values:
        raise HTTPException(status_code=400, detail="Empty values list")
    append_values(seed_id, batch.values)
    return {"status": "ok", "appended": len(batch.values)}

@app.get("/median")
async def median(seed_id: str = Query(...)):
    df = read_data(seed_id)
    return {"median": df['value'].median()}

@app.get("/export")
async def export(seed_id: str = Query(...)):
    file_path = get_file_path(seed_id)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Seed not found")
    with open(file_path, "r") as f:
        content = f.read()
    return {"data": content}

@app.get("/health")
async def health():
    return {"status": "alive"}

# ======= Фоновая задача очистки старых файлов =======
def cleanup_loop():
    while True:
        now = time.time()
        expired = []
        with LOCK:
            for fname in os.listdir(DATA_DIR):
                path = os.path.join(DATA_DIR, fname)
                if os.path.isfile(path):
                    last_access = os.path.getatime(path)
                    if now - last_access > EXPIRATION_MINUTES * 60:
                        expired.append(path)
            for path in expired:
                try:
                    os.remove(path)
                except Exception:
                    pass
        time.sleep(60)  # Проверять раз в минуту

threading.Thread(target=cleanup_loop, daemon=True).start()
