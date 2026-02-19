from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import zipfile
import uuid
import os
import json
from kafka import KafkaProducer

app = FastAPI()

# Allow UI (port 8080) to call backend (8000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# In-memory progress store
# -----------------------------
progress_store = {}

UPLOAD_DIR = "/tmp/uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# -----------------------------
# Kafka Producer (Redpanda)
# -----------------------------
producer = KafkaProducer(
    bootstrap_servers="redpanda:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# -----------------------------
# Upload & Unzip API
# -----------------------------
@app.post("/upload")
async def upload_zip(file: UploadFile = File(...)):
    if not file.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only ZIP files allowed")

    upload_id = str(uuid.uuid4())
    zip_path = os.path.join(UPLOAD_DIR, f"{upload_id}.zip")

    # Save uploaded ZIP
    with open(zip_path, "wb") as f:
        f.write(await file.read())

    extract_path = os.path.join(UPLOAD_DIR, upload_id)
    os.makedirs(extract_path, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        file_list = zip_ref.namelist()
        total_files = len(file_list)

        processed_files = 0

        # Init progress
        progress_store[upload_id] = {
            "total_files": total_files,
            "processed_files": 0,
            "status": "unzipping"
        }

        for name in file_list:
            zip_ref.extract(name, extract_path)
            processed_files += 1

            # Update progress
            progress_store[upload_id] = {
                "total_files": total_files,
                "processed_files": processed_files,
                "status": "unzipping"
            }

            # Publish event
            producer.send(
                "upload-progress",
                {
                    "upload_id": upload_id,
                    "total_files": total_files,
                    "processed_files": processed_files,
                    "status": "unzipping"
                }
            )

    # Mark completed
    progress_store[upload_id]["status"] = "completed"
    producer.send(
        "upload-progress",
        {
            "upload_id": upload_id,
            "total_files": total_files,
            "processed_files": total_files,
            "status": "completed"
        }
    )

    return {"upload_id": upload_id}

# -----------------------------
# Progress API (Phase-4 Minimal)
# -----------------------------
@app.get("/progress/{upload_id}")
def get_progress(upload_id: str):
    if upload_id not in progress_store:
        raise HTTPException(status_code=404, detail="Upload ID not found")

    data = progress_store[upload_id]

    percent = int(
        (data["processed_files"] / data["total_files"]) * 100
        if data["total_files"] > 0 else 0
    )

    return {
        "upload_id": upload_id,
        "status": data["status"],
        "processed_files": data["processed_files"],
        "total_files": data["total_files"],
        "percent": percent
    }
