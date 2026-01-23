from fastapi import FastAPI
import threading
import main

app = FastAPI()

@app.get("/")
def root():
    return {"status": "Bot running"}

@app.on_event("startup")
def start_bot():
    threading.Thread(target=main.start_bot, daemon=True).start()
