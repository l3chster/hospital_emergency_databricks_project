import asyncio
import csv
import io
import json
import os

from fastapi import FastAPI, HTTPException   # FastAPI -> python framework for creating APIs
from fastapi.responses import StreamingResponse
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

SOURCE_DIR = os.environ["STREAMING_DATA_VOLUME"]
STATE_DIR = os.environ["STATE_VOLUME"]          
STATE_PATH = f"{STATE_DIR}/stream_state.json"

app = FastAPI()
w = WorkspaceClient()           # enables to operate on resources in Databricks
stream_lock = asyncio.Lock()    # only one active stream at once


def load_state() -> dict:
    '''
    {"done": list of files (strings) that have been fully read, "current": file that is currently being read, 
    "row": number of row in current file to read}    
    '''

    try:
        raw: dict = w.files.download(STATE_PATH).contents.read()
        return json.loads(raw)     
    except NotFound:
        return {"done": [], "current": None, "row": 0}



def save_state(state: dict) -> None:
    w.files.upload(
        STATE_PATH,
        io.BytesIO(json.dumps(state).encode("utf-8")),
        overwrite=True,
    )


def list_csv_files() -> list[str]:
    entries = w.files.list_directory_contents(SOURCE_DIR)
    return sorted(
        e.name for e in entries
        if not e.is_directory and e.name.lower().endswith((".csv", ".txt"))
    )


def read_csv(name: str) -> tuple[list[str], list[list[str]]]:
    '''
    Seperate function that reads our data in byte format, then trasforms it 
    into string
    Returns: tuple -> header and file content
    '''

    raw = w.files.download(f"{SOURCE_DIR}/{name}").contents.read()
    reader = csv.reader(io.StringIO(raw.decode("utf-8")))
    header = next(reader)
    header = [h.strip().lower().replace(" ", "_") for h in header]
    return header, list(reader)  


# data generator
async def generate_csv_stream():
    """
    asyncio.to_thread() is a bridge between asynchronous world and multithreading
    it lets us run synchronous functions in a separate thread and await for it
    """
    state = None
    
    try:
        state = await asyncio.to_thread(load_state)     # obtaining current state
        files = await asyncio.to_thread(list_csv_files)     # got all our files

        # files that werent processed
        pending = [f for f in files if f not in state["done"]]
        if state["current"] in pending:         # case when current file were interupted in the middle
            pending.remove(state["current"])
            pending.insert(0, state["current"])
        
        for name in pending:
            OVERLAP = 5             # for data security we go back each time 5 rows -> there will be duplicates but they will be deleted in silver layer 

            start_row = max(0, state["row"] - OVERLAP) if name == state["current"] else 0         # because there might be queues in buffer
            header, rows = await asyncio.to_thread(read_csv, name)

            for i in range(start_row, len(rows)):
                await asyncio.sleep(1)

                data = dict(zip(header, rows[i]))
                payload = json.dumps({"file": name, "row": i + 1, **data})    # **data is new format for adding values in format key:value
                yield f"{payload}\n\n"
                
                # updating after adding row
                state["current"] = name
                state["row"] = i + 1
                
                if (i + 1) % 50 == 0:
                    await asyncio.to_thread(save_state, state)              # saving state each 50 rows

            # file finished
            state["done"].append(name)
            state["current"] = None
            state["row"] = 0
            await asyncio.to_thread(save_state, state)
    finally:
        try:
            if state is not None:
                save_state(state)       # synchronous
        finally:
            stream_lock.release()       

@app.get("/api/stream")
async def stream_data():
    if stream_lock.locked():
        raise HTTPException(409, "Stream is currently working")
    await stream_lock.acquire()
    return StreamingResponse(
        generate_csv_stream(),
        media_type="text/event-stream"        
    )


@app.post("/api/reset")
async def reset_state():
    """Optional: start from the beggining."""

    await asyncio.to_thread(save_state, {"done": [], "current": None, "row": 0})
    return {"status": "reset"}