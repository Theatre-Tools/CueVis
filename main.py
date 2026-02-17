from email import message_from_file
from enum import Enum
from operator import call
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pyo import OSCString, Peer, OSCMessage, OSCModes, OSCFraming, OSCArg, call_handler, Dispatcher
from pydantic import BaseModel
import asyncio
from logging import Logger, basicConfig, getLogger
import json
from enum import Enum



format = f"%(asctime)s - %(name)s: %(levelname)s - %(message)s"
basicConfig(format=format)
logger = Logger("OSCLogger")
current_cue_queue = None
event_loop = None

# Create a model that contains the two types, active and pending
class Type(Enum):
    ACTIVE = "active"
    PENDING = "pending"
        
    

class Cue_Text(BaseModel):
    args: tuple[OSCString]
    address: str
    @property
    def type(self) -> Type:
        if self.address.split('/')[3] == "active":
            return Type.ACTIVE
        elif self.address.split('/')[3] == "pending":
            return Type.PENDING
        else:
            raise ValueError("Invalid type in address path.")
        
    @property
    def cue(self) -> int :
        if self.type == Type.PENDING and self.args[0].value == "":
            return 'OUT'
        try:
            if not '/' in self.args[0].value:
                print(f"{self.args[0].value}")
                return 0
            return int(self.args[0].value.split(" ")[0].split("/")[1])
        except (IndexError, ValueError) as exc:
            raise ValueError(f"No cue number provided in the argument string. {self.address} {self.args}") from exc

    
    @property
    def list(self) -> int:
        print(self)
        if not '/' in self.args[0].value:
            return 0
        else:
            return int(self.args[0].value.split(" ")[0].split("/")[0])
    
    @property
    def percentage(self) -> str | None:
        if self.type != Type.PENDING:
            try:    
                return self.args[0].value.split(" ")[2]
            except IndexError:
                raise ValueError(f"No percentage provided in the argument string. {self}")
    
    
    @property
    def duration(self) -> float:
        try:
            return float(self.args[0].value.split(" ")[1])
        except IndexError:
            raise ValueError(f"No duration provided in the argument string. {self}")
    
    @property
    def complete(self) -> bool | None:
        if self.type != Type.PENDING:
            try:
                if self.percentage == "100%":
                    return True
                else:
                    return False
            except ValueError:
                raise ValueError(f"Percentage value is not a valid float. {self}")
        

class PingValidator(BaseModel):
    args: tuple[OSCString]
    address: str
    @property
    def pong(self) -> str:
        try: 
            return self.args[0].value
        except IndexError:
            raise ValueError(f"No response provided in the argument string. {self}")



def text_cue_handler(message: Cue_Text):
    print(message)
    if message.type == Type.ACTIVE:
        logger.info(f"Received active cue: {message.cue}, list: {message.list}, percentage: {message.percentage}, Duration: {message.duration}s, complete: {message.complete}")
        try:
            if message.complete:
                if active_loop is not None and active_cue_queue is not None:
                    active_loop.call_soon_threadsafe(active_cue_queue.put_nowait, message.cue)
        except Exception as exc:
            logger.error("Cue text handler error: %s", exc)
    elif message.type == Type.PENDING:
        logger.info(f"Received pending cue: {message.cue}, list: {message.list}, percentage: {message.percentage}, Duration: {message.duration}s, complete: {message.complete}")
        try:
            if message.complete:
                if pending_loop is not Noe and pending_cue_queue is not None:
                    pending_loop.call_soon_threadsafe(pending_cue_queue.put_nowait, message.cue)
        except Exception as exc:
            logger.error("Cue text handler error: %s", exc)
    
    
    #try:
    #    print(message.cue)
    #    if message.complete:
    #        if event_loop is not None and current_cue_queue is not None:
    #            event_loop.call_soon_threadsafe(current_cue_queue.put_nowait, message.cue)
    #except Exception as exc:
    #    logger.error("Cue text handler error: %s", exc)





## Define FastApi app and initialize the templates and statics
app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


global peer
global caller
global dispatcher

dispatcher = None

try:
    peer = Peer(address='localhost', port=3032, mode=OSCModes.TCP, framing=OSCFraming.OSC11)
    caller = call_handler.CallHandler(peer=peer)
    dispatcher = peer.Dispatcher
    dispatcher.add_handler('/eos/out/{active,pending}/cue/text', text_cue_handler, validator=Cue_Text)
except Exception as exc:
    logger.error("Failed to create OSC Peer or add handler: %s", exc)

#When Fastapi is online, start listening to OSC packets
@app.on_event("startup")
def startup_event():
    global active_cue_queue, active_loop, pending_cue_queue, pending_loop
    active_loop = asyncio.get_running_loop()
    active_cue_queue = asyncio.Queue()
    pending_loop = asyncio.get_running_loop()
    pending_cue_queue = asyncio.Queue()
    peer.start_listening()


@app.on_event("shutdown")
def shutdown_event():
    peer.stop_listening()
    
def default_handler(message: OSCMessage):
    logger.info(f"Received message at {message.address} with arguments: {message.args}")
    with open("osc_log.txt", "a") as log_file:
        log_file.write(f"{message.address} {message.args}\n")

try:
    dispatcher.add_handler('/*', default_handler)
except Exception as exc:
    logger.error("Failed to add default handler: %s", exc)

@app.get("/api/cue")
async def read_root():
    if current_cue_queue is not None and not current_cue_queue.empty():
        cue = current_cue_queue.get_nowait()
        return {"cue": cue}
    return {"cue": "No active cue"}

@app.get('/api/status')
def status(ping_message: str = "Hello from CueVis!"):
    message = OSCMessage(address='/eos/ping', args=(OSCString(value=ping_message),))
    response = caller.call(message=message, validator=PingValidator, return_address="/eos/out/ping")
    if response is None:
        return {"status": {"code": 500, "response": "No response from EOS"}}
    print(response.pong)
    if response.pong == ping_message:
        return {"status": {
            "code": 200,
            "response": response.pong
        }}
    else:
        raise ValueError("Unexpected response from EOS: %s" % response.pong)

@app.get("/api/active/stream")
async def active_stream():
    async def active_cue_generator():
        if active_cue_queue is None:
            return
        try:
            while True:
                cue = await active_cue_queue.get()
                ## Create a json string with the cue number and replace the entire event-stream with the new cue number, this will trigger the event listener in the frontend to update the displayed cue number
                yield json.dumps({"active": cue})
        except asyncio.CancelledError:
            logger.info("Event generator cancelled.")

    # Return the streaming JSON response with the appropriate media type for Server-Sent Events (SSE)
    return StreamingResponse(active_cue_generator(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}, status_code=200)

@app.get('/api/pending/stream')
async def pending_stream():
    async def event_generator():
        if current_cue_queue is None:
            return
        try:
            while True:
                cue = await current_cue_queue.get()
                yield json.dumps({"pending": cue})
        except asyncio.CancelledError:
            logger.info("Event generator cancelled.")

    return StreamingResponse(event_generator(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}, status_code=200)


@app.get('/')
def index():
    return templates.TemplateResponse("index.html", {"request": {}})
