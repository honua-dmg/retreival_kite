from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()

class ConnectionManager:
    def __init__(self):
        self.active_connections = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)


class ConsumerConnectionManager(ConnectionManager):
    def __init__(self):
        """
        Initialised the active_connections as a dictionary to store the websocket connection and the associated stock_name. 
        This allows us to keep track of which stock is connected to which websocket connection.
        
        active_connections = {stock: [websocket1, websocket2, ...], ...}
        """
        self.active_connections = {} 

    async def connect(self, websocket: WebSocket, stock_name: str):
        await websocket.accept()
        if stock_name not in self.active_connections:
            self.active_connections[stock_name] = []
        self.active_connections[stock_name].append(websocket)
    
    async def disconnect(self, websocket: WebSocket):
        for stock_name, connections in self.active_connections.items():
            if websocket in connections:
                connections.remove(websocket)
                if not connections:  # If no more connections for this stock, remove the stock entry
                    del self.active_connections[stock_name]
                break
    async def broadcast(self, message: str,stock_name: str):
        for connection in self.active_connections.get(stock_name, []):
            await connection.send_text(message)

manager = ConsumerConnectionManager()
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket, stock_name="default")
    try:
        while True:
            data = await websocket.receive_text()
            print(f"Received: {data}")
            print(f"Active connections: {len(manager.active_connections)}")
            await manager.broadcast(f"Echo: {data[::-1]}")

    except WebSocketDisconnect:
        print("Client disconnected cleanly")
        manager.disconnect(websocket)
