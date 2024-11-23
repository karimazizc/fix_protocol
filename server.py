import socket
import threading
import simplefix
from datetime import datetime

class FIXServer:
    def __init__(self, host='localhost', port=5001):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.host, self.port))
        self.clients = []
        self.parser = simplefix.FixParser()
        
    def start(self):
        self.socket.listen(5)
        print(f"Server listening on {self.host}:{self.port}")
        
        while True:
            client, address = self.socket.accept()
            print(f"Client connected from {address}")
            self.clients.append(client)
            client_thread = threading.Thread(target=self.handle_client, args=(client,))
            client_thread.daemon = True
            client_thread.start()
            
    def handle_client(self, client):
        while True:
            try:
                data = client.recv(4096)
                if not data:
                    break
                
                self.parser.append_buffer(data)
                message = self.parser.get_message()
                
                if not message:
                    continue
                    
                msg_type = message.get(35)
                
                if msg_type == b"A":  # Logon
                    self.handle_logon(client, message)
                elif msg_type == b"D":  # New Order
                    self.handle_new_order(client, message)
                elif msg_type == b"F":  # Cancel Request
                    self.handle_cancel(client, message)
                    
            except Exception as e:
                print(f"Error handling client: {e}")
                break
                
        self.clients.remove(client)
        client.close()
        
    def handle_logon(self, client, message):
        response = simplefix.FixMessage()
        response.append_pair(8, "FIX.4.2")
        response.append_pair(35, "A")
        response.append_pair(49, "SERVER")
        response.append_pair(56, message.get(49).decode())
        response.append_pair(34, 1)
        response.append_pair(52, datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3])
        
        client.send(response.encode())
        
    def handle_new_order(self, client, message):
        # Create execution report
        exec_report = simplefix.FixMessage()
        exec_report.append_pair(8, "FIX.4.2")
        exec_report.append_pair(35, "8")  # ExecutionReport
        exec_report.append_pair(49, "SERVER")
        exec_report.append_pair(56, message.get(49).decode())
        exec_report.append_pair(34, 2)
        exec_report.append_pair(52, datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3])
        exec_report.append_pair(37, f"EXEC{datetime.now().timestamp()}")
        exec_report.append_pair(11, message.get(11))  # ClOrdID
        exec_report.append_pair(150, "2")  # ExecType = Filled
        exec_report.append_pair(39, "2")   # OrdStatus = Filled
        exec_report.append_pair(55, message.get(55))  # Symbol
        exec_report.append_pair(54, message.get(54))  # Side
        exec_report.append_pair(32, message.get(38))  # LastQty = OrderQty
        
        # If limit order, use limit price, else generate random price
        if message.get(40) == b"2":
            price = message.get(44)
        else:
            price = "100.00"
            
        exec_report.append_pair(31, price)  # LastPx
        
        client.send(exec_report.encode())
        
    def handle_cancel(self, client, message):
        # Send cancel ack
        cancel_ack = simplefix.FixMessage()
        cancel_ack.append_pair(8, "FIX.4.2")
        cancel_ack.append_pair(35, "8")  # ExecutionReport
        cancel_ack.append_pair(49, "SERVER")
        cancel_ack.append_pair(56, message.get(49).decode())
        cancel_ack.append_pair(34, 3)
        cancel_ack.append_pair(52, datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3])
        cancel_ack.append_pair(37, f"CXLACK{datetime.now().timestamp()}")
        cancel_ack.append_pair(11, message.get(11))  # ClOrdID
        cancel_ack.append_pair(41, message.get(41))  # OrigClOrdID
        cancel_ack.append_pair(150, "4")  # ExecType = Canceled
        cancel_ack.append_pair(39, "4")   # OrdStatus = Canceled
        cancel_ack.append_pair(55, message.get(55))  # Symbol
        cancel_ack.append_pair(54, message.get(54))  # Side
        
        client.send(cancel_ack.encode())

if __name__ == "__main__":
    server = FIXServer()
    server.start()