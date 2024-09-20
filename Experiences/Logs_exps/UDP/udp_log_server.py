import socket

# 1. Create a UDP socket
udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

server_address = ("0.0.0.0", 8080)
udp_socket.bind(server_address)

print(f"UDP server is listening on {server_address} ...")

while True:
    # 3. Receive message from the client (with a buffer size of 1024 bytes)
    message, client_address = udp_socket.recvfrom(1024)
    
    print(f"Received message: {message.decode('utf-8')} from {client_address}")
