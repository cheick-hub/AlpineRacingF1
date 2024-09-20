import socket
UDP_IP = "127.0.0.1"
UDP_PORT = 8080

def send_message(message, encoding='utf-8'):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
    sock.sendto(bytes(message, encoding=encoding), (UDP_IP, UDP_PORT))

if __name__ == '__main__':
    send_message("Cheick est un monstre 1")
    send_message("Cheick est un monstre 2")
    send_message("Cheick est un monstre 3")
    send_message("Cheick est un monstre 4")