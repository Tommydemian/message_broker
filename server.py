import os
import socket  # noqa: F401
import threading  # noqa: F401

from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv("HOST")  # localhost
PORT = 55555

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((HOST, PORT))
server.listen()

clients = []
nicknames = []


def broadcast(message):
    for client in clients:
        client.send(message)


def handle(client):
    while True:
        try:
            message = client.recv(1024)
            broadcast(message=message)
        except Exception:
            index = clients.index(client)
            clients.remove(index)
            client.close()
            nickname = nicknames[index]
            broadcast(f"{nickname} left the chat!".encode("utf-8"))
            nicknames.remove(nickname)
            break


def receive():
    while True:
        client, address = server.accept()
        print(f"Client connected with address: {str(address)}")

        client.send("NICK".encode("utf-8"))
        nickname = client.recv(1024).decode("utf-8")
        nicknames.append(nickname)
        clients.append(client)

        print(f"Nickname of client is {nickname}")
        broadcast(f"{nickname} just joined the chat!".encode("utf-8"))
        client.send("Connected to server!".encode("utf-8"))

        thread = threading.Thread(target=handle, args=(client,))
        thread.start()


receive()
