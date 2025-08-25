import os
import socket  # noqa: F401
import threading  # noqa: F401

nickname = input("Chose a Nickname")

from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv("HOST")  # localhost
PORT = 55555

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect((HOST, PORT))


def receive():
    while True:
        try:
            message = client.recv(1024).decode("utf-8")
            if message == "NICK":
                client.send(nickname.encode("utf-8"))
            else:
                print(message)
        except Exception as e:
            print("An error occured!", e)
            client.close()
            break


def write():
    while True:
        message = f"{nickname}: {input('')}"
        client.send(message.encode("utf-8"))


receive_thread = threading.Thread(target=receive)
receive_thread.start()
write_thread = threading.Thread(target=write)
write_thread.start()
