import json
import socket
from enum import Enum

HOST = "192.168.0.221"
PORT = 9776


class HttpMethod(Enum):
    GET = "GET"
    POST = "POST"


server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((HOST, PORT))

server.listen(5)

while True:
    communication_socket, address = server.accept()
    print(f"conneceted to: {address}")
    # Client send message in binary
    message = communication_socket.recv(1024).decode("utf-8")
    # Parse req
    lines = message.split("\r\n")

    first_line = lines[0]
    req_body = lines[-1]

    client_accepts_json = "Accept: */*" in lines or "Accept: application/json" in lines

    parts = first_line.split(" ")

    method = parts[0]
    path = parts[1]
    version = parts[2]

    res: dict[str, str] = {}

    if path == "/":
        if method == HttpMethod.GET.value:
            res = {"status": "200 OK", "body": "Welcome"}
        if method == HttpMethod.POST.value:
            res = {"status": "200 OK", "body": f"Recieved: {req_body}"}
    elif path == "/hello":
        if method == HttpMethod.GET.value:
            res = {"status": "200 OK", "body": "Hello Thun"}
        if method == HttpMethod.POST.value:
            res = {"status": "200 OK", "body": f"Recieved: {req_body}"}
    else:
        res = {"status": "404 Not Found", "body": "Not found"}
    # Content Length
    if client_accepts_json:
        content_length = len(json.dumps(res).encode("utf-8"))
    else:
        content_length = len((res["body"]).encode("utf-8"))
    print(message)
    print(lines)
    content_type_header = "Content-type: application/json"
    communication_socket.send(
        f"HTTP/1.1 {res['status']}\r\nContent-Length: {content_length}\r\n{content_type_header if client_accepts_json else ''}\r\n\r\n{json.dumps(res) if client_accepts_json else res['body']}".encode()
    )
    communication_socket.close()
    print(f"Connection with {address} ended!")
