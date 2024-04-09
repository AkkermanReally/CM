import socket


# HOST = (socket.gethostname(), 10000)
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('192.168.43.200', 8080))
server.listen(4)

print('Work')
client, address = server.accept()
data = client.recv(1024).decode('utf-8')
print('data:', data)
print('End.')