import socket
import time
def receive_data():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', 5000))  # Connect to the server

    try:
        while True:
            data = client_socket.recv(1024)  # Receive up to 1024 bytes
            if not data:  # Check for disconnection
                print("Server disconnected.")
                break

            decoded_data = data.decode("utf-8")
            print("Received:", decoded_data)
            print('-'*20)
            time.sleep(1)

    except ConnectionRefusedError:
        print("Connection refused. Make sure the server is running.")
    except KeyboardInterrupt:
        print("Client terminated by Keyboard Interruption...")
    finally:
        client_socket.close()

if __name__ == "__main__":
    receive_data()
