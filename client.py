import argparse
import errno
import os
import socket
import time
import datetime
import json

SERVER_ADDRESS = 'localhost', 7788
REQUEST = b"""\
GET /hello HTTP/1.1
Host: localhost:8888
"""

def main(max_clients, max_conns):
    socks = []
    for client_num in range(max_clients):
        pid = os.fork()

        if pid == 0:
            for connection_num in range(max_conns):
                now = datetime.datetime.now()
                nowtime = now.strftime('%Y-%m-%d %H-%M-%S')
                body = [
                    {
                        "measurement": "students",
                        "fields": {
                            'host': 'localhost',
                            'PID':  f'{client_num} - {connection_num}',
                            'name': 'bbb',
                            'tags': 'student'
                        }
                    }
                ]
                # print(type(body))
                json_body = json.dumps(body)
                # print(type(json_body))
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(SERVER_ADDRESS)
                sock.sendall(json_body.encode('utf-8'))
                socks.append(sock)
                # print(connection_num)
                # time.sleep(60)
            os._exit(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Test client for LSBAWS',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--max-conns',
        type=int,
        default=1024,
        help='Maximum number of connections per client.'
    )
    parser.add_argument(
        '--max-clients',
        type=int,
        default=1,
        help='Maximum number of clients.'
    )
    args = parser.parse_args()
    print(f'args.max_clients={args.max_clients}, args.max_conns={args.max_conns}')

    start = time.time()
    main(args.max_clients, args.max_conns)
    print(f'used: {time.time() - start} s.')