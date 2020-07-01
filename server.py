import _influx as ifx
from socket import *
import select
import time
import threading

lock = threading.Lock()

def db_ctl_thread(client, body_info):
    lock.acquire()      # 获得锁
    try:
        ifx.db_add(client, body_info)   # 进行数据库操作
    finally:
        lock.release()  # 释放锁

def main():

	# 连接数据库
    client = ifx.db_init()
    # 开启服务器监听端口，响应客户端操作
    # 创建socket对象
    sock_server = socket(AF_INET, SOCK_STREAM)
    # 绑定IP和端口
    sock_server.bind(("localhost", 7788))
    # 将主动模式设置为被动模式，监听连接
    sock_server.listen(5)
    # 创建epoll检测对象
    epoll = select.epoll()
    # 注册主套接字，监控读状态
    epoll.register(sock_server.fileno(), select.EPOLLIN)
    # 创建字典，保存套接字对象
    sock_dicts = {}
    # 创建字典，保存客户端信息
    client_dicts = {}

    # start = time.time()

    while True:
        # print(f'{len(client_dicts)}连接上...')
        # print(f'{len(sock_dicts)}')
        # 程序阻塞在这，返回文件描述符有变化的对象
        poll_list = epoll.poll()
        for sock_fileno, events in poll_list:
            start = time.time()
            # print(f'sock_fileno: {sock_fileno}, events: {events}')
            # 判断是否是主套接字
            if sock_fileno == sock_server.fileno():
                # 创建新套接字
                new_sock, client_info = sock_server.accept()
                # print(f'客户端: {client_info}已连接')
                # print(f'{new_sock}: {client_info}')
                # 注册到epoll检测中
                epoll.register(new_sock.fileno(), select.EPOLLIN)
                # 添加到套接字字典当中
                sock_dicts[new_sock.fileno()] = new_sock
                client_dicts[new_sock.fileno()] = client_info
            else:
                # 接收信息
                raw_data = sock_dicts[sock_fileno].recv(1024)
                if raw_data:
                    # print(f"来自{client_dicts[sock_fileno]}的数据: {raw_data.decode('utf-8')}")
                    body_info = raw_data.decode('utf-8')
                    # 新建线程处理数据库
                    td = threading.Thread(target=db_ctl_thread, args=(client, body_info))
                    td.start()  # 执行线程
                    # td.join()   # 主线程阻塞，等待子线程结束
                    # ifx.db_add(client, body_info)

                else:
                    # 关闭连接
                    sock_dicts[sock_fileno].close()
                    # 注销epoll检测对象
                    epoll.unregister(sock_fileno)
                    # 数据为空，则客户端断开连接，删除相关数据
                    del sock_dicts[sock_fileno]
                    del client_dicts[sock_fileno]
            print(f'used: {time.time() - start}s.')

if __name__ == '__main__':
    main()
