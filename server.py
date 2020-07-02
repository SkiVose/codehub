import _influx as ifx
from socket import *
import select
import time
import threading
from concurrent.futures import ThreadPoolExecutor

# 获取全局锁
lock = threading.Lock()

# tep数组为了记录数据和耗时
tep = [0 for i in range(2)]

def db_ctl_thread(client, body_info):
    lock.acquire()      # 获得锁
    try:
        ifx.db_add(client, body_info)   # 进行数据库操作
        tep[0] += 1
        # print(f'count = {tep[0]}')
        if tep[0] == 1:
            tep[1] = time.time()
        elif tep[0]%100 == 0:
            # pass
            print(f'count:{tep[0]} | used: {time.time() - tep[1]}s.')
            tep[1] = time.time()
    finally:
        lock.release()  # 释放锁

def main():

    # 连接数据库
    client = ifx.db_init()

    # 设置线程池，用于数据库操作
    # 设置最大线程数为4
    p = ThreadPoolExecutor(max_workers=4) 

    # 开启服务器监听端口，响应客户端操作
    # 创建socket对象
    sock_server = socket(AF_INET, SOCK_STREAM)

    # 绑定IP和端口
    sock_server.bind(("172.18.186.173", 7788))
    # 将主动模式设置为被动模式，监听连接
    sock_server.listen(1024)
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
        try:
            for sock_fileno, events in poll_list:
                # start = time.time()
                # print(f'sock_fileno: {sock_fileno}, events: {events}')
                # 判断是否是主套接字
                if sock_fileno == sock_server.fileno():
                    # 创建新套接字
                    new_sock, client_info = sock_server.accept()
                    # print(f'客户端: {client_info}已连接')
                    # print(f'fileno: {sock_fileno}, sock: {new_sock}, client_info: {client_info}')
                    # 注册到epoll检测中
                    epoll.register(new_sock.fileno(), select.EPOLLIN)
                    # 添加到套接字字典当中
                    sock_dicts[new_sock.fileno()] = new_sock
                    client_dicts[new_sock.fileno()] = client_info
                    
                   # tep[0] += 1
                   # if tep[0] == 1:
                   #     tep[1] = time.time()
                   # elif tep[0]%100 == 0:
                   #     print(f'count: {tep[0]}, used: {time.time() - tep[1]}s.')
                else:
                    # 接收信息
                    # 根据数据包格式，假定接收256B，故设置传输接收一次接收2048bit
                    raw_data = sock_dicts[sock_fileno].recv(2048)
                    if raw_data:
                        # print(f"来自{client_dicts[sock_fileno]}的数据: {raw_data.decode('utf-8')}")
                        body_info = raw_data.decode('utf-8')
                        # 新建线程处理数据库
                        # td = threading.Thread(target=db_ctl_thread, args=(client, body_info))
                        # td.start()  # 执行线程
                        # td.join()   # 主线程阻塞，等待子线程结束

                        # 设置线程池来处理数据库事务
                        # 先保留future变量 方便以后使用
                        future = p.submit(db_ctl_thread, client, body_info)

                        # ifx.db_add(client, body_info)

                    else:
                        # 关闭连接
                        sock_dicts[sock_fileno].close()
                        # 注销epoll检测对象
                        epoll.unregister(sock_fileno)
                        # 数据为空，则客户端断开连接，删除相关数据
                        del sock_dicts[sock_fileno]
                        del client_dicts[sock_fileno]
                #  print(f'used: {time.time() - start}s.')
        except:
            raise

if __name__ == '__main__':
    main()
