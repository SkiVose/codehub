from influxdb import InfluxDBClient
import datetime
import json

# 初始化数据库
def db_init():
    # 连接数据库
    client = InfluxDBClient('localhost', 8086, 'admin', 'admin', '')
    # 创建新的数据库
    client.create_database('testdb')
    # 连接到新的数据库
    client = InfluxDBClient('localhost', 8086, 'admin', 'admin', 'testdb')
    # 返回该连接
    return client

# 写数据
def db_add(client, data):
    try:
        # 对数据进行处理
        body = json.loads(data)
        # print(type(body))
        # print(body)

        # 写入数据库
        client.write_points(body)
        # print('数据已写入...')
    except:
        raise
# 删除数据
def db_remove(client, msg):
    pass

# 删除数据库
def db_drop(client, dbname):
    client.drop_database(dbname)   