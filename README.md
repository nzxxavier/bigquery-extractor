# 脚本运行方式
## 安装python依赖
```shell
pip install -r requirements.txt
```

## 修改配置文件
```shell
cp config.ini.example config.ini
vim reader/reader.py 
修改 BASE_DIR = '.'
```

## 环境变量
```shell
export GOOGLE_APPLICATION_CREDENTIALS="XXX"
```

## 启动脚本
```shell
# 读取当天1小时内数据
# python start.py minute 0
# 读取当天4小时内数据
# python start.py hour 0
# 读取当天全量数据
# python start.py day 0

# 读取昨天天全量
# python start.py day 1
# 读取前天天全量
# python start.py day 2
```
