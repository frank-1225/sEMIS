# 使用官方 Python 基础镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 复制依赖文件和数据文件
COPY requirements.txt .
COPY traindata.csv .

# 安装依赖
# 注意: PyMySQL 是纯 Python 驱动，通常不需要安装额外的 C 库
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY app.py .

# 暴露 Flask 默认端口
EXPOSE 5000

# 运行应用
CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]