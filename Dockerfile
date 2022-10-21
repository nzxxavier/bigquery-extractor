FROM python:3.9.7
WORKDIR /bigquery-extractor
COPY . /bigquery-extractor
RUN pip install --upgrade pip -i https://repo.huaweicloud.com/repository/pypi/simple/ \
 && pip install --no-cache-dir -r requirements.txt -i https://repo.huaweicloud.com/repository/pypi/simple/

ENTRYPOINT ["python", "start.py"]