FROM python:3.8.5-slim-buster

RUN apt-get update
RUN apt-get install -y default-libmysqlclient-dev
RUN apt-get install -y gcc

COPY setup.py setup.py
RUN python3 setup.py install

COPY . .
CMD ["./rars-import/__main__.py"]

ENTRYPOINT [ "python3" ]