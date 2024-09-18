FROM python:3.10

RUN apt-get update && apt-get install -y libsnappy-dev

RUN git clone https://github.com/Gin-G/NickKnows.git

WORKDIR NickKnows

RUN pip install -r requirements.txt

CMD [ "python3", "./wsgi.py" ]