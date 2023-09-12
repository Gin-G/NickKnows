FROM python:3
RUN git clone https://github.com/Gin-G/NickKnows.git
WORKDIR NickKnows
RUN apt-get install libsnappy-dev
RUN pip install -r requirements.txt
CMD [ "python3", "./wsgi.py" ]