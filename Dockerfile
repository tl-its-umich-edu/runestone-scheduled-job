FROM docker.io/python:2.7

COPY requirements.txt /requirements.txt

COPY caliper_sender.py /caliper_sender.py

# COPY startup script into known file location in container
COPY start.sh /start.sh

RUN pip install -r requirements.txt

# Get caliper
RUN pip install git+https://github.com/IMSGlobal/caliper-python@develop

# Sets the local timezone of the docker image
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone


EXPOSE 8000

CMD ["/start.sh"]
