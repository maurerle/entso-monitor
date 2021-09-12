FROM python:3.8-slim
RUN useradd -ms /bin/bash admin
RUN pip install --no-cache-dir Flask pandas numpy dash

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

USER admin
WORKDIR /app
COPY entsoe/* /app/
COPY entsog/* /app/
COPY . /app

ENV GUNICORN_CMD_ARGS="--bind=0.0.0.0:8000 --chdir=./ --worker-tmp-dir /dev/shm --workers=2 --threads=4 --worker-class=gthread"

EXPOSE 8000

CMD ["gunicorn", "index:server"] #.py"]