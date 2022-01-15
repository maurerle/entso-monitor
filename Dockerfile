FROM python:3.10-slim
RUN useradd -ms /bin/bash admin

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

USER admin
WORKDIR /app
COPY entsoe_vis/* /app/
COPY entsog/* /app/
COPY entsoe_vis/assets /app/assets
COPY . /app

ENV GUNICORN_CMD_ARGS="--bind=0.0.0.0:8000 --chdir=./ --worker-tmp-dir /dev/shm --workers=2 --threads=4 --worker-class=gthread"

EXPOSE 8000

CMD ["gunicorn", "index:server"] #.py"]
