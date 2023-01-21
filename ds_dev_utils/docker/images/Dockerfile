FROM python

ENV PYTHONUNBUFFERED=1

WORKDIR /usr/src/ds

RUN mkdir /usr/src/ds/functions
RUN mkdir /usr/src/ds/pickled
RUN mkdir /usr/src/ds/connectors
RUN mkdir -p /mnt/data_mount

COPY . .

RUN apt-get update
RUN apt-get install -y fuse
RUN apt-get install libfuse-dev
RUN rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt

# CMD ["python", "./setup.py"]