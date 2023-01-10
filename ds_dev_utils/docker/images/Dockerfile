FROM python

ENV PYTHONUNBUFFERED=1

WORKDIR /usr/src/ds

RUN mkdir /usr/src/ds/functions
RUN mkdir /usr/src/ds/pickled
RUN mkdir /usr/src/ds/connectors

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

# CMD ["python", "./setup.py"]