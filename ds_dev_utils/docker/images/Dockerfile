FROM python

WORKDIR /usr/src/ds

RUN mkdir /usr/src/ds/functions

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

# CMD ["python", "./setup.py"]