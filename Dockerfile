FROM python:3.10.5

COPY . .

RUN pip install --upgrade pip

RUN pip install -r requirements.txt

FROM library/postgres
COPY ./data/init.sql /docker-entrypoint-initdb.d/

# CMD ["python", "./src/main.py"]
# CMD ["python", "./main.py", "2022-07-26"]