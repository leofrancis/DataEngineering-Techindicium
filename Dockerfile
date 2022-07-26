FROM python:3.10.5

COPY ./src ./src
COPY ./data/order_details.csv ./data/order_details.csv
COPY ./data/northwind.sql ./data/northwind.sql
COPY requirements.txt .

RUN sudo pip install --upgrade pip

RUN pip install -r requirements.txt

FROM library/postgres
COPY ./data/init.sql /docker-entrypoint-initdb.d/

CMD ["python", "./main.py"]
# CMD ["python", "./main.py", "2022-07-26"]