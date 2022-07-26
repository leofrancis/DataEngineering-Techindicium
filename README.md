# DataEngineering-Techindicium

<h4> Steps for generating: </h4> <br>
I. Pipeline: Running step by step <br>
I.1) Considering a Linux OS, run on terminal: pip install -r requirements.txt <br>
I.2) Configure the files database.ini for the northwind database and database_exp.ini for the database of exporting <br>
I.3) On IntegrationPileline inform the INPUT_DATE as (Year-Month-Day) and remove the # from the beginning for early dates or just run the code for the current date. <br>
I.4) Run all pipelines of code, each code block will be responsible for an act of EXTRACTING DATA from CSV and POSTGRES, and EXPORT for generating the second database. <br>
I.4.1) At the end, it will generate a file result-(year-month-day).csv on the data folder <br>
<br>
<br>
II. Automatically <br>
II.1) Considering a Linux OS, and after installing the docker run: sudo docker-compose up <br> 
II.2) Generate Image: sudo docker build -t python-01 . <br>
II.3) Running Image: sudo docker run python-01 <br>
II.3.1) python3.10 src/main.py 'Year-Month-Day' <br> 
II.3.2) python3.10 src/main.py <br> 