import requests
import psycopg2
from psycopg2 import sql

# Список ID организаций
employer_ids = ["2501719", "3202190", "2533", "9450736", "3934385", "227", "1204987", "2644166", "723204", "1008541"]

# Получение данных о работодателях с hh.ru
employers = []
for id in employer_ids:
    url = f"https://api.hh.ru/employers/{id}"
    response = requests.get(url)
    employer = response.json()
    employer['url'] = f"https://hh.ru/employer/{employer['id']}"  # Формируем URL работодателя
    employers.append(employer)

# Получение данных о вакансиях
vacancies = []
for employer in employers:
    page = 0
    while True:
        url = f"https://api.hh.ru/vacancies?employer_id={employer['id']}&page={page}"
        response = requests.get(url)
        if 'items' in response.json() and response.json()['items']:
            vacancies.extend(response.json()['items'])  # Добавляем вакансии этого работодателя
            page += 1
        else:
            break

# Подключение к БД
conn = psycopg2.connect(dbname='course_work5', user='postgres', password='12345', host='localhost')
cursor = conn.cursor()

# Создание таблиц
commands = (
    """
    CREATE TABLE employers (
        id INTEGER PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        url VARCHAR(255) NOT NULL
    )
    """,
    """ 
    CREATE TABLE vacancies (
        id INTEGER PRIMARY KEY,
        employer_id INTEGER NOT NULL,
        name VARCHAR(255) NOT NULL,
        salary_from INTEGER,
        salary_to INTEGER,
        url VARCHAR(255),
        requirement TEXT,
        FOREIGN KEY (employer_id)
        REFERENCES employers (id)
        ON UPDATE CASCADE ON DELETE CASCADE
    )
    """
)

for command in commands:
    cursor.execute(command)

# Заполнение таблиц данными
for employer in employers:
    insert_query = sql.SQL("INSERT INTO employers (id, name, url) VALUES (%s, %s, %s)")
    cursor.execute(insert_query, (employer['id'], employer['name'], employer['url']))

for vacancy in vacancies:
    salary_from = vacancy['salary']['from'] if 'salary' in vacancy and vacancy['salary'] is not None and 'from' in \
                                               vacancy['salary'] else None
    salary_to = vacancy['salary']['to'] if 'salary' in vacancy and vacancy['salary'] is not None and 'to' in vacancy[
        'salary'] else None
    requirement = vacancy['snippet']['requirement'] if 'snippet' in vacancy and vacancy[
        'snippet'] is not None and 'requirement' in vacancy['snippet'] else 'Нет информации о требованиях'
    url = vacancy['alternate_url'] if 'alternate_url' in vacancy else 'Нет информации об URL-адресе'
    insert_query = sql.SQL(
        "INSERT INTO vacancies (id, employer_id, name, salary_from, salary_to, url, requirement) VALUES (%s, %s, %s, %s, %s, %s, %s)")
    cursor.execute(insert_query, (
    vacancy['id'], vacancy['employer']['id'], vacancy['name'], salary_from, salary_to, url, requirement))

# Закрытие соединения
conn.commit()
cursor.close()
conn.close()
