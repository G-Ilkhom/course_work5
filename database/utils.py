from psycopg2 import sql


def create_database(cursor, dbname):
    cursor.execute(f"DROP DATABASE {dbname}")
    cursor.execute(f"CREATE DATABASE {dbname}")


def create_tables(cursor):
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


def insert_data(cursor, scraper):
    for employer in scraper.employers:
        insert_query = sql.SQL(
            "INSERT INTO employers (id, name, url) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING")
        cursor.execute(insert_query, (employer['id'], employer['name'], employer['url']))

    for vacancy in scraper.vacancies:
        salary_from = vacancy['salary']['from'] if 'salary' in vacancy and vacancy['salary'] is not None and 'from' in \
                                                   vacancy['salary'] else None
        salary_to = vacancy['salary']['to'] if 'salary' in vacancy and vacancy['salary'] is not None and 'to' in \
                                               vacancy[
                                                   'salary'] else None
        requirement = vacancy['snippet']['requirement'] if 'snippet' in vacancy and vacancy[
            'snippet'] is not None and 'requirement' in vacancy['snippet'] else 'Нет информации о требованиях'
        url = vacancy['alternate_url'] if 'alternate_url' in vacancy else 'Нет информации об URL-адресе'
        insert_query = sql.SQL(
            "INSERT INTO vacancies (id, employer_id, name, salary_from, salary_to, url, requirement) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING")
        cursor.execute(insert_query, (
            vacancy['id'], vacancy['employer']['id'], vacancy['name'], salary_from, salary_to, url, requirement))
