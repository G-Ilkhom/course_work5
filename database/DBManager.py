import psycopg2


class DBManager:
    def __init__(self, dbname, user, password, host):
        self.conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
        self.cursor = self.conn.cursor()

    def get_companies_and_vacancies_count(self):
        self.cursor.execute(
            "SELECT e.name, COUNT(v.id) FROM employers e JOIN vacancies v ON e.id = v.employer_id GROUP BY e.name")
        return self.cursor.fetchall()

    def get_all_vacancies(self):
        self.cursor.execute(
            "SELECT e.name AS company, v.name, v.salary_from, v.salary_to, v.url FROM employers e JOIN vacancies v ON e.id = v.employer_id")
        return self.cursor.fetchall()

    def get_avg_salary(self):
        self.cursor.execute(
            "SELECT AVG((salary_from + salary_to) / 2) FROM vacancies WHERE salary_from IS NOT NULL AND salary_to IS NOT NULL")
        return self.cursor.fetchone()

    def get_vacancies_with_higher_salary(self):
        avg_salary = self.get_avg_salary()[0]
        self.cursor.execute(
            "SELECT e.name AS company, v.name, v.salary_from, v.salary_to, v.url FROM employers e JOIN vacancies v ON e.id = v.employer_id WHERE (salary_from + salary_to) / 2 > %s",
            (avg_salary,))
        return self.cursor.fetchall()

    def get_vacancies_with_keyword(self, keyword):
        self.cursor.execute(
            "SELECT e.name, v.name, v.salary_from, v.salary_to, v.url FROM employers e JOIN vacancies v ON e.id = v.employer_id WHERE v.name LIKE %s",
            ('%' + keyword + '%',))
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()
        self.conn.close()
