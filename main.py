import config
import psycopg2
from hh_data import HhScraper
from database.utils import create_database, create_tables, insert_data
from database.DBManager import DBManager

# Список ID организаций
employer_ids = ["2501719", "3202190", "2533", "9450736", "3934385", "227", "1204987", "2644166", "723204", "1008541"]

scraper = HhScraper(employer_ids)
scraper.get_employers()
scraper.get_vacancies()

# Создание соединения
conn = psycopg2.connect(dbname='postgres', user=config.user, password=config.password, host=config.host)
conn.autocommit = True

# Создание курсора
cursor = conn.cursor()

create_database(cursor, config.dbname)

# Подключение к БД
conn = psycopg2.connect(dbname=config.dbname, user=config.user, password=config.password, host=config.host)
cursor = conn.cursor()

create_tables(cursor)
insert_data(cursor, scraper)

# Закрытие соединения
conn.commit()
cursor.close()
conn.close()

db_manager = DBManager(config.dbname, config.user, config.password, config.host)

print("Компании и количество вакансий:")
companies_and_vacancies_count = db_manager.get_companies_and_vacancies_count()
for company in companies_and_vacancies_count:
    print(f"Компания: {company[0]}, Количество вакансий: {company[1]}")

print("\nВсе вакансии:")
all_vacancies = db_manager.get_all_vacancies()
for vacancy in all_vacancies:
    print(
        f"Компания: {vacancy[0]}, Вакансия: {vacancy[1]}, Зарплата от {vacancy[2]} до {vacancy[3]}, URL: {vacancy[4]}")

print("\nСредняя зарплата:")
avg_salary = db_manager.get_avg_salary()
print(f"Средняя зарплата: {avg_salary[0]}")

print("\nВакансии с зарплатой выше средней:")
vacancies_with_higher_salary = db_manager.get_vacancies_with_higher_salary()
for vacancy in vacancies_with_higher_salary:
    print(
        f"Компания: {vacancy[0]}, Вакансия: {vacancy[1]}, Зарплата от {vacancy[2]} до {vacancy[3]}, URL: {vacancy[4]}")

print("\nВакансии с ключевым словом:")
vacancies_with_keyword = db_manager.get_vacancies_with_keyword('Python')
for vacancy in vacancies_with_keyword:
    print(
        f"Компания: {vacancy[0]}, Вакансия: {vacancy[1]}, Зарплата от {vacancy[2]} до {vacancy[3]}, URL: {vacancy[4]}")

db_manager.close()
