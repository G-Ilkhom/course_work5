from DBManager import DBManager


db_manager = DBManager('course_work5', 'postgres', '12345', 'localhost')

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
