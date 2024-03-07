import requests


class HhScraper:
    def __init__(self, employer_ids):
        self.employer_ids = employer_ids
        self.employers = []
        self.vacancies = []

    def get_employers(self):
        for id in self.employer_ids:
            url = f"https://api.hh.ru/employers/{id}"
            response = requests.get(url)
            employer = response.json()
            employer['url'] = f"https://hh.ru/employer/{employer['id']}"  # Формируем URL работодателя
            self.employers.append(employer)

    def get_vacancies(self):
        for employer in self.employers:
            page = 0
            while True:
                url = f"https://api.hh.ru/vacancies?employer_id={employer['id']}&page={page}"
                response = requests.get(url)
                if 'items' in response.json() and response.json()['items']:
                    self.vacancies.extend(response.json()['items'])  # Добавляем вакансии этого работодателя
                    page += 1
                else:
                    break
