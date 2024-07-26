import psycopg2
import requests
from typing import List, Dict, Any


def get_hh_ru_data(company_ids: List[str]) -> List[Dict[str, Any]]:
    """ Получение данных о работодателях и вакансиях с использованием API HH.ru."""
    params = {
        'area': 1,
        'page': 0,
        'per_page': 100
    }
    data = []
    vacancies = []
    employers = []
    for employer_id in company_ids:
        url_emp = f"https://api.hh.ru/employers/{employer_id}"
        employer_info = requests.get(url_emp, ).json()
        employers.append(employer_info)

        url_vac = f"https://api.hh.ru/vacancies?employer_id={employer_id}"
        vacancies_info = requests.get(url_vac, params=params).json()
        vacancies.extend(vacancies_info['items'])
    data.append({
        'employers': employers,
        'vacancies': vacancies
    })
    return data


def create_database(database_name: str, params: dict) -> None:

    """Создание базы данных и таблиц для сохранения данных о компаниях и их вакансиях"""

    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f'DROP DATABASE IF EXISTS {database_name}')
    cur.execute(f'CREATE DATABASE {database_name}')

    cur.close()
    conn.close()

    conn = psycopg2.connect(dbname=database_name, **params)
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE employers (
                employer_id SERIAL PRIMARY KEY,
                company_name VARCHAR(300) NOT NULL,
                open_vacancies INTEGER,
                employer_url TEXT,
                description TEXT)
        """)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE vacancies (
                vacancy_id SERIAL PRIMARY KEY,
                employer_id INT ,
                vacancy_name VARCHAR(255) NOT NULL,
                salary_from INTEGER,
                vacancy_url TEXT)
        """)

    conn.commit()
    conn.close()


def save_data_to_database_emp(data: list[dict[str, Any]], database_name: str, params: dict) -> None:
    """Сохранение данных о компаниях и их вакансиях """

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        for text in data:
            employer_data = text['employers']
            for emp in employer_data:
                cur.execute(
                    """
                    INSERT INTO employers (employer_id, company_name, open_vacancies, employer_url, description)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (emp['id'], emp['name'], emp['open_vacancies'], emp['alternate_url'],
                     emp['description'])
                )

    conn.commit()
    conn.close()


def save_data_to_database_vac(data: list[dict[str, Any]], database_name: str, params: dict) -> None:
    """Сохранение данных о компаниях и их вакансиях """

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        for text in data:
            vacancies_data = text['vacancies']
            for vacancy in vacancies_data:
                if vacancy['salary'] is None:
                    cur.execute(
                        """
                        INSERT INTO vacancies (vacancy_id, employer_id, vacancy_name, salary_from, vacancy_url)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (vacancy['id'], vacancy['employer']['id'], vacancy['name'], 0,
                         vacancy['alternate_url'])
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO vacancies (vacancy_id, employer_id, vacancy_name, salary_from, vacancy_url)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (vacancy['id'], vacancy['employer']['id'], vacancy['name'], vacancy['salary']['from'],
                         vacancy['alternate_url'])
                    )

    conn.commit()
    conn.close()
