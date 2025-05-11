from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
import json
from selenium.webdriver.common.keys import Keys

# -------------------CRAWL DATA-----------------------------------------------------------
from bs4 import BeautifulSoup

TIME_SLEEP = 5
TIME_STREAM = 50
START_PAGE=1
END_PAGE=1
fix=1


# task 1: crawl link page
#examble links https://www.vietnamworks.com/viec-lam-tot-nhat?page=0
# examble links https://www.topcv.vn/tim-viec-lam-software-engineering-cr257cb258?company_field=0&exp=0&page=1&salary=0&sort=up_top
def crawl_page_links(**kwargs):
    links=[]
    for i in range(START_PAGE, END_PAGE + 1):
        links.append(
            "https://www.topcv.vn/tim-viec-lam-software-engineering-cr257cb258?company_field=0&exp=0&page=" + str(i) + "&salary=0&sort=up_top"
        )
    print("links: " + str(links))
    kwargs['ti'].xcom_push(key='page_links', value=links)
def get_webdriver():
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    chrome_options = Options()
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0")
    chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
    driver_path = "/usr/bin/chromedriver"
    service = Service(driver_path)
    return webdriver.Chrome(service=service, options=chrome_options)
# task 2: crawl list company
def crawl_company_links(**kwargs):
    page_links = kwargs['ti'].xcom_pull(key='page_links', task_ids='crawl_page_links')
    company_links = []

    
    try:
        for link in page_links:
            print("link: " + str(link))
            driver = get_webdriver()
            driver.get(link)

            time.sleep(3)
            
            titles = driver.find_elements(By.CSS_SELECTOR, "h3.title a")
            for title in titles:
                company_links.append(title.get_attribute("href"))  
    except Exception as e:
        print("Error: " + str(e))
    finally:
        driver.quit()
    kwargs['ti'].xcom_push(key='company_links', value=company_links)

# task 3: crawl data company
def crawl_company_data(**kwargs):
    company_links = kwargs['ti'].xcom_pull(key='company_links', task_ids='crawl_company_links')
    company_data_list = []
    
    try:
        for link in company_links:
            print("link: " + str(link))
            driver = get_webdriver()
            driver.get(link)
            time.sleep(3)

            try:
                name_element = driver.find_element(By.CSS_SELECTOR, "div.company-name-label a")
                name = name_element.text.strip()
            except Exception as e:
                print("Error: " + str(e))
                name = "N/A"
            try:
                job_description_element = driver.find_element(By.CSS_SELECTOR, "div.job-description")
                job_description = job_description_element.text.strip()
            except Exception as e:
                print("Error: " + str(e))
                job_description = "N/A"
            company_data={
                'id':time.time(),
                'name': name,
                'job_description': job_description,
            }
            company_data_list.append(company_data)
            break
            
    except Exception as e:
        print("Error: " + str(e))
    finally:
        driver.quit()
    kwargs['ti'].xcom_push(key='company_data', value=company_data_list)


# Task 4: Gửi dữ liệu đến Kafka
def send_to_kafka(**kwargs):
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    company_data_list = kwargs['ti'].xcom_pull(key='company_data', task_ids='crawl_company_data')

    for data in company_data_list:
        producer.send('recruitment_information', json.dumps(data).encode('utf-8'))
        time.sleep(3)

# ------------------------------------------------------------------------------

default_args = {
    'owner': 'vancuong',
    'start_date': datetime(2024, 12, 22, 10, 00),
    'retries': 5,
    'retry_delay': timedelta(minutes = 2)
}


with DAG('stream_recruitment_information',
         default_args=default_args,
         description = "This is kafka stream task.",
         schedule_interval='@daily',
         catchup=False) as dag:

    crawl_page_links_task= PythonOperator(
        task_id='crawl_page_links',
        python_callable=crawl_page_links,
        provide_context=True
    )
    crawl_company_links_task= PythonOperator(
        task_id='crawl_company_links',
        python_callable=crawl_company_links,
        provide_context=True
    )
    crawl_company_data_task= PythonOperator(
        task_id='crawl_company_data',
        python_callable=crawl_company_data,
        provide_context=True
    )
    send_to_kafka_task= PythonOperator(
        task_id='send_to_kafka',
        python_callable=send_to_kafka,
        provide_context=True
    )

    crawl_page_links_task >> crawl_company_links_task>> crawl_company_data_task>> send_to_kafka_task