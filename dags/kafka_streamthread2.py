from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
import json
from selenium.webdriver.common.keys import Keys
import re
from kafka import KafkaProducer

# -------------------CRAWL DATA-----------------------------------------------------------
from bs4 import BeautifulSoup

TIME_SLEEP = 3
TIME_STREAM = 50
START_PAGE=71
END_PAGE=92
fix=1


# task 1: crawl link page

# examble links https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257?sort=up_top&type_keyword=0&page=2&category_family=r257&sba=
def crawl_page_links(**kwargs):
    links=[]
    for i in range(START_PAGE, END_PAGE + 1):
        links.append(
            "https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257?sort=up_top&type_keyword=0&page="+str(i)+"&category_family=r257&sba="
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
            driver.quit()
    except Exception as e:
        print("Error: " + str(e))
        driver.quit()
    finally:
        driver.quit()
    kwargs['ti'].xcom_push(key='company_links', value=company_links)
def extract_sections(text):
    
    headers = [
        "Mô tả công việc",
        "Yêu cầu ứng viên",
        "Quyền lợi",
        "Địa điểm làm việc",
        "Thời gian làm việc",
        "Cách thức ứng tuyển"
    ]

    
    pattern = "|".join(re.escape(h) for h in headers)
    
    
    splits = re.split(f"({pattern})", text)

    
    result = {}
    for i in range(1, len(splits) - 1, 2):
        header = splits[i].strip()
        content = splits[i + 1].strip()
        result[header] = content

    return result

# task 3: crawl data company
def crawl_company_data(**kwargs):
    company_links = kwargs['ti'].xcom_pull(key='company_links', task_ids='crawl_company_links')
    company_data_list = []
    
    try:
        for link in company_links:
            print("link: " + str(link))
            driver = get_webdriver()
            driver.get(link)

            try:
                name_element = driver.find_element(By.CSS_SELECTOR, "div.company-name-label a")
                name = name_element.text.strip()
            except Exception as e:
                print("Error: " + str(e))
                name = "N/A"
            try:
                job_description_element = driver.find_element(By.CSS_SELECTOR, "div.job-description")
                job_description = job_description_element.text.strip()
                sections = extract_sections(job_description)
            except Exception as e:
                print("Error: " + str(e))
                job_description = "N/A"
            
            # get data chuyen mon from <a> class="item search-from-tag link"
            try:
                chuyen_mon_element = driver.find_elements(By.CSS_SELECTOR, "a.item.search-from-tag.link")
                # get text first element
                chuyen_mon = chuyen_mon_element[0].text.strip()
                # example data chuyen_mon: "Chuyên môn Software Engineer" -> "Software Engineer"
                chuyen_mon = chuyen_mon.replace("Chuyên môn ", "")
            except Exception as e:
                print("Error: " + str(e))
                chuyen_mon = "N/A"

            company_data={
                'id':time.time(),
                'name': name,
                'chuyen_mon': chuyen_mon,
                'mo_ta_cong_viec': sections.get("Mô tả công việc", "N/A"),
                'yeu_cau_cong_viec': sections.get("Yêu cầu ứng viên", "N/A"),
                'quyen_loi': sections.get("Quyền lợi", "N/A"),
                'dia_diem_lam_viec': sections.get("Địa điểm làm việc", "N/A"),
                'thoi_gian_lam_viec': sections.get("Thời gian làm việc", "N/A"),
                'cach_thuc_ung_tuyen': sections.get("Cách thức ứng tuyển", "N/A")
            }
            company_data_list.append(company_data)
            driver.quit()
            # i put break here to test, if you want run project, u need delete "break"
            # break
            
    except Exception as e:
        driver.quit()
        print("Error: " + str(e))
    finally:
        driver.quit()
    kwargs['ti'].xcom_push(key='company_data', value=company_data_list)


# Task 4: send data to kafka
def send_to_kafka(**kwargs):
    
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    company_data_list = kwargs['ti'].xcom_pull(key='company_data', task_ids='crawl_company_data')

    for data in company_data_list:
        producer.send('recruitment_information', json.dumps(data).encode('utf-8'))
        time.sleep(3)

# Task 2+3+4: crawl data company and send to kafka
def crawl_and_send_to_kafka(**kwargs):
    
    
    try:
        page_links = kwargs['ti'].xcom_pull(key='page_links', task_ids='crawl_page_links')
        producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
        for link in page_links:
           
           print("link: " + str(link))
           driver = get_webdriver()
           driver.get(link)
           # check data
           
           time.sleep(TIME_SLEEP)
           
           titles = driver.find_elements(By.CSS_SELECTOR, "h3.title a")
           for title in titles:
                link = title.get_attribute("href")
                
                print("link: " + str(link))
                driver_data = get_webdriver()
                driver_data.get(link)
                time.sleep(TIME_SLEEP)
                try:
                    name_element = driver_data.find_element(By.CSS_SELECTOR, "div.company-name-label a")
                    name = name_element.text.strip()
                except Exception as e:
                    print("Error: " + str(e))
                    name = "N/A"
                try:
                    job_description_element = driver_data.find_element(By.CSS_SELECTOR, "div.job-description")
                    job_description = job_description_element.text.strip()
                    sections = extract_sections(job_description)
                except Exception as e:
                    print("Error: " + str(e))
                    job_description = "N/A"

                # get data chuyen mon from <a> class="item search-from-tag link"
                try:
                    chuyen_mon_element = driver_data.find_elements(By.CSS_SELECTOR, "a.item.search-from-tag.link")
                    # get text first element
                    chuyen_mon = chuyen_mon_element[0].text.strip()
                    # example data chuyen_mon: "Chuyên môn Software Engineer" -> "Software Engineer"
                    chuyen_mon = chuyen_mon.replace("Chuyên môn ", "")
                except Exception as e:
                    print("Error: " + str(e))
                    chuyen_mon = "N/A"
                company_data={
                    'id':time.time(),
                    'name': name,
                    'chuyen_mon': chuyen_mon,
                    'mo_ta_cong_viec': sections.get("Mô tả công việc", "N/A"),
                    'yeu_cau_cong_viec': sections.get("Yêu cầu ứng viên", "N/A"),
                    'quyen_loi': sections.get("Quyền lợi", "N/A"),
                    'dia_diem_lam_viec': sections.get("Địa điểm làm việc", "N/A"),
                    'thoi_gian_lam_viec': sections.get("Thời gian làm việc", "N/A"),
                    'cach_thuc_ung_tuyen': sections.get("Cách thức ứng tuyển", "N/A")
                }
                
                producer.send('recruitment_information', json.dumps(company_data).encode('utf-8'))
                driver_data.quit()
                # i put break here to test, if you want run project, u need delete "break"
            
    except Exception as e:
        driver.quit()
        print("Error: " + str(e))
    finally:
        driver.quit()
        producer.close()

# ------------------------------------------------------------------------------

default_args = {
    'owner': 'vancuong',
    'start_date': datetime(2024, 12, 22, 10, 00),
    'retries': 5,
    'retry_delay': timedelta(minutes = 2)
}


with DAG('stream_recruitment_information_thread2',
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
    # crawl_and_send_to_kafka= PythonOperator(
    #     task_id='crawl_and_send_to_kafka',
    #     python_callable=crawl_and_send_to_kafka,
    #     provide_context=True
    # )

    crawl_page_links_task >> crawl_company_links_task>> crawl_company_data_task >> send_to_kafka_task
    #crawl_page_links_task >> crawl_and_send_to_kafka
