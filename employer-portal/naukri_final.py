from pyspark.sql import SparkSession

import os
import sys
import time
import logging
import json

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

from bs4 import BeautifulSoup
from random import uniform
from pyspark.sql.types import *
from datetime import datetime
from time import sleep
from retry import retry
from NoResultsFoundException import NoResultsFoundException


# Set login URL
NaukriURL = "https://enterprise.naukri.com"

logging.basicConfig(
    level=logging.INFO, filename="naukri.log", format="%(asctime)s    : %(message)s"
)
# logging.disable(logging.CRITICAL)
os.environ["WDM_LOG_LEVEL"] = "0"


def log_msg(message):
    """Print to console and store to Log"""
    print(message)
    logging.info(message)


def catch(error):
    """Method to catch errors and log error details"""
    exc_type, exc_obj, exc_tb = sys.exc_info()
    line_no = str(exc_tb.tb_lineno)
    msg = "%s : %s at Line %s." % (type(error), error, line_no)
    print(msg)
    logging.error(msg)


def get_obj(locator_type):
    """This map defines how elements are identified"""
    map = {
        "ID": By.ID,
        "NAME": By.NAME,
        "XPATH": By.XPATH,
        "TAG": By.TAG_NAME,
        "CLASS": By.CLASS_NAME,
        "CSS": By.CSS_SELECTOR,
        "LINKTEXT": By.LINK_TEXT
    }
    return map[locator_type]


def get_element(driver, element_tag, locator="ID"):
    """Wait max 15 secs for element and then select when it is available"""
    try:
        def _get_element(_tag, _locator):
            _by = get_obj(_locator)
            if is_element_present(driver, _by, _tag):
                return WebDriverWait(driver, 15).until(
                    lambda d: driver.find_element(_by, _tag))

        element = _get_element(element_tag, locator.upper())
        if element:
            return element
        else:
            log_msg("Element not found with %s : %s" % (locator, element_tag))
            return None
    except Exception as e:
        catch(e)
    return None


def is_element_present(driver, how, what):
    """Returns True if element is present"""
    try:
        driver.find_element(by=how, value=what)
    except NoSuchElementException:
        return False
    return True


def wait_till_element_present(driver, element_tag, locator="ID", timeout=30):
    """Wait till element present. Default 30 seconds"""
    result = False
    driver.implicitly_wait(0)
    locator = locator.upper()

    for i in range(timeout):
        time.sleep(0.99)
        try:
            if is_element_present(driver, get_obj(locator), element_tag):
                result = True
                break
        except Exception as e:
            log_msg(f'Exception when wait_till_element_present : {e}')
            pass

    if not result:
        log_msg("Element not found with %s : %s" % (locator, element_tag))
    driver.implicitly_wait(3)
    return result


def tear_down(driver):
    try:
        driver.close()
        log_msg("Driver Closed Successfully")
    except Exception as e:
        catch(e)
        pass

    try:
        driver.quit()
        log_msg("Driver Quit Successfully")
    except Exception as e:
        catch(e)
        pass


def load_naukri(headless):
    """Open Chrome to load Naukri.com"""
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-notifications")
    options.add_argument("--start-maximized")  # ("--kiosk") for MAC
    options.add_argument("--disable-popups")
    options.add_argument("--disable-gpu")
    options.add_argument("user-data-dir=C:\\Users\\rohan\\AppData\\Local\\Google\\Chrome\\User Data")
    options.add_argument("--profile-directory=Profile 4")
    if headless:
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("headless")

    driver = None
    try:
        s = Service('../chromedriver_win32/chromedriver.exe')
        driver = webdriver.Chrome(service=s, options=options)
    except:
        driver = webdriver.Chrome(options=options)
        print("Exception caught. Starting Chrome again...")
    log_msg("Google Chrome Launched!")

    driver.implicitly_wait(3)
    driver.get(NaukriURL)
    return driver


def naukri_login(properties, headless=False):
    """Open Chrome browser and Login to Naukri.com"""
    status = False
    driver = None

    try:
        driver = load_naukri(headless)

        if "naukri" in driver.title.lower():
            log_msg("Website Loaded Successfully.")

        if not is_element_present(driver, By.ID, "Sug_autoComplete"):
            login_tab = None
            if is_element_present(driver, By.ID, "toggleRCBLoginForm"):
                login_tab = driver.find_element(By.ID, "toggleRCBLoginForm")
                login_tab = login_tab.find_element(By.ID, "toggleForm")
                login_tab = login_tab.find_elements_by_xpath(".//*")[1]
            if login_tab is not None:
                login_tab.click()

            if is_element_present(driver, By.ID, "loginEmail"):
                email_field_element = get_element(driver, "loginEmail", locator="ID")
                time.sleep(1)
                pass_field_element = get_element(driver, "password", locator="ID")
                time.sleep(1)
                login_xpath = '//*[@type="submit"]'
                login_button = driver.find_element(By.XPATH, login_xpath)
            elif is_element_present(driver, By.NAME, "userName"):
                email_field_element = get_element(driver, "userName", locator="NAME")
                time.sleep(1)
                pass_field_element = get_element(driver, "password", locator="NAME")
                time.sleep(1)
                login_xpath = '//*[@type="submit"]'
                login_button = driver.find_element(By.XPATH, login_xpath)
            else:
                log_msg("No elements found to login.")

            if email_field_element is not None:
                email_field_element.clear()
                email_field_element.send_keys(properties["username"])
                time.sleep(1)
                pass_field_element.clear()
                pass_field_element.send_keys(properties["password"])
                time.sleep(1)
                login_button.send_keys(Keys.ENTER)
                time.sleep(5)

        # CheckPoint to verify login
        if wait_till_element_present(driver, "Sug_autoComplete", locator="ID", timeout=40):
            check_point = get_element(driver, "Sug_autoComplete", locator="ID")
            if check_point:
                log_msg("Naukri Login Successful")
                status = True
                return status, driver
            else:
                log_msg("Unknown Login Error")
                return status, driver
        else:
            log_msg("Unknown Login Error")
            return status, driver

    except Exception as e:
        catch(e)
    return status, driver


@retry(NoResultsFoundException, delay=2, tries=5)
def get_resume_count(spark, driver, domain, tech_category, tech_name, current_date, complete_df):
    resumes_per_page_name = 'resumesPerPage'

    schema = StructType([
        StructField("domain", StringType(), False),
        StructField("category", StringType(), True),
        StructField("technology", StringType(), False),
        StructField("res_count", LongType(), False),
        StructField("created_date", StringType(), False)
    ])

    try:
        if is_element_present(driver, By.NAME, resumes_per_page_name):
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            script = soup.find_all('script')
            for x in script:
                line = str(x)
                pos = line.find('totalCount')
                if pos > -1:
                    words = line.split(' ')
                    res_count = words[words.index("searchData['totalCount']") + 2].strip()[:-1]
                    part_df = spark.createDataFrame([(domain,
                                                      tech_category,
                                                      tech_name,
                                                      int(res_count),
                                                      current_date)],
                                                    schema)
                    complete_df = complete_df.union(part_df)
                    break
            return complete_df
        else:
            raise NoResultsFoundException(f"No results found for '{tech_name}'.")
    except Exception:
        driver.back()
        search_box_name = 'ezString'
        search_btn_id = 'gnbSrchBtn'
        search_box_element = get_element(driver, search_box_name, locator="NAME")
        search_button_element = get_element(driver, search_btn_id, locator="ID")
        search_box_element.send_keys(tech_name)
        search_button_element.send_keys(Keys.ENTER)
        time.sleep(1)
        raise


def get_data(spark, driver, input):
    """Populate dataframe with resume count"""
    schema = StructType([
        StructField("domain", StringType(), False),
        StructField("category", StringType(), True),
        StructField("technology", StringType(), False),
        StructField("res_count", LongType(), False),
        StructField("created_date", StringType(), False)
    ])

    complete_df = spark.createDataFrame([('', '', '', 0, '')], schema)

    search_box_name = 'ezString'
    search_btn_id = 'gnbSrchBtn'
    resumes_per_page_name = 'resumesPerPage'

    current_date = str(datetime.now())

    for obj in input:
        domain = obj["domain"][0]
        for tech in obj["technology"]:
            sleep(round(uniform(0.00, 5.00), 2))
            if is_element_present(driver, By.NAME, search_box_name):
                search_box_element = get_element(driver, search_box_name, locator="NAME")
                search_button_element = get_element(driver, search_btn_id, locator="ID")
                util_policy_popup_class = 'btn btn-primary btnCtr lt_close'

                tech_name = tech["name"]
                tech_category = tech["category"]
                search_box_element.send_keys(tech_name)
                search_button_element.send_keys(Keys.ENTER)
                time.sleep(1)
                if is_element_present(driver, By.CLASS_NAME, util_policy_popup_class):
                    util_policy_popup_element = get_element(driver, util_policy_popup_class, locator="CLASS_NAME")
                    util_policy_popup_element.send_keys(Keys.ENTER)
                complete_df = get_resume_count(spark, driver, domain, tech_category,
                                               tech_name, current_date, complete_df)
                # if is_element_present(driver, By.NAME, resumes_per_page_name):
                #     soup = BeautifulSoup(driver.page_source, 'html.parser')
                #     script = soup.find_all('script')
                #     for x in script:
                #         line = str(x)
                #         pos = line.find('totalCount')
                #         if pos > -1:
                #             words = line.split(' ')
                #             res_count = words[words.index("searchData['totalCount']") + 2].strip()[:-1]
                #             part_df = spark.createDataFrame([(domain,
                #                                               tech_category,
                #                                               tech_name,
                #                                               int(res_count),
                #                                               current_date)],
                #                                             schema)
                #             complete_df = complete_df.union(part_df)
                #             break
                # else:
                #     log_msg(f"No results found for '{tech_name}'.")
            else:
                log_msg("No search box found.")
    complete_df = complete_df.withColumn('created_ts', complete_df['created_date'].cast(TimestampType())) \
        .drop('created_date')
    return complete_df


def ingest_data(properties, complete_df):
    """Ingest data to Local PostgreSQL Server"""
    complete_df = complete_df.where("technology != ''")
    # complete_df.show(truncate=False)
    complete_df.write.format("jdbc").mode("append") \
        .options(
        url='jdbc:postgresql://localhost:5432/postgres',
        dbtable='main.resume_count',
        user=properties["pgUser"],
        password=properties["pgPassword"],
        driver='org.postgresql.Driver') \
        .save()
    log_msg(f"Ingested {complete_df.count()} records.")


def main():
    log_msg("-----Naukri.py Script Run Begin-----")

    spark = SparkSession \
        .builder \
        .appName("ingestion") \
        .config("spark.jars", "../Database/postgresql-42.3.1.jar") \
        .getOrCreate()

    with open("data.json", "r") as jsonfile:
        input = json.load(jsonfile)

    with open("properties.json", "r") as jsonfile:
        properties = json.load(jsonfile)

    driver = None
    try:
        status, driver = naukri_login(properties)
        if status:
            complete_df = get_data(spark, driver, input["data"])
            ingest_data(properties, complete_df)

    except Exception as e:
        catch(e)

    finally:

        tear_down(driver)

    log_msg("-----Naukri.py Script Run Ended-----\n")


if __name__ == "__main__":
    main()





