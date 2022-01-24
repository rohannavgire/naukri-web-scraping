#! python3
# -*- coding: utf-8 -*-
"""Naukri Daily update - Using Chrome"""

from pyspark.sql import SparkSession
import re
import os
import io
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
from selenium.common.exceptions import NoAlertPresentException
# from PyPDF2 import PdfFileReader, PdfFileWriter
from bs4 import BeautifulSoup
from string import ascii_uppercase, digits
from random import choice, randint, uniform
from pyspark.sql.types import *
from datetime import datetime
from time import sleep
import json
# from reportlab.pdfgen import canvas
# from reportlab.lib.pagesizes import letter
from webdriver_manager.chrome import ChromeDriverManager as CM

# Add folder Path of your resume
originalResumePath = "original_resume.pdf"
# Add Path where modified resume should be saved
modifiedResumePath = "modified_resume.pdf"

# Update your naukri username and password here before running
username = "Type Your email ID Here"
password = "Type Your Password Here"
mob = "1234567890"  # Type your mobile number here

# False if you dont want to add Random HIDDEN chars to your resume
updatePDF = True

# ----- No other changes required -----

# Set login URL
# NaukriURL = "https://www.naukri.com/recruit/login?msg=TO&URL=https%3A%2F%2Frecruit.naukri.com%2FhomePage%2Findex"
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
    lineNo = str(exc_tb.tb_lineno)
    msg = "%s : %s at Line %s." % (type(error), error, lineNo)
    print(msg)
    logging.error(msg)


def getObj(locatorType):
    """This map defines how elements are identified"""
    map = {
        "ID" : By.ID,
        "NAME" : By.NAME,
        "XPATH" : By.XPATH,
        "TAG" : By.TAG_NAME,
        "CLASS" : By.CLASS_NAME,
        "CSS" : By.CSS_SELECTOR,
        "LINKTEXT" : By.LINK_TEXT
    }
    return map[locatorType]


def GetElement(driver, elementTag, locator="ID"):
    """Wait max 15 secs for element and then select when it is available"""
    try:
        def _get_element(_tag, _locator):
            _by = getObj(_locator)
            if is_element_present(driver, _by, _tag):
                return WebDriverWait(driver, 15).until(
                    lambda d: driver.find_element(_by, _tag))

        element = _get_element(elementTag, locator.upper())
        if element:
            return element
        else:
            log_msg("Element not found with %s : %s" % (locator, elementTag))
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


def WaitTillElementPresent(driver, elementTag, locator="ID", timeout=30):
    """Wait till element present. Default 30 seconds"""
    result = False
    driver.implicitly_wait(0)
    locator = locator.upper()

    for i in range(timeout):
        time.sleep(0.99)
        try:
            if is_element_present(driver, getObj(locator), elementTag):
                result = True
                break
        except Exception as e:
            log_msg('Exception when WaitTillElementPresent : %s' %e)
            pass

    if not result:
        log_msg("Element not found with %s : %s" % (locator, elementTag))
    driver.implicitly_wait(3)
    return result


def tearDown(driver):
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


def randomText():
    return "".join(choice(ascii_uppercase + digits) for _ in range(randint(1, 5)))


def LoadNaukri(headless):
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

    # updated to use ChromeDriverManager to match correct chromedriver automatically
    driver = None
    try:
        s = Service('../chromedriver_win32/chromedriver.exe')
        driver = webdriver.Chrome(service=s, options=options)
        # driver = webdriver.Chrome(executable_path=CM().install(), options=options)
    except:
        driver = webdriver.Chrome(options=options)
        print("Exception caught. Starting Chrome again...")
    log_msg("Google Chrome Launched!")

    driver.implicitly_wait(3)
    driver.get(NaukriURL)
    return driver


def naukriLogin(headless = False):
    """ Open Chrome browser and Login to Naukri.com"""
    status = False
    driver = None

    try:
        driver = LoadNaukri(headless)

        if "naukri" in driver.title.lower():
            log_msg("Website Loaded Successfully.")

        if not is_element_present(driver, By.ID, "Sug_autoComplete"):
            print("Attempting to log in.")
            loginTab = None
            if is_element_present(driver, By.ID, "toggleRCBLoginForm"):
                loginTab = driver.find_element(By.ID, "toggleRCBLoginForm")
                loginTab = loginTab.find_element(By.ID, "toggleForm")
                loginTab = loginTab.find_elements_by_xpath(".//*")[1]
            if loginTab is not None:
                loginTab.click()

            if is_element_present(driver, By.ID, "loginEmail"):
                print("Login1")
                emailFieldElement = GetElement(driver, "loginEmail", locator="ID")
                time.sleep(1)
                passFieldElement = GetElement(driver, "password", locator="ID")
                time.sleep(1)
                loginXpath = '//*[@type="submit"]'
                loginButton = driver.find_element(By.XPATH, loginXpath)
            elif is_element_present(driver, By.NAME, "userName"):
                print("Login2")
                emailFieldElement = GetElement(driver, "userName", locator="NAME")
                time.sleep(1)
                passFieldElement = GetElement(driver, "password", locator="NAME")
                time.sleep(1)
                loginXpath = '//*[@type="submit"]'
                loginButton = driver.find_element(By.XPATH, loginXpath)
            else:
                log_msg("No elements found to login.")

            if emailFieldElement is not None:
                emailFieldElement.clear()
                emailFieldElement.send_keys('ashwini.bogadi@worldemp.com')
                time.sleep(1)
                passFieldElement.clear()
                passFieldElement.send_keys('Miss@1234')
                time.sleep(1)
                loginButton.send_keys(Keys.ENTER)
                time.sleep(35)

        # CheckPoint to verify login
        if WaitTillElementPresent(driver, "Sug_autoComplete", locator="ID", timeout=40):
            CheckPoint = GetElement(driver, "Sug_autoComplete", locator="ID")
            if CheckPoint:
                log_msg("Naukri Login Successful")
                status = True
                return (status, driver)
            else:
                log_msg("Unknown Login Error")
                return (status, driver)
        else:
            log_msg("Unknown Login Error")
            return (status, driver)

    except Exception as e:
        catch(e)
    return (status, driver)


def UpdateProfile(driver):
    try:
        mobXpath = "//*[@name='mobile'] | //*[@id='mob_number']"
        profeditXpath = "//a[contains(text(), 'UPDATE PROFILE')] | //a[contains(text(), ' Snapshot')] | //a[contains(@href, 'profile') and contains(@href, 'home')]"
        saveXpath = "//button[@ type='submit'][@value='Save Changes'] | //*[@id='saveBasicDetailsBtn']"
        editXpath = "//em[text()='Edit']"

        WaitTillElementPresent(driver, profeditXpath, "XPATH", 20)
        profElement = GetElement(driver, profeditXpath, locator="XPATH")
        profElement.click()
        driver.implicitly_wait(2)

        WaitTillElementPresent(driver, editXpath + " | " + saveXpath, "XPATH", 20)
        if is_element_present(driver, By.XPATH, editXpath):
            editElement = GetElement(driver, editXpath, locator="XPATH")
            editElement.click()

            WaitTillElementPresent(driver, mobXpath, "XPATH", 20)
            mobFieldElement = GetElement(driver, mobXpath, locator="XPATH")
            mobFieldElement.clear()
            mobFieldElement.send_keys(mob)
            driver.implicitly_wait(2)

            saveFieldElement = GetElement(driver, saveXpath, locator="XPATH")
            saveFieldElement.send_keys(Keys.ENTER)
            driver.implicitly_wait(3)

            WaitTillElementPresent(driver, "//*[text()='today']", "XPATH", 10)
            if is_element_present(driver, By.XPATH, "//*[text()='today']"):
                log_msg("Profile Update Successful")
            else:
                log_msg("Profile Update Failed")

        elif is_element_present(driver, By.XPATH, saveXpath):
            mobFieldElement = GetElement(driver, mobXpath, locator="XPATH")
            mobFieldElement.clear()
            mobFieldElement.send_keys(mob)
            driver.implicitly_wait(2)

            saveFieldElement = GetElement(driver, saveXpath, locator="XPATH")
            saveFieldElement.send_keys(Keys.ENTER)
            driver.implicitly_wait(3)

            WaitTillElementPresent(driver, "confirmMessage", locator="ID", timeout=10)
            if is_element_present(driver, By.ID, "confirmMessage"):
                log_msg("Profile Update Successful")
            else:
                log_msg("Profile Update Failed")

        time.sleep(5)

    except Exception as e:
        catch(e)

def get_data(spark, driver, input):
    schema = StructType([
        StructField("domain",StringType(),True),
        StructField("technology",StringType(),True),
        StructField("res_count",LongType(),True),
        StructField("created_date",StringType(),True)
    ])

    complete_df = spark.createDataFrame([('','', None, '')], schema)

    search_box_name = 'ezString'
    search_btn_id = 'gnbSrchBtn'
    resumes_per_page_name = 'resumesPerPage'

    current_ts = str(datetime.now())

    for obj in input:
        domain = obj["domain"][0]
        for tech in obj["technology"]:
            sleep(round(uniform(0.00, 10.00), 2))
            if is_element_present(driver, By.NAME, search_box_name):
                search_box_element = GetElement(driver, search_box_name, locator="NAME")
                search_button_element = GetElement(driver, search_btn_id, locator="ID")
                util_policy_popup_class = 'btn btn-primary btnCtr lt_close'

                search_box_element.send_keys(tech)
                search_button_element.send_keys(Keys.ENTER)
                time.sleep(3)
                if is_element_present(driver, By.CLASS_NAME, util_policy_popup_class):
                    util_policy_popup_element = GetElement(driver, util_policy_popup_class, locator="CLASS_NAME")
                    util_policy_popup_element.send_keys(Keys.ENTER)

                if is_element_present(driver, By.NAME, resumes_per_page_name):
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    resumes_per_page = soup.find(id=resumes_per_page_name)
                    script = soup.find_all('script')
                    paragraphs = []
                    for x in script:
                        line = str(x)
                        pos = line.find('totalCount')
                        if pos > -1:
                            words = line.split(' ')
                            res_count = words[words.index("searchData['totalCount']") + 2].strip()[:-1]
                            part_df = spark.createDataFrame([(domain, tech, int(res_count), current_ts)], schema)
                            complete_df = complete_df.union(part_df)
                            break
                else:
                    log_msg("No results found.")
            else:
                log_msg("No search box found.")
    complete_df = complete_df.withColumn('created_ts', complete_df['created_date'].cast(TimestampType())).drop('created_date')
    return complete_df

def ingest_data(complete_df):
    complete_df = complete_df.where("technology != ''")
    complete_df.write.format("jdbc").mode("append") \
        .options(
        url='jdbc:postgresql://localhost:5432/postgres',
        dbtable='main.resume_count',
        user='postgres',
        password='admin',
        driver='org.postgresql.Driver') \
        .save()

def main():
    log_msg("-----Naukri.py Script Run Begin-----")

    spark = SparkSession \
        .builder \
        .appName("ingestion") \
        .config("spark.jars", "../Database/postgresql-42.3.1.jar") \
        .getOrCreate()

    with open("data.json", "r") as jsonfile:
        input = json.load(jsonfile)

    driver = None
    try:
        status, driver = naukriLogin()
        if status:
            complete_df = get_data(spark, driver, input["data"])
            complete_df.show()
            ingest_data(complete_df)

    except Exception as e:
        catch(e)

    finally:

        tearDown(driver)

    log_msg("-----Naukri.py Script Run Ended-----\n")


if __name__ == "__main__":
    main()





