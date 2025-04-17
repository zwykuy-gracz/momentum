from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

option = webdriver.ChromeOptions()
option.add_argument("headless")

driverpath = Service("/usr/lib/chromium-browser/chromedriver")
driver = webdriver.Chrome(service=driverpath, options=option)

driver.get("https://www.nasdaq.com/market-activity/stocks/screener")
element = driver.find_element(By.CLASS_NAME, "h2.jupiter22-c-section-heading-title")
# element = driver.find_element(
#     By.XPATH,
#     "/html/body/div[2]/div/main/div[2]/article/div/div[1]/div[2]/div/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/button",
# )
driver.quit()


from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import time

option = webdriver.ChromeOptions()
option.add_argument("headless")

driverpath = Service("/usr/lib/chromium-browser/chromedriver")
driver = webdriver.Chrome(service=driverpath, options=option)

driver.get("https://www.nasdaq.com/market-activity/stocks/screener")

element = driver.find_element(By.CLASS_NAME, "jupiter22-market-status--card--date")
# element = driver.find_element(By.XPATH, "//h2[contains(text(), 'Stock Screener')]")
print(driver.current_url)
print(driver.title)
print("+++++++++")
# print(element.text)
driver.quit()
