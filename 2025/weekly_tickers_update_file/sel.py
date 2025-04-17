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
