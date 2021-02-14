from selenium import webdriver
from selenium.webdriver.support.ui import Select
import json
import os
import yaml
import re

# Input.yaml file path
input_file = os.path.join(os.getcwd(), r'input.yaml')

# fetch inputs from input.yaml file
with open(input_file, 'r') as ymlfile:
    cfg = yaml.safe_load(ymlfile)

url = cfg['dataset_links']['crime_data_link']
start_year = cfg['data_period']['start_year']
end_year = cfg['data_period']['end_year']

# Selenium driver details
selenium_driver_path = os.path.join(os.getcwd(), r'chromedriver_win32\chromedriver.exe')

# Path of result json file
json_file_path = os.path.join(os.getcwd(), r'result\CrimeDatabyState.json')

def fillDriverDetails(driver):
    """
    Method to fill details in the selenium driver
    """
    driver.get(url)


def fillOptions(year, driver):
    """
    Selecting the options in the required tables in url
    """
    # time.sleep(1)
    # Selecting the States variables from the dropdown
    if (year == start_year):
        elementStates = driver.find_element_by_xpath('//*[@id="states"]')
        drpStates = Select(elementStates)
        for i in range(1, 52):  # state (1 - 52)
            drpStates.select_by_index(i)

        ### Selecting the Crime variable from dropdown
        elementCrime = driver.find_element_by_xpath('//*[@id="groups"]')
        drpCrime = Select(elementCrime)
        for i in range(0, 4):
            drpCrime.select_by_index(i)
        # print(drpCrime.first_selected_option.text)

    # Selecting the year variable from dropdown
    elementYear = driver.find_element_by_xpath('// *[ @ id = "year"]')
    drpYear = Select(elementYear)
    drpYear.select_by_visible_text(str(year))

    # Submitting the selections to generate data
    # time.sleep(1)
    submitBtn = driver.find_element_by_xpath('//*[@id="CFForm_1"]/table/tbody/tr[3]/td[1]/p/input[1]')
    submitBtn.submit()


def scrapeTable(year, driver):
    """
    Method to scrape the table details and filling in the dictionary
    """
    keys = ['Population_Coverage', 'Violent_crime_total', 'Murder_and_nonnegligent_manslaughter',
            'Legacy_rape1', 'Robbery', 'Aggravated_assault', 'Property_crime_total', 'Burglary', 'Larceny-theft',
            'Motor_vehicle_theft', 'Violent_Crime_rate', 'Murder_and_nonnegligent_manslaughter_rate',
            'Legacy_rape_rate1', 'Robbery_rate', 'Aggravated_assault_rate', 'Property_crime_rate', 'Burglary_rate',
            'Larceny-theft_rate', 'Motor_vehicle_theft_rate']
    dict = {}
    data = []

    for i in range(5, 55):  # 51 states (5-55)
        xpath = '/html/body/div[2]/table/tbody/tr[{}]'.format(i)
        values = str(driver.find_element_by_xpath(xpath).text)
        r = re.compile("([a-zA-Z ]+)([0-9,. ]+)")
        match = r.match(values)
        dict['State'] = match.group(1)
        list = match.group(2).strip().split()
        for j in range(len(keys)):
            dict[keys[j]] = list[j]
        dict['Year'] = str(year)
        data.append(dict.copy())
    return data


def scrapCrimeDataByYear(driver):
    """
    Method that will call all methods in order to scrap crime data from provided url and generating json out of teh data
    """
    final_data = []
    for year in range(start_year, end_year + 1):  # 2017
        # Calling method to fill Selenium driver details
        fillDriverDetails(driver)

        # Calling method to fill details in website
        fillOptions(year, driver)
        # time.sleep(1)
        # Calling method to scrape data from the generated result after fillOptions() method
        data = scrapeTable(year, driver)
        final_data.extend(data.copy())

    # Stopping the selenium browser
    driver.quit()

    # Store the scrapped data into json file
    with open(json_file_path, 'w') as json_file:
        json.dump(final_data, json_file, indent=4, sort_keys=True)


def main():
    """
    Start web scrapping data from the given website
    """
    driver = webdriver.Chrome(executable_path=selenium_driver_path)
    # fetch data
    scrapCrimeDataByYear(driver)


if __name__ == "__main__":
    main()