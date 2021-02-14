from selenium.webdriver.support.ui import Select
from selenium import webdriver
import json
import time
import os
import yaml

# Input.yaml file path
input_file = os.path.join(os.getcwd(), r'input.yaml')

# Set months for which data to be fetched
months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
          'November', 'December']

# Set path of the result json file and chrome driver(required for selenium)
result_json_file = os.path.join(os.getcwd(), r'result\unemployment-data.json')
chrome_driver_path = os.path.join(os.getcwd(), r'chromedriver_win32\chromedriver.exe')


def fill_options(driver, start_year, end_year):
    """
    Selecting the options in the required tables
    """

    # Selecting the year from drop down
    records = []
    for year in range(start_year, end_year + 1):
        time.sleep(1)
        # Selecting the month from drop down and iterating to scrap data
        for month in months:
            # Selecting year from drop down
            elementYear = driver.find_element_by_xpath('//*[@id="year"]')
            drpYear = Select(elementYear)
            drpYear.select_by_visible_text(str(year))

            # Selecting month from drop down
            elementMonth = driver.find_element_by_xpath('// *[ @ id = "period"]')
            drpMonth = Select(elementMonth)
            drpMonth.select_by_visible_text(month)

            # Submitting the selections to generate data
            submitBtn = driver.find_element_by_xpath('//*[@id="btn_sumbit"]')
            submitBtn.click()

            # Calling the scraper method to scrape data of a month
            unemployment_data_list = scrape_table(driver, month, str(year))
            records.extend(unemployment_data_list)

    # Write data into json file
    with open(result_json_file, 'w') as json_file:
        json.dump(records, json_file, indent=4, sort_keys=True)


def scrape_table(driver, month, year):
    """
    Scrapping data from the result of fill_options
    """
    # Add wait time to reload webpage
    time.sleep(2)
    
    record_list = []

    # Fetching data from the result table
    for i in range(1, 53):  # data available in 53 rows
        monthly_dict = {}
        xpath = '//*[@id="tb_data"]/tbody/tr[{}]'.format(i)
        values = str(driver.find_element_by_xpath(xpath).text)
        key_value = values.strip().split()
        if len(key_value) > 3:
            monthly_dict['state'] = '{} {} {}'.format(key_value[0], key_value[1], key_value[2])
            monthly_dict['rate'] = key_value[3]
        elif len(key_value) > 2:
            monthly_dict['state'] = '{} {}'.format(key_value[0], key_value[1])
            monthly_dict['rate'] = key_value[2]
        else:
            monthly_dict['state'] = '{}'.format(key_value[0])
            monthly_dict['rate'] = key_value[1]
        monthly_dict['month'] = month
        monthly_dict['year'] = year
        # Print row data
        print(monthly_dict)
        record_list.append(monthly_dict)
    return record_list


def main():
    """
    start web scrapping data from given website
    """

    # fetch inputs from input.yaml file
    with open(input_file, 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile)

    url = cfg['dataset_links']['unemlpoyment_data_link']
    start_year = cfg['data_period']['start_year']
    end_year = cfg['data_period']['end_year']

    # Get driver object for selenium
    driver = webdriver.Chrome(executable_path=chrome_driver_path)

    # Get unemployment data from given URL and write into result JSON file
    driver.get(url)
    fill_options(driver, start_year, end_year)

    # Stop selenium browser
    driver.quit()


if __name__ == "__main__":
    main()
