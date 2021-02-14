import pandas as pd
import csv
import urllib.request as req
import json
import os, shutil
import yaml

# Input.yaml file path
input_file = os.path.join(os.getcwd(), r'input.yaml')

# json result file path for education data
jsonFilePath = os.path.join(os.getcwd(), r'result\Education.json')

# create directory for saving education data files
edu_files_path = os.path.join(os.getcwd(), r'edu_files')
if not os.path.exists(edu_files_path):
    os.makedirs(edu_files_path)


def extract_edu_data(start_year, end_year):
    """
    Etract education dataand create JSON file
    """
    data = []
    output = {}
    # Download data files and convert into csv for encoding formatting
    for i in range(start_year, end_year + 1):
        i = str(i)
        j = i[2:4]
        list = []
        dls = "https://www2.census.gov/programs-surveys/school-finances/tables/{}/secondary-education-finance/elsec{}.xls"\
            .format(i, j)
        resp = req.urlretrieve(dls, os.path.join(os.getcwd(), r'edu_files\test_{}.xls'.format(i)))
        output_xls = pd.read_excel(os.path.join(os.getcwd(), r'edu_files\test_{}.xls'.format(i)))
        output_xls = output_xls.groupby(['STATE']).sum()
        output_csv = output_xls.to_csv(os.path.join(os.getcwd(), r'edu_files\test_{}.csv'.format(i)))
        csvFilePath = os.path.join(os.getcwd(), r'edu_files\test_{}.csv'.format(i))

        with open(csvFilePath) as csvFile:
            csvReader = csv.DictReader(csvFile)
            for Row in csvReader:
                output["STATE"] = Row["STATE"]
                output["YEAR"] = i
                output["TOTALREV"] = Row["TOTALREV"]
                output["TFEDREV"] = Row["TFEDREV"]
                output["TSTREV"] = Row["TSTREV"]
                output["TLOCREV"] = Row["TLOCREV"]
                output["TOTALEXP"] = Row["TOTALEXP"]
                output["TCURINST"] = Row["TCURINST"]
                output["TCURSSVC"] = Row["TCURSSVC"]
                output["TCUROTH"] = Row["TCUROTH"]
                output["TCAPOUT"] = Row["TCAPOUT"]
                list.append(output.copy())
        data.extend(list)

    # Create JSON file from extracted data
    with open(jsonFilePath, "w") as jsonFile:
        json.dump(data, jsonFile, indent=4, sort_keys=True)

def delete_edu_files():
    """
    Delete all the xls and csv files from the directory
    """
    for filename in os.listdir(edu_files_path):
        file_path = os.path.join(edu_files_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))


def main():
    """
    Etract education data
    """
    # fetch inputs from input.yaml file
    with open(input_file, 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile)

    start_year = cfg['data_period']['start_year']
    end_year = cfg['data_period']['end_year']
    
    # Extract education data by downloading files and consolidate data into single JSON
    extract_edu_data(start_year, end_year)
    
    # Delete all education data files
    delete_edu_files()
    
    
if __name__ == "__main__":
    main()