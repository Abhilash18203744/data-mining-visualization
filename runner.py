import os
import scrape_unemployment_data
import extract_education_data
import scrape_crime_data
import mongodb_upload_data
import postgresql_upload_data
import visualize_data
import yaml

# Dependencies file path
requirement_file = os.path.join(os.getcwd(), r'requirement.txt')

# Input.yaml file path
input_file = os.path.join(os.getcwd(), r'input.yaml')


def main():
    """
    Execute end to end project
    """
    # Install all denepedencies
    os.system("pip install -r {}".format(requirement_file))

    # fetch inputs from input.yaml file
    with open(input_file, 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile)

    if cfg['fetch_data_website']['web_scrape_unemployment']:
        print("Web scrape Unemployment data from website")
        # Web scrape Unemployment data
        scrape_unemployment_data.main()

    if cfg['fetch_data_website']['extract_education']:
        print("Extract Education data from website")
        # Extract Education data
        extract_education_data.main()

    if cfg['fetch_data_website']['extract_education']:
        print("Web scrape crime rate data from website")
        # Web scrape crime rate data
        scrape_crime_data.main()

    # Upload data into mongoDB
    mongodb_upload_data.main()

    # Upload data into postgresql database
    postgresql_upload_data.main()

    # Analyse and visualize data
    visualize_data.main()

if __name__ == "__main__":
    main()
