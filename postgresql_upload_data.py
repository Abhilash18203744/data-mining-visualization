import pymongo
import os
import pandas as pd
from tabulate import tabulate
import psycopg2 as pg
import yaml

# Input.yaml file path
input_file = os.path.join(os.getcwd(), r'input.yaml')

# schema creation sql file path
create_schema_file = os.path.join(os.getcwd(), r'db_schema.sql')


def fetch_unemployment_data(mongoDB_details):
    """
    Fetch unemployment data from mongodb Clean and process
    :return data: unemployment rate dataframe after cleanning and processing
    """

    # Create connection with the mongodb
    myclient = pymongo.MongoClient(mongoDB_details['link'])
    usadb = myclient[mongoDB_details['db_name']]
    scores = []
    for score in list(usadb[mongoDB_details['unemp_collection_name']].find({}, {"_id":0})):
        scores.append(score)
    data = pd.DataFrame(scores, index=None)

    # Type casting on states
    data['rate'] = data['rate'].apply(pd.to_numeric, errors='coerce')

    # Cleaning data (drop null value records)
    data.dropna()

    # Merge year and state columns to get unique
    data['state_year'] = data['state'] + data['year']

    return data


def fetch_crime_data(mongoDB_details):
    """
    Fetch crime data from mongodb Clean and process
    :return data: Crime rate dataframe after cleanning and processing
    """
    # Create connection with the mongodb
    myclient = pymongo.MongoClient(mongoDB_details['link'])
    usaCrimeDb = myclient[mongoDB_details['db_name']]
    crimes = []

    # keys
    keys = ['State', 'Year', 'Population_Coverage', 'Violent_crime_total', 'Murder_and_nonnegligent_manslaughter',
            'Legacy_rape1', 'Robbery', 'Aggravated_assault', 'Property_crime_total', 'Burglary', 'Larceny-theft',
            'Motor_vehicle_theft']

    # Row wise data
    for crime in list(usaCrimeDb[mongoDB_details['crime_collection_name']].find({}, {"_id":0})):
        crimes.append(crime)
    crime_data = pd.DataFrame(data=crimes, columns=keys)

    # Cleaning data (drop null value records)
    crime_data.dropna()

    # Type casting string to float
    for key in keys[2:12]:
        crime_data[key] = crime_data[key].str.replace(',', '').astype(float)

    # Merge year and state columns to get unique
    crime_data['state_year'] = crime_data['State'].str.strip() + crime_data['Year'].str.strip()

    return crime_data


def fetch_education_data(mongoDB_details):
    """
    Fetch education data from mongodb Clean and process
    :return edu_df: education rate dataframe after cleanning and processing
    """
    fetch_list = []
    # Names of columns in the data
    financial_list = ['TOTALREV', 'TFEDREV', 'TSTREV', 'TLOCREV', 'TOTALEXP', 'TCURINST', 'TCURSSVC', 'TCUROTH',
                      'TCAPOUT']

    # Create connection with the mongodb
    myclient = pymongo.MongoClient(mongoDB_details['link'])
    usadb = myclient[mongoDB_details['db_name']]

    for item in (usadb[mongoDB_details['edu_collection_name']].find({}, {"_id": 0})):
        fetch_list.append(item)

    edu_df = pd.DataFrame(fetch_list, index=None)
    edu_df['STATE'] = edu_df['STATE'].map(
        {'1': 'Alabama', '2': 'Alaska', '3': 'Arizona', '4': 'Arkansas', '5': 'California', '6': 'Colorado',
         '7': 'Connecticut', '8': 'Delaware', '9': 'District of Columbia', '10': 'Florida', '11': 'Georgia',
         '12': 'Hawaii', '13': 'Idaho', '14': 'Illinois', '15': 'Indiana', '16': 'Iowa', '17': 'Kansas',
         '18': 'Kentucky', '19': 'Louisiana', '20': 'Maine', '21': 'Maryland', '22': 'Massachusetts',
         '23': 'Michigan', '24': 'Minnesota', '25': 'Mississippi', '26': 'Missouri', '27': 'Montana',
         '28': 'Nebraska', '29': 'Nevada', '30': 'New Hampshire', '31': 'New Jersey', '32': 'New Mexico',
         '33': 'New York', '34': 'North Carolina', '35': 'North Dakota', '36': 'Ohio', '37': 'Oklahoma',
         '38': 'Oregon', '39': 'Pennsylvania', '40': 'Rhode Island', '41': 'South Carolina', '42': 'South Dakota',
         '43': 'Tennessee', '44': 'Texas', '45': 'Utah', '46': 'Vermont', '47': 'Virginia',
         '48': 'Washington', '49': 'West Virginia', '50': 'Wisconsin', '51': 'Wyoming'})

    # Type casting, convert string type into numeric
    for item in financial_list:
        edu_df[item] = edu_df[item].apply(pd.to_numeric, errors='coerce')

    # Cleaning data (drop null value records)
    edu_df.dropna()

    # Merge year and state columns to get unique
    edu_df['state_year'] = edu_df['STATE'] + edu_df['YEAR']

    return edu_df


def upload_state_year_data_postgres(data, postgresqlDB_details):
    """
    Upload data into postgres
    :param data: unemployment rate dataframe
    """
    subset = data[['state_year', 'state', 'year']]
    subset.drop_duplicates(subset ="state_year", keep = 'last', inplace = True)
    tuples = [tuple(x) for x in subset.to_numpy()]
    try:
        conn = pg.connect(user=postgresqlDB_details['username'],
                          password=postgresqlDB_details['password'],
                          host=postgresqlDB_details['pg_host'],
                          port=postgresqlDB_details['pg_port'],
                          database=postgresqlDB_details['pg_db'])
        cursor = conn.cursor()
        args_str = b",".join(cursor.mogrify("(%s,%s,%s)", x) for x in tuples)
        cursor.execute("INSERT INTO state_year(state_year_id, state, year) VALUES " + args_str.decode("utf-8"))
        conn.commit()
        cursor.close()
        conn.close()
    except (Exception, pg.Error) as e:
        print(e)

def upload_crime_data_postgres(data, postgresqlDB_details):
    """
    Upload crime data into postgres
    :param data: crime rate dataframe
    """
    subset = data[['state_year', 'Population_Coverage', 'Violent_crime_total', 'Murder_and_nonnegligent_manslaughter',
            'Legacy_rape1', 'Robbery', 'Aggravated_assault', 'Property_crime_total', 'Burglary', 'Larceny-theft',
            'Motor_vehicle_theft']]
    tuples = [tuple(x) for x in subset.to_numpy()]
    try:
        conn = pg.connect(user=postgresqlDB_details['username'],
                          password=postgresqlDB_details['password'],
                          host=postgresqlDB_details['pg_host'],
                          port=postgresqlDB_details['pg_port'],
                          database=postgresqlDB_details['pg_db'])
        cursor = conn.cursor()
        args_str = b",".join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x) for x in tuples)
        cursor.execute("INSERT INTO crime_rate(state_year_id, population_coverage, violent_crime_total, "
                       "murder_and_nonnegligent_manslaughter, legacy_rape1, robbery, aggravated_assault, "
                       "property_crime_total, burglary, larceny_theft, motor_vehicle_theft) VALUES " +
                       args_str.decode("utf-8"))
        conn.commit()
        cursor.close()
        conn.close()
    except (Exception, pg.Error) as e:
        print(e)


def upload_unemployment_data_postgres(data, postgresqlDB_details):
    """
    Upload unemployment data into postgres
    :param data: unemployment rate dataframe
    """
    subset = data[['state_year', 'month', 'rate']]
    tuples = [tuple(x) for x in subset.to_numpy()]
    try:
        conn = pg.connect(user=postgresqlDB_details['username'],
                          password=postgresqlDB_details['password'],
                          host=postgresqlDB_details['pg_host'],
                          port=postgresqlDB_details['pg_port'],
                          database=postgresqlDB_details['pg_db'])
        cursor = conn.cursor()
        args_str = b",".join(cursor.mogrify("(%s,%s,%s)", x) for x in tuples)
        cursor.execute("INSERT INTO unemployment_rate(state_year_id, month, unemployment_rate) VALUES " +
                       args_str.decode("utf-8"))
        conn.commit()
        cursor.close()
        conn.close()
    except (Exception, pg.Error) as e:
        print(e)


def upload_education_data_postgres(data, postgresqlDB_details):
    """
    Upload education data into postgres
    :param data: education expenditure and revenue dataframe
    """
    subset = data[['state_year', 'TOTALREV', 'TFEDREV', 'TSTREV', 'TLOCREV', 'TOTALEXP', 'TCURINST', 'TCURSSVC',
                   'TCUROTH', 'TCAPOUT']]
    subset.drop_duplicates(subset="state_year", keep='last', inplace=True)
    tuples = [tuple(x) for x in subset.to_numpy()]
    try:
        conn = pg.connect(user=postgresqlDB_details['username'],
                          password=postgresqlDB_details['password'],
                          host=postgresqlDB_details['pg_host'],
                          port=postgresqlDB_details['pg_port'],
                          database=postgresqlDB_details['pg_db'])
        cursor = conn.cursor()
        args_str = b",".join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x) for x in tuples)
        cursor.execute(
            "INSERT INTO education_expenditure(state_year_id, total_revenue, federal_revenue, state_revenue, "
            "local_revenue, total_expenditure, instruction_expenditure, support_services_expenditure, "
            "other_expenditure, capital_outlay_expenditure) VALUES " + args_str.decode("utf-8"))
        conn.commit()
        cursor.close()
        conn.close()
    except (Exception, pg.Error) as e:
        print(e)


def create_schema(postgresqlDB_details):
    """
    Create postgres schema
    """
    try:
        conn = pg.connect(user=postgresqlDB_details['username'],
                          password=postgresqlDB_details['password'],
                          host=postgresqlDB_details['pg_host'],
                          port=postgresqlDB_details['pg_port'],
                          database=postgresqlDB_details['pg_db'])
        cursor = conn.cursor()
        file = open(create_schema_file, 'r')
        sql_file = s = " ".join(file.readlines())
        cursor.execute(sql_file)
        conn.commit()
        cursor.close()
        conn.close()
    except (Exception, pg.Error) as e:
        print(e)



def main():
    """
    Import data into postgresql db
    """
    # fetch inputs from input.yaml file
    with open(input_file, 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile)

    # db name and collection name for all datasets in mongoDB
    mongoDB_details = cfg['mongoDB_details']
    postgresqlDB_details = cfg['postgresqlDB_details']

    # create postgresql database schema
    create_schema(postgresqlDB_details)

    # get unemployment data from mongo db and import into postgresql db
    data = fetch_unemployment_data(mongoDB_details)
    upload_state_year_data_postgres(data, postgresqlDB_details)
    upload_unemployment_data_postgres(data, postgresqlDB_details)

    # get education data from mongo db and import into postgresql db
    edu_data = fetch_education_data(mongoDB_details)
    upload_education_data_postgres(edu_data, postgresqlDB_details)

    # get crime rate data from mongo db and import into postgresql db
    crime_data = fetch_crime_data(mongoDB_details)
    upload_crime_data_postgres(crime_data, postgresqlDB_details)



if __name__ == "__main__":
    main()
