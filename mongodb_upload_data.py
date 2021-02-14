import pymongo
import os
import json
import yaml

# Input.yaml file path
input_file = os.path.join(os.getcwd(), r'input.yaml')

# Result file path of USA unemployment dataset
unemployment_result = os.path.join(os.getcwd(), r'result\unemployment-data.json')

# Result file path of USA education dataset
education_result = os.path.join(os.getcwd(), r'result\Education.json')

# Result file path of education dataset
crime_result = os.path.join(os.getcwd(), r'result\CrimeDatabyState.json')


def insert_unemp_data(usadb, unemp_collection_name):
    """
    Create collection for unemployment data and insert data
    """
    unempcol = usadb[unemp_collection_name]

    # Delete if collection has already data
    if unemp_collection_name in usadb.list_collection_names():
        unempcol.drop()
        print("Dropped collection {} from database".format(unemp_collection_name))
        unempcol = usadb[unemp_collection_name]

    with open(unemployment_result, encoding='utf-8') as data_file:
        data = json.loads(data_file.read())

    for record in data:
        result = unempcol.insert_one(record)
        print('Inserted USA unemployment data...')
        print('Inserted post id %s ' % result.inserted_id)


def insert_education_data(usadb, edu_collection_name):
    """
    Create collection for education data and insert data
    """
    edu_coll = usadb[edu_collection_name]

    # Delete if collection has already data
    if edu_collection_name in usadb.list_collection_names():
        edu_coll.drop()
        print("Dropped collection {} from database".format(edu_collection_name))
        educoll = usadb[edu_collection_name]

    with open(education_result, 'r', encoding='utf-8') as data_file:
        data = (json.loads(data_file.read()))

    for record in data:
        result = edu_coll.insert_one(record)
        print('Inserted USA educational data...')
        print('Inserted post id %s ' % result.inserted_id)


def insert_crime_data(usadb, crime_collection_name):
    """
    Create database, collection for crime dataset and insert data
    """
    crimecol = usadb[crime_collection_name]

    # Delete if collection has already data
    if crime_collection_name in usadb.list_collection_names():
        crimecol.drop()
        print("Dropped collection {} from database".format(crime_collection_name))
        crimecol = usadb[crime_collection_name]

    with open(crime_result, encoding='utf-8') as data_file:
        data = json.loads(data_file.read())

    for record in data:
        result = crimecol.insert_one(record)
        print('Inserted USA Crime data...')
        print('Inserted post id %s ' % result.inserted_id)


def connect_create_db(mongo_link, db_name):
    """
    Create connection with the mongodb and create database, collection
    """
    myclient = pymongo.MongoClient(mongo_link)
    usadb = myclient[db_name]
    return usadb


def main():
    """
    Import json data into mongodb
    """
    # fetch inputs from input.yaml file
    with open(input_file, 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile)

    # db name and collection name for all datasets in mongoDB
    db_name = cfg['mongoDB_details']['db_name']
    unemp_collection_name = cfg['mongoDB_details']['unemp_collection_name']
    edu_collection_name = cfg['mongoDB_details']['edu_collection_name']
    crime_collection_name = cfg['mongoDB_details']['crime_collection_name']
    mongo_link = cfg['mongoDB_details']['link']

    # Create connection with mongoDB
    db = connect_create_db(mongo_link, db_name)

    # Insert unemployment data into the mongodb
    insert_unemp_data(db, unemp_collection_name)

    # Insert education data into the mongodb
    insert_education_data(db, edu_collection_name)

    # Insert crime rate data into the mongodb
    insert_crime_data(db, crime_collection_name)


if __name__ == "__main__":
    main()
