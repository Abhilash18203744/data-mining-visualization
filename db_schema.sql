DROP TABLE IF EXISTS unemployment_rate;
DROP TABLE IF EXISTS crime_rate;
DROP TABLE IF EXISTS education_expenditure;
DROP TABLE IF EXISTS state_year;
CREATE TABLE state_year(state_year_id VARCHAR (50) PRIMARY KEY,
                           year VARCHAR (50) NOT NULL,
                           state VARCHAR (50) NOT NULL
                           );
CREATE TABLE unemployment_rate(state_year_id VARCHAR (50) NOT NULL REFERENCES state_year (state_year_id),
                                   month VARCHAR (50) NOT NULL,
                                   unemployment_rate FLOAT NOT NULL,
                                   PRIMARY KEY (state_year_id, month)
                                   );
CREATE TABLE crime_rate(state_year_id VARCHAR (50) NOT NULL REFERENCES state_year (state_year_id),
                           population_coverage FLOAT,
                           violent_crime_total FLOAT,
                           murder_and_nonnegligent_manslaughter FLOAT,
                           legacy_rape1 FLOAT,
                           robbery FLOAT,
                           aggravated_assault FLOAT,
                           property_crime_total FLOAT,
                           burglary FLOAT,
                           larceny_theft FLOAT,
                           motor_vehicle_theft FLOAT,
                           PRIMARY KEY (state_year_id)
                           );
CREATE TABLE education_expenditure(state_year_id VARCHAR (50) NOT NULL REFERENCES state_year (state_year_id),
                                       total_revenue FLOAT,
                                       federal_revenue FLOAT,
                                       state_revenue FLOAT,
                                       local_revenue FLOAT,
                                       total_expenditure FLOAT,
                                       instruction_expenditure FLOAT,
                                       support_services_expenditure FLOAT,
                                       other_expenditure FLOAT,
                                       capital_outlay_expenditure FLOAT,
                                       PRIMARY KEY (state_year_id)
                                       );