import pymongo
import os
import pandas as pd
from tabulate import tabulate
import psycopg2 as pg
import plotly.express as px
import yaml
import plotly.graph_objects as go

# Input.yaml file path
input_file = os.path.join(os.getcwd(), r'input.yaml')


def fetch_unemployment_data(postgresqlDB_details):
    """
    Get unemployment data from postgresql
    :return: unemployment_data: unemployment data from postgresql
    """
    get_unemployment_data_query = "SELECT x.year, state, avg(unemployment_rate) FROM (SELECT state_year.year, " \
                                  "state_year.state, unemp_rate.unemployment_rate FROM unemployment_rate as unemp_rate " \
                                  "JOIN state_year ON unemp_rate.state_year_id = state_year.state_year_id) " \
                                  "as X GROUP BY year, state"
    data = None
    try:
        conn = pg.connect(user=postgresqlDB_details['username'],
                          password=postgresqlDB_details['password'],
                          host=postgresqlDB_details['pg_host'],
                          port=postgresqlDB_details['pg_port'],
                          database=postgresqlDB_details['pg_db'])
        cursor = conn.cursor()
        cursor.execute(get_unemployment_data_query)
        data = cursor.fetchall()
        cursor.close()
        conn.close()
    except (Exception, pg.Error) as e:
        print(e)
    unemployment_data = pd.DataFrame(data, columns=['year', 'state', 'avg_unemployment_rate'])

    return unemployment_data


def fetch_education_data(postgresqlDB_details):
    """
    Get education data from postgresql
    :return: education_data: education data from postgresql
    """
    get_education_data_query = "SELECT state_year.year, state_year.state, edu_exp.total_revenue, " \
                                  "edu_exp.federal_revenue, edu_exp.state_revenue, edu_exp.local_revenue, " \
                                  "edu_exp.total_expenditure, edu_exp.instruction_expenditure, " \
                                  "edu_exp.support_services_expenditure, edu_exp.other_expenditure, " \
                                  "edu_exp.capital_outlay_expenditure FROM education_expenditure as edu_exp " \
                                  "JOIN state_year ON edu_exp.state_year_id = state_year.state_year_id"
    data = None
    try:
        conn = pg.connect(user=postgresqlDB_details['username'],
                          password=postgresqlDB_details['password'],
                          host=postgresqlDB_details['pg_host'],
                          port=postgresqlDB_details['pg_port'],
                          database=postgresqlDB_details['pg_db'])
        cursor = conn.cursor()
        cursor.execute(get_education_data_query)
        data = cursor.fetchall()
        cursor.close()
        conn.close()
    except (Exception, pg.Error) as e:
        print(e)
    education_data = pd.DataFrame(data, columns=['year', 'state', 'total_revenue', 'federal_revenue',
                                                    'state_revenue', 'local_revenue', 'total_expenditure',
                                                    'instruction_expenditure', 'support_services_expenditure',
                                                    'other_expenditure', 'capital_outlay_expenditure'])

    return education_data


def fetch_crime_data(postgresqlDB_details):
    """
    Get crime data from postgresql
    :return: crime_data: crime rate data from postgresql
    """
    get_crime_data_query = "SELECT state_year.year, state_year.state, crime_rate.population_coverage, " \
                                  "crime_rate.violent_crime_total, crime_rate.murder_and_nonnegligent_manslaughter, " \
                                  "crime_rate.legacy_rape1, crime_rate.robbery, crime_rate.aggravated_assault, " \
                                  "crime_rate.property_crime_total, crime_rate.burglary, " \
                                  "crime_rate.larceny_theft, crime_rate.motor_vehicle_theft FROM crime_rate " \
                                  "JOIN state_year ON crime_rate.state_year_id = state_year.state_year_id"
    data = None
    try:
        conn = pg.connect(user=postgresqlDB_details['username'],
                          password=postgresqlDB_details['password'],
                          host=postgresqlDB_details['pg_host'],
                          port=postgresqlDB_details['pg_port'],
                          database=postgresqlDB_details['pg_db'])
        cursor = conn.cursor()
        cursor.execute(get_crime_data_query)
        data = cursor.fetchall()
        cursor.close()
        conn.close()
    except (Exception, pg.Error) as e:
        print(e)
    crime_data = pd.DataFrame(data, columns=['year', 'state', 'Population_Coverage', 'Violent_crime_total',
                                                 'Murder_and_nonnegligent_manslaughter', 'Legacy_rape1', 'Robbery',
                                                 'Aggravated_assault', 'Property_crime_total', 'Burglary',
                                                 'Larceny_theft', 'Motor_vehicle_theft'])

    return crime_data

def visualize_unemployment_rate(unemployment_data):
    """
    Visualize and plot graphs for unemployment rate data
    :param unemployment_data: unemployment data from postgresql
    """
    # Visualising change in Unemployment rate for every state in USA for selected period
    unemployment_data = unemployment_data.sort_values(by=['year'])
    fig = px.line(unemployment_data, x='year', y='avg_unemployment_rate', color='state')
    fig.show()


def visualize_education_data(edu_df):
    """
    Generate visualization plots for education data
    """

    # Plot graph total revenue vs year
    fig1 = px.bar(edu_df, x='year', y='total_revenue',
                  hover_data=['state'], color='state',
                  labels={'total_revenue': 'TOTAL REVENUE RECEIVED'}, height=400)

    # Plot graph total expenditure vs year
    fig2 = px.bar(edu_df, x='year', y='total_expenditure',
                  hover_data=['state'], color='state',
                  labels={'total_expenditure': 'TOTAL EXPENDITURE'}, height=400)
    fig1.show()
    fig2.show()


def visualize_crime_data(crime_data):
    """
    Generate visualization plots for crime data
    """

    # data_alabama = state_data.gapminder().query("State == 'Alabama'")
    Alabama_data = crime_data[crime_data['state'] == 'Alabama']
    fig = px.bar(Alabama_data, x='year', y='Robbery',
                 hover_data=['year'])
    fig.show()


def visualize_combined_result(unemployment_data, education_data, crime_data):
    """
    Generate visualization plots
    """

    # Calculate total of education expenditure in USA per year
    edu_total_data = pd.DataFrame(education_data, columns=['year', 'total_expenditure'])
    edu_total_data = edu_total_data.groupby(['year']).sum()

    # Calculate total crime in USA per year
    crime_data['Total_crime'] = crime_data['Motor_vehicle_theft'] + crime_data['Larceny_theft'] + crime_data['Burglary'] + \
                          crime_data['Property_crime_total'] + crime_data['Aggravated_assault'] + crime_data['Robbery']\
                          + crime_data['Legacy_rape1'] + crime_data['Murder_and_nonnegligent_manslaughter'] + \
                          crime_data['Violent_crime_total']
    crime_total_data = crime_data.groupby(['year']).sum()


    # Calculate average unemployment rate in USA per year
    unemp_total_data = unemployment_data.groupby(['year']).sum()

    # Visualize crime, unemployment and education expenditure pattern in USA per year
    draw_combined_graph(unemp_total_data, edu_total_data, crime_total_data)


def draw_combined_graph(unemp_total_data, edu_total_data, crime_total_data):
    """
    Draw graphs including unemployment, crime and education expenditure data
    """
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=unemp_total_data.index,
        y=unemp_total_data['avg_unemployment_rate'],
        name="Unemployment data"
    ))

    fig.add_trace(go.Scatter(
        x=edu_total_data.index,
        y=edu_total_data['total_expenditure'],
        name="Total expenditure data",
        yaxis="y2"
    ))

    fig.add_trace(go.Scatter(
        x=crime_total_data.index,
        y=crime_total_data['Total_crime'],
        name="Total crime data",
        yaxis="y3"
    ))

    # Create axis objects
    fig.update_layout(
        xaxis=dict(
            domain=[0.3, 0.7]
        ),
        yaxis=dict(
            title="Unemployment data",
            titlefont=dict(
                color="#1f77b4"
            ),
            tickfont=dict(
                color="#1f77b4"
            )
        ),
        yaxis2=dict(
            title="Total expenditure data",
            titlefont=dict(
                color="#ff7f0e"
            ),
            tickfont=dict(
                color="#ff7f0e"
            ),
            anchor="free",
            overlaying="y",
            side="left",
            position=0.15
        ),
        yaxis3=dict(
            title="Total crime data",
            titlefont=dict(
                color="#d62728"
            ),
            tickfont=dict(
                color="#d62728"
            ),
            anchor="x",
            overlaying="y",
            side="right"
        )
    )
    # Update layout properties
    fig.update_layout(
        title_text="multiple y-axes example",
        width=1800,
    )

    fig.show()


def main():
    """
    Fetch data from postgresql db and visualize
    """
    # fetch inputs from input.yaml file
    with open(input_file, 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile)

    # db name and collection name for all datasets in mongoDB
    postgresqlDB_details = cfg['postgresqlDB_details']

    # Get unemployment rate data and visualize
    unemployment_data = fetch_unemployment_data(postgresqlDB_details)
    visualize_unemployment_rate(unemployment_data)

    # Get education data and visualize
    education_data = fetch_education_data(postgresqlDB_details)
    visualize_education_data(education_data)

    # Get education data and visualize
    crime_data = fetch_crime_data(postgresqlDB_details)
    visualize_crime_data(crime_data)

    # Visualize final result on relation between all three data
    visualize_combined_result(unemployment_data, education_data, crime_data)



if __name__ == "__main__":
    main()