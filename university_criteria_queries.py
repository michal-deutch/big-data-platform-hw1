import socket
import re
import gevent
import gevent.monkey

gevent.monkey.patch_all()
from cassandra.io.geventreactor import GeventConnection
from cassandra.cluster import Cluster
import pandas as pd
from University_World_Rankings_queries import invoke_query

keyspace = "university_rankings"
contact_points = ["127.0.0.1"]
cluster = Cluster(contact_points=contact_points)
cluster.connection_class = GeventConnection
session = cluster.connect(keyspace)
table_name = "University_Criteria"


## PRIMARY KEY(year, country, university_name)


##########################################################################################
##########################University_Criteria-table-queries##############################
##########################################################################################

def get_criteria_trend(school_names, criteria_name, year_range=None):
    schools_list = "(" + ", ".join(f"'{item}'" for item in school_names) + ")"

    query = f"""SELECT
        university_name, year, {criteria_name}
    FROM {table_name}
    WHERE university_name in {schools_list}
    {'' if year_range is None else f'and year>={year_range[0]} and year<= {year_range[1]}'}; """

    return invoke_query(query)


def get_school_trends(school_name, criteria_names, year_range=None):
    criteria_list = ", ".join(f"{item}" for item in criteria_names)
    query = f"""
    SELECT university_name, year, {criteria_list}
    FROM {table_name}
    WHERE university_name = '{school_name}'
    {'' if year_range is None else f'and year>={year_range[0]} and year<= {year_range[1]}'}; 
    """
    return invoke_query(query)

#note tu use with None specified for to default argument
def get_criterias_by_year(criteria_one, criteria_two=None , year_range=None):

    if criteria_two is None:
        query = f"""
        SELECT year,university_name, {criteria_one}
        FROM {table_name}
        {'' if year_range is None else f'WHERE year>={year_range[0]} and year<= {year_range[1]} ALLOW FILTERING'}
        """

    else:
        query = f"""
        SELECT year,university_name, {criteria_one}, {criteria_two}
        FROM {table_name}
        {'' if year_range is None else f'WHERE year>={year_range[0]} and year<= {year_range[1]} ALLOW FILTERING'} 
        """

    return invoke_query(query)


def get_all_possible_Institutes():

    query = f"""
    SELECT DISTINCT university_name
    FROM {table_name}
    """
    return invoke_query(query)


# print(get_criteria_trend(["Tel Aviv University","Bar-Ilan University"], "times_teaching_score", (2012,2016)))
# print(get_school_trends('Tel Aviv University', ["times_teaching_score","times_research_score"], (2012,2016)))
# print(get_criterias_by_year('times_citations_influence_score',None,(2012,2012)))
