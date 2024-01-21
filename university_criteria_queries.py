import socket
import re
import gevent
import gevent.monkey
gevent.monkey.patch_all()
from cassandra.io.geventreactor import GeventConnection
from cassandra.cluster import Cluster



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

def get_criteria_trend(school_names, criteria_name, year_range = None):
    schools_list = "(" + ", ".join(f"'{item}'" for item in school_names) + ")"

    query =  f"""SELECT
        university_name, year, {criteria_name}
    FROM {table_name}
    WHERE university_name in {schools_list} 
    {'' if year_range is None else f'and year>={year_range[0]} and year<= {year_range[1]}'}; """
    invoke_query(query)  


def get_school_trends(school_name, criteria_names,  year_range = None):
    criteria_list = ", ".join(f"{item}" for item in criteria_names)
    query = f"""
    SELECT
        university_name, year, {criteria_list}
    FROM {table_name}
    WHERE university_name = '{school_name}'
    {'' if year_range is None else f'and year>={year_range[0]} and year<= {year_range[1]}'}; 
    """
    invoke_query(query)  

def get_criterias_correlation(criteria_one, criteria_two, year_range = None):
    query = f"""
    SELECT university_name, year, {criteria_one}, {criteria_two}
    FROM {table_name} 
    """
    invoke_query(query)  

def invoke_query(query):
    try:
        result = session.execute(query)
        for row in result:
            print(row)
    except Exception as e:
        print(f"Error executing query: {e}")





#get_criteria_trend(['Tel Aviv University', 'Hebrew University of Jerusalem'], "times_teaching_score", (2012,2016))
#get_school_trends('Tel Aviv University', ["times_teaching_score","times_research_score"], (2012,2016))
#get_criterias_correlation('times_citations_influence_score', 'shanghai_hici_score',[2011, 2015])
