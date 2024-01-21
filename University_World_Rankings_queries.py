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
table_name = "University_World_Rankings"
## PRIMARY KEY(year, country, university_name)



##########################################################################################
##########################University_World_Rankings-table-queries##############################
##########################################################################################

def get_number_of_ranked_school_by_country_and_year(countries, start_year, end_year):
    countries_list = "(" + ", ".join(f"'{item}'" for item in countries) + ")"

    query =  f"""SELECT country, year,
        Count(shanghai_rank) AS total_schools_ranked_in_shanghai,
        Count(cwur_rank) AS total_schools_ranked_in_cwur,
        Count(times_rank) AS total_schools_ranked_in_times,
        Count(university_name) AS total_ranked_universities
    FROM {table_name}
    WHERE country in {countries_list} and year>={start_year} and year<={end_year}
    GROUP BY country, year; """

    invoke_query(query)

def get_best_rank_of_ranked_school_by_country_and_year(countries, start_year, end_year):
    countries_list = "(" + ", ".join(f"'{item}'" for item in countries) + ")"

    query =  f"""SELECT country, year,
        Min(shanghai_rank) AS best_rank_in_shanghai,
        Min(cwur_rank) AS best_rank_in_cwur,
        Min(times_rank) AS best_rank_in_times
    FROM {table_name}
    WHERE country in {countries_list} and year>={start_year} and year<={end_year}
    GROUP BY country, year; """
    
    invoke_query(query)

def get_all_times_stats(countries):
    countries_list = "(" + ", ".join(f"'{item}'" for item in countries) + ")"

    query =  f"""SELECT country,
        Max(shanghai_rank) AS worst_rank_in_shanghai,
        Max(cwur_rank) AS worst_rank_in_cwur,
        Max(times_rank) AS worst_rank_in_times,
        Min(shanghai_rank) AS best_rank_in_shanghai,
        Min(cwur_rank) AS best_rank_in_cwur,
        Min(times_rank) AS best_rank_in_times,
        AVG(shanghai_rank) AS average_rank_in_shanghai,
        AVG(cwur_rank) AS average_rank_in_cwur,
        AVG(times_rank) AS average_rank_in_times
    FROM {table_name}
    WHERE country in {countries_list}
    GROUP BY country; """
    
    invoke_query(query)   

def get_all_schools_rank_and_scores(countries, start_year, end_year):
    countries_list = "(" + ", ".join(f"'{item}'" for item in countries) + ")"

    query =  f"""SELECT university_name, year,
        shanghai_rank,shanghai_total_score,  cwur_rank,cwur_total_score,times_rank,times_total_score
    FROM {table_name}
    WHERE country in {countries_list} and year>={start_year} and year<={end_year};
    """
    
    invoke_query(query)   

def invoke_query(query):
    try:
        result = session.execute(query)
        for row in result:
            print(row)
    except Exception as e:
        print(f"Error executing query: {e}")


'''get_number_of_ranked_school_by_country_and_year(['USA', 'Israel'], 2010,2015)
get_best_rank_of_ranked_school_by_country_and_year(['USA', 'Israel'], 2010,2015)
get_all_times_stats(['USA', 'Israel'])
get_all_schools_rank_and_scores(['Israel'], 2014, 2015)'''