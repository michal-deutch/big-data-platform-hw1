import socket
import re
import gevent
import gevent.monkey
gevent.monkey.patch_all()
from cassandra.io.geventreactor import GeventConnection
from cassandra.cluster import Cluster
import os
import csv
from thefuzz import fuzz
from thefuzz import process

# Cassandra connection details
keyspace = "university_rankings"
contact_points = ["127.0.0.1"]
cluster = Cluster(contact_points=contact_points)
cluster.connection_class = GeventConnection
session = cluster.connect(keyspace)
university_world_rankings_table = "University_World_Rankings"
universities_criteria_table = "University_Criteria"


f'''
CREATE TABLE IF NOT EXISTS {university_world_rankings_table}(
    year INT,
    university_name TEXT,
    country TEXT,
    shanghai_rank INT,
    shanghai_total_score FLOAT,
    cwur_rank INT,
    cwur_total_score FLOAT,
    times_rank INT,
    times_total_score FLOAT,
    PRIMARY KEY(country, year , university_name)
);
session.execute(create_table_query)
'''

'''
CREATE TABLE IF NOT EXISTS Country_Statistics (
    year INT,
    country TEXT,
    metric TEXT, // from attainment: series_name, from expenditure: institute_type + direct_expenditure
    measure FLOAT,
    PRIMARY KEY (country, year)
);
'''

f'''
CREATE TABLE IF NOT EXISTS {universities_criteria_table} (
    year INT,
    university_name TEXT,
    country TEXT,

    // Shanghai Criteria
    shanghai_alumni_award_score FLOAT,
    shanghai_staff_award_score FLOAT,
    shanghai_hici_score FLOAT, // Highly cited researchers in 21 broad subject categories
    shanghai_ns_score FLOAT, // Papers published in Nature and Science
    shanghai_pub_score FLOAT, // Papers indexed in Science Citation Index-expanded 
    shanghai_pcp_score FLOAT, // Per capita academic performance of an institution.

    // Times Criteria
    times_teaching_score FLOAT,
    times_research_score FLOAT,
    times_citations_influence_score FLOAT,

    // CWUR Criteria
    cwur_alumni_employment_rank INT,
    cwur_publications_rank INT,
    cwur_influnce_jurnal_rank INT,
    cwur_citation_rank INT,
    cwur_patents_rank INT,

    PRIMARY KEY(university_name, year)
);
'''

def insert_cwur_data(file_path, university_to_country):
    with open(file_path, 'r', encoding='utf-8') as file:
        header = file.readline().strip().split(',')
        uni_count = 1
        csv_file = csv.reader(file)
        for line in csv_file:
            # Map column names from CWUR data to the Cassandra table
            year = int(line[header.index('year')])
            university = line[header.index('institution')]
            country = line[header.index('country')] 
            score = float(line[header.index('score')])
            rank = int(line[header.index('world_rank')])

            cwur_alumni_employment_rank = int(line[header.index('alumni_employment')])
            cwur_publications_rank = int(line[header.index('publications')])
            cwur_influnce_jurnal_rank = int(line[header.index('influence')])
            cwur_citation_rank = int(line[header.index('citations')])
            cwur_patents_rank = int(line[header.index('patents')])

            if university_to_country.get(university) is None:
                uni_count+=1
                university_to_country[university] = country
            
            university_World_Rankings_insert_query = f"""
            INSERT INTO {university_world_rankings_table}
            (year, university_name, country, cwur_total_score, cwur_rank)
            VALUES (%s, %s, %s, %s, %s);
            """
            session.execute(university_World_Rankings_insert_query, (year, university, country, score, rank))

            university_Criteria_query = f"""
            INSERT INTO {universities_criteria_table}
            (year, university_name, country, cwur_alumni_employment_rank, cwur_publications_rank, cwur_influnce_jurnal_rank, cwur_citation_rank, cwur_patents_rank)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
            session.execute(university_Criteria_query, (year, university, country,cwur_alumni_employment_rank ,cwur_publications_rank, cwur_influnce_jurnal_rank,cwur_citation_rank,cwur_patents_rank))

def insert_times_data(file_path, university_to_country):
    times_ingested = set()
    with open(file_path, 'r', encoding='utf-8') as file:
        header = file.readline().strip().split(',')
        csv_file = csv.reader(file)
        uni_count = 1
        duplications_mapping = dict()
        for line in csv_file:
            # Map column names from Times data to the Cassandra table
            year = int(line[header.index('year')])
            university = line[header.index('university_name')]
            country = standarize_country(line[header.index('country')])
            score_as_string = line[header.index('total_score')]
            score = float(score_as_string) if score_as_string != "-" else None
            rank_as_string = line[header.index('world_rank')]
            rank = int(rank_as_string.split('-')[0]) if '-' in rank_as_string else  int(rank_as_string.replace('=', ''))

            times_teaching_score = float(line[header.index('teaching')])
            times_research_score = float(line[header.index('research')])
            times_citations_influence_score = float(line[header.index('citations')])

            if university_to_country.get(university) is None:
                duplication = check_if_exist_duplication(university_to_country, university, duplications_mapping,times_ingested, year, country)
                if(duplication!= None and duplication != university):
                    university = duplication
                    country = university_to_country[university]
                else:
                    uni_count+=1
                    university_to_country[university] = country


            if score is None:
                insert_query_with_missed_score = f"""
                INSERT INTO {university_world_rankings_table}
                (year, university_name, country, times_rank)
                VALUES (%s, %s, %s, %s);
                """
                session.execute(insert_query_with_missed_score, (year, university, country, rank))

            else:
                insert_query_with_all_params = f"""
                INSERT INTO {university_world_rankings_table}
                (year, university_name, country, times_total_score, times_rank)
                VALUES (%s, %s, %s, %s, %s);
                """
                session.execute(insert_query_with_all_params, (year, university, country, score, rank))
               
            university_Criteria_query = f"""
            INSERT INTO {universities_criteria_table}
            (year, university_name, country, times_teaching_score, times_research_score, times_citations_influence_score)
            VALUES (%s, %s, %s, %s, %s, %s);
            """
            session.execute(university_Criteria_query, (year, university, country,times_teaching_score ,times_research_score, times_citations_influence_score))
            times_ingested.add(f"{year}_{university}_{country}")

def insert_shanghai_data(file_path, university_to_country):
    shanghai_ingested = set()
    shanghai_universities_to_countries = create_shanghai_universities_to_countries()
    with open(file_path, 'r', encoding='utf-8') as file:
        header = file.readline().strip().split(',')
        csv_file = csv.reader(file)
        uni_count = 0
        duplications_mapping = dict()
        for line in csv_file:
            year = int(line[header.index('year')])
            university = line[header.index('university_name')]
            if(university == ""):
                continue # 2013 99
            score_as_string = line[header.index('total_score')]
            score = float(score_as_string) if score_as_string != "" else None
            rank_as_string = line[header.index('world_rank')]
            rank = int(rank_as_string.split('-')[0]) if '-' in rank_as_string else  int(rank_as_string.replace('=', ''))
            shanghai_alumni_award_score = float(line[header.index('alumni')])
            shanghai_staff_award_score = float(line[header.index('award')]) if line[header.index('award')] != "" else 0 # handle University of Oregon case
            shanghai_hici_score = float(line[header.index('hici')]) if line[header.index('hici')] != "" else 0 # handle University of Oregon case
            shanghai_pub_score = float(line[header.index('pub')]) if line[header.index('pub')] != "" else 0 # handle University of Oregon case
            shanghai_pcp_score = float(line[header.index('pcp')]) if line[header.index('pcp')] != "" else 0 # handle University of Oregon case
            shanghai_ns_score = float(line[header.index('pcp')]) if line[header.index('ns')] != "" else 0 # handle University of Oregon case

            country = None
            if university_to_country.get(university) is None:
                duplication = check_if_exist_duplication(university_to_country, university,duplications_mapping, shanghai_ingested, year) if country is None else check_if_exist_duplication(university_to_country, university,duplications_mapping, shanghai_ingested, country)
                if(duplication!= None and duplication != university):
                    if f"{year}_{duplication}_{university_to_country[duplication]}" not in shanghai_ingested:
                        university = duplication
                        country = university_to_country[university]
                    else:
                        continue
                else:
                    # Detection of university that was not ranked in other ranks. Assign country to unkown/ trying get from countries table
                    uni_count+=1
                    country = "Unknown" if shanghai_universities_to_countries.get(university) == None else shanghai_universities_to_countries.get(university)
                    university_to_country[university] = country   
            else:
                country = university_to_country[university]

            if score is None:
                insert_query_with_missed_score = f"""
                INSERT INTO {university_world_rankings_table}
                (year, university_name, country, shanghai_rank)
                VALUES (%s, %s, %s, %s);
                """
                session.execute(insert_query_with_missed_score, (year, university, country, rank))

            else:
                insert_query_with_all_params = f"""
                INSERT INTO {university_world_rankings_table}
                (year, university_name, country, shanghai_rank, shanghai_total_score)
                VALUES (%s, %s, %s, %s, %s);
                """
                session.execute(insert_query_with_all_params, (year, university, country, rank, score )) 
            
            university_Criteria_query = f"""
            INSERT INTO {universities_criteria_table}
            (year, university_name, country, shanghai_alumni_award_score, shanghai_staff_award_score, shanghai_hici_score, shanghai_pub_score, shanghai_pcp_score, shanghai_ns_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            session.execute(university_Criteria_query, (year, university, country,shanghai_alumni_award_score ,shanghai_staff_award_score, shanghai_hici_score, shanghai_pub_score, shanghai_pcp_score, shanghai_ns_score))

            shanghai_ingested.add(f"{year}_{university}_{country}")

# This method intended to deal with universities duplicated names that could not be recognized by
#  substrings matching or by  Levenshtein Distance
def get_custom_duplicated_name(university):
    if (university == "KU Leuven" or  university == "Catholic University of Leuven"):
        return "Katholieke Universiteit Leuven"
    if (university == "State University of New York Albany" or university == "SUNY at Albany"):
        return "University at Albany, SUNY"
    if (university == "The Imperial College of Science, Technology and Medicine"):
        return "Imperial College London"
    if (university == "LMU Munich"):
        return "Ludwig Maximilian University of Munich" #cacthed in threshold >=86
    if (university == "William & Mary"):
        return "College of William and Mary" #cacthed in threshold >=86
    if (university ==  "Heidelberg University"):
        return "Ruprecht Karl University of Heidelberg"
    if (university ==  "TU Dresden"): # detected as Dortumund in case of threshold 86
        return "Dresden University of Technology"
    if (university ==  "Technical University of Darmstadt" or university == "Technical University Darmstadt"): 
        return "Darmstadt University of Technology"
    if (university ==  "University of Paris North – Paris 13"): 
        return "University of Paris 13"
    if (university ==  "University of Roma - La Sapienza"): 
        return "Sapienza University of Rome"
    if(university == "State University of New York at Stony Brook"):
        return "Stony Brook University"
    if(university == "University of Frankfurt"):
        return "Goethe University Frankfurt"
    if(university == "Polytechnic Institute of Milan"):
        return "Polytechnic University of Milan"
    if (university == "Technical University of Braunschweig"):
        return "Braunschweig University of Technology"
    if (university == "The Open University"):
        return "Open University (UK)"

def check_if_exist_duplication(university_to_country, university, duplications_mapping, ingestedKey, year, country = None):
    duplicated_name = duplications_mapping.get(university) or get_custom_duplicated_name(university)
    if(duplicated_name is not None):
        return duplicated_name
    # Levenshtein Distance
    similar_names = process.extractBests(university, university_to_country.keys(), score_cutoff=90, limit=3) if country is None else process.extractBests(university, [key for key, value in university_to_country.items() if value == country], score_cutoff=90, limit=3)
    i = 0 
    while(i != len(similar_names)):
        if country is None or country == university_to_country[similar_names[i][0]] and f"{year}_{similar_names[i][0]}_{university_to_country[similar_names[i][0]]}" not in ingestedKey:
            duplications_mapping[university] = similar_names[i][0]
            return similar_names[i][0]
        i +=1
    
def create_shanghai_universities_to_countries():
    with open(os.path.join(script_directory, "data-sets", "timesData.csv"), 'r', encoding='utf-8') as file:
        csv_file = csv.reader(file)
        uni_to_country = dict()
        for line in csv_file:
            uni = line[0]
            country = standarize_country(line[1])
            uni_to_country[uni] = country
    return uni_to_country
   
def standarize_country(country):
    if(country ==  "United States of America"):
        return "USA"
    if(country == "Republic of Ireland"):
        return "Ireland"
    return country


# Insert data into the table
universityToCountry = dict()
script_directory = os.path.dirname(os.path.abspath(__file__))


insert_cwur_data(os.path.join(script_directory, "data-sets", "cwurData.csv"), universityToCountry)
insert_times_data(os.path.join(script_directory, "data-sets", "timesData.csv"), universityToCountry)
insert_shanghai_data(os.path.join(script_directory, "data-sets", "shanghaiData.csv"), universityToCountry)

# Close the Cassandra connection
session.shutdown()
cluster.shutdown()
