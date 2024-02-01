CREATE KEYSPACE IF NOT EXISTS University_Rankings WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE University_Rankings;

CREATE TABLE IF NOT EXISTS University_World_Rankings (
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

CREATE TABLE IF NOT EXISTS University_Criteria (
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

CREATE TABLE IF NOT EXISTS Country_Statistics (
    year INT,
    country TEXT,
    metric TEXT, // from attainment: series_name, from expenditure: institute_type + direct_expenditure
    measure FLOAT,
    PRIMARY KEY ((country, year), metric)
);
