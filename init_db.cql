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
    shanghai_total_score FLOAT, // used to determine rank
    cwur_rank INT,
    times_rank INT,
    times_total_score FLOAT, // used to determine rank
    PRIMARY KEY(year, country, university_name)
);

CREATE TABLE IF NOT EXISTS University_Criteria (
    year INT,
    university_name TEXT,
    country TEXT,

    // Shanghai Criteria
    alumni_award_score FLOAT,
    staff_award_score FLOAT,
    hici_score FLOAT,
    ns_score FLOAT,
    pub_score FLOAT,
    pcp_score FLOAT,

    // Times Criteria
    teaching_score FLOAT,
    international_score FLOAT,
    research_score FLOAT,
    citations_influence_score FLOAT,
    income_score FLOAT,
    num_students INT,
    student_staff_ratio FLOAT,
    international_students_percentage INT,
    female_male_ratio FLOAT,

    // CWUR Criteria
    quality_of_education_rank INT,
    alumni_employment_rank INT,
    quality_of_faculty_rank INT,
    publications_rank INT,
    influence_rank INT,
    citations_number INT,

    PRIMARY KEY(university_name, year)
);

CREATE TABLE IF NOT EXISTS Country_Statistics (
    year INT,
    country TEXT,
    metric TEXT, // from attainment: series_name, from expenditure: institute_type + direct_expenditure
    measure FLOAT,
    PRIMARY KEY ((country, year), metric)
);
