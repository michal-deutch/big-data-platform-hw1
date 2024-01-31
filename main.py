import math
import university_criteria_queries as uc
import University_World_Rankings_queries as ur
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats

##notes for user:
#The database contains data from three different sets for three different rating methods (shanghai rating, times rating and CWUR).
# each data set contain data over different range of years. When inserting arguments to the following functions, pay attention to this
# in order to receive non-empty, interesting information.

#years ranges are:
    #Times ranking 2011-2016
    #Shanghai ranking 2005-2015
    #cwur 2012-2015

#insert institues names from the following df
all_inst_df=uc.get_all_possible_Institutes()
# print(all_inst_df)


#enter countries as an array
#to get results separted by country enter by_country=True, to get results separted by year enter by_year=True. to get full data enter False for both
def vis_get_number_of_ranked_school_by_country_and_year(countries,start_year,end_year,by_country,by_year):
    df1 = ur.get_number_of_ranked_school_by_country_and_year(countries, start_year, end_year)

    if by_country is True:
        grouped=df1.groupby("country")
        keys=grouped.groups.keys()

        for country in keys:
            df=grouped.get_group(country)[["year","total_schools_ranked_in_shanghai","total_schools_ranked_in_cwur","total_schools_ranked_in_times","total_ranked_universities"]]
            df=df.reset_index(drop=True)
            print(country)
            print(df)

    if by_year is True:
        grouped=df1.groupby("year")
        keys=grouped.groups.keys()

        for year in keys:
            df=grouped.get_group(year)[["country","total_schools_ranked_in_shanghai","total_schools_ranked_in_cwur","total_schools_ranked_in_times","total_ranked_universities"]]
            df=df.reset_index(drop=True)
            print(year)
            print(df)

    if by_country is False and by_year is False:
        print(df1)

#enter countries as an array
#to get results separted by country enter by_country=True, to get results separted by year enter by_year=True. to get full data enter False for both
def vis_get_best_rank_of_ranked_school_by_country_and_year(countries, start_year, end_year,by_country,by_year):
    df2=ur.get_best_rank_of_ranked_school_by_country_and_year(countries, start_year, end_year)

    if by_country is True:
        grouped = df2.groupby("country")
        keys = grouped.groups.keys()

        for country in keys:
            df = grouped.get_group(country)[
                ["year", "best_rank_in_shanghai", "best_rank_in_cwur", "best_rank_in_times"]]
            df=df.reset_index(drop=True)
            print(country)
            print(df)

    if by_year is True:
        grouped = df2.groupby("year")
        keys = grouped.groups.keys()

        for year in keys:
            df = grouped.get_group(year)[
                ["country", "best_rank_in_shanghai", "best_rank_in_cwur", "best_rank_in_times"]]
            df=df.reset_index(drop=True)
            print(year)
            print(df)

    if by_country is False and by_year is False:
        print(df2)

#enter countries as an array
#to get results separted by country enter by_country=True, to get full data enter False
def vis_get_all_times_stats(countries,by_country):
    df3=ur.get_all_times_stats(countries)

    if by_country is True:
        grouped = df3.groupby("country")
        keys = grouped.groups.keys()

        for country in keys:
            df = grouped.get_group(country)[
                ["worst_rank_in_shanghai","worst_rank_in_cwur","worst_rank_in_times","best_rank_in_shanghai","best_rank_in_cwur","best_rank_in_times",
                 "average_rank_in_shanghai","average_rank_in_cwur","average_rank_in_times"]]
            df=df.reset_index(drop=True)
            print(country)
            print(df)

    else:
        print(df3)

#enter countries as an array
#to get results separted by countryand year enter by_country_year=True, to get results separted by country and institution enter by_country_institution=True.
# to get full data enter False for both
def vis_get_all_schools_rank_and_scores(countries,start_year,end_year,by_country_year,by_country_institution):
    df4 = ur.get_all_schools_rank_and_scores(countries, start_year, end_year)

    grouped_c = df4.groupby("country")
    keys_c = grouped_c.groups.keys()

    if by_country_year is True:
        for country in keys_c:
            print(country)
            grouped_y = df4.groupby("year")
            keys_y = grouped_y.groups.keys()
            for year in keys_y:
                df = grouped_y.get_group(year)[
                    ["university_name","shanghai_rank","shanghai_total_score","cwur_rank","cwur_total_score","times_rank","times_total_score"]]
                df=df.reset_index(drop=True)
                print(year)
                print(df)

    if by_country_institution is True:
        for country in keys_c:
            print(country)
            grouped_i = df4.groupby("university_name")
            keys_i = grouped_i.groups.keys()
            for uni in keys_i:
                df = grouped_i.get_group(uni)[
                    ["year", "shanghai_rank", "shanghai_total_score", "cwur_rank", "cwur_total_score",
                     "times_rank", "times_total_score"]]
                df=df.reset_index(drop=True)
                print(uni)
                print(df)

    if by_country_year is False and by_country_institution is False:
        print(df4)

#enter school_names as an array
#get criteria trend over the year for specific institues
def vis_get_criteria_trend(plt_path,school_names, criteria_name, year_range=None):
    dfs=[]
    if year_range is None:
        f6 = uc.get_criteria_trend(school_names, criteria_name)
    else:
        df6=uc.get_criteria_trend(school_names, criteria_name, year_range)

    #group by school
    grouped=df6.groupby("university_name")
    keys=grouped.groups.keys()

    #drop school name column and change column name to school name
    for uni in keys:
        df=grouped.get_group(uni)[["year",criteria_name]]
        df=df.rename(columns={criteria_name: uni})
        dfs.append(df)

    #merge to one df
    for idx,df in enumerate(dfs):
        if idx==0:
            final_df=df
        else:
            final_df=final_df.merge(df,on="year",how="outer")


    final_df=final_df.dropna(axis=1,how='all')
    # final_df["year"]=final_df["year"].astype(int)
    unis = final_df.columns.tolist()
    unis.remove("year")

    for uni in unis:
        sns.lineplot(data=final_df,x="year",y=uni,label=uni)

    plt.ylabel(criteria_name)
    plt.title(criteria_name+" for chosen institutes")
    plt.legend(loc='center right',fontsize='x-small')
    plt.xticks(range(year_range[0],year_range[1]))
    plt.savefig(plt_path)
    plt.clf()

#enter criteria_names as an array
#get criterias trends over the year for specific school
def vis_get_school_trends(plt_path,school_name, criteria_names, year_range=None):
    if year_range is None:
        df7=uc.get_school_trends(school_name, criteria_names)
    else:
        df7 = uc.get_school_trends(school_name, criteria_names,year_range)

    df7=df7.drop(["university_name"],axis=1)

    for cr in criteria_names:
        sns.lineplot(data=df7,x='year',y=cr,label=cr)

    plt.ylabel("Scores")
    plt.title(school_name)
    plt.legend(loc='center right',fontsize='x-small')
    plt.xticks(range(year_range[0],year_range[1]))
    plt.savefig(plt_path)
    plt.clf()

#show correlation between two criterias for each year in range
def vis_get_criterias_correlation(plt_path,criteria_one, criteria_two,year_range):

    num_of_plots = year_range[1] - year_range[0] + 1
    if num_of_plots % 3 == 0:
        num_rows = int(num_of_plots / 3)
    else:
        num_rows = math.floor(num_of_plots / 3) + 1

    fig, axs = plt.subplots(num_rows, 3, sharex=True, sharey=True, figsize=(15, 8))

    # get data for year
    df8 = uc.get_criterias_by_year(criteria_one, criteria_two,year_range)
    df8=df8.dropna(axis=0)
    grouped = df8.groupby("year")

    year = year_range[0]

    for ax in axs.ravel():
        if year <= year_range[1]:
            df_g=grouped.get_group(year)[[criteria_one,criteria_two]]
            pear = stats.pearsonr(df_g[criteria_one], df_g[criteria_two])
            sns.regplot(data=df_g, x=criteria_one, y=criteria_two, ax=ax)
            ax.title.set_text(year)
            ax.legend([" $R^2$=" + str(round(pear[0], 3))])
            year = year + 1
            print(df_g)
        else:
            continue

    fig.suptitle("Correlation between " + criteria_one + " and " + criteria_two, fontsize='x-large')
    plt.tight_layout()
    plt.savefig(plt_path)
    plt.clf()


#for rank_type enter one of the following: shanghai,times or cwur
def build_line_plot_by_institute_and_rank_type(plt_path,country,year_range,rank_type):
    if rank_type == "shanghai":
        rank="shanghai_rank"
    elif rank_type == "times":
        rank="times_rank"
    else:
        rank="cwur_rank"

    df = ur.get_rank_per_year(rank, year_range)
    col = df[rank].tolist()
    max_rank = int(max(col))

    dfs=[]


    df4=ur.get_all_schools_rank_and_scores([country],year_range[0],year_range[1])
    df4=df4[["year","university_name",rank]]
    grouped=df4.groupby("university_name")
    keys=grouped.groups.keys()

    for uni in keys:
        df=grouped.get_group(uni)[["year",rank]]
        df=df.rename(columns={rank: uni})
        dfs.append(df)

    for idx,df in enumerate(dfs):
        if idx==0:
            final_df=df
        else:
            final_df=final_df.merge(df,on="year",how="outer")

    final_df=final_df.dropna(axis=1,how='all')
    # final_df["year"]=final_df["year"].astype(int)
    unis = final_df.columns.tolist()
    unis.remove("year")

    for uni in unis:
        sns.lineplot(data=final_df,x="year",y=uni,label=uni)

    plt.ylabel(rank +" out of "+str(max_rank))
    plt.title(rank_type+" ranking for "+country+"'s"+" institutes",fontsize='large')
    plt.legend(bbox_to_anchor=(1.05, 1.0), loc='upper left')
    # plt.legend(loc='center right',fontsize='x-small')
    plt.xticks(range(year_range[0],year_range[1]+1))
    plt.savefig(plt_path, bbox_inches='tight')
    plt.clf()

#for rank_type enter one of the following: shanghai,times or cwur
def corr_btw_criteria_and_rank (plt_path,cr,rank_type,year_range):

    if rank_type == "shanghai":
        rank="shanghai_rank"
    elif rank_type == "times":
        rank="times_rank"
    else:
        rank="cwur_rank"


    num_of_plots=year_range[1]-year_range[0]+1
    if num_of_plots%3==0:
        num_rows=int(num_of_plots/3)
    else:
        num_rows=math.floor(num_of_plots/3)+1

    fig, axs = plt.subplots(num_rows,3,sharex=True,sharey=True,figsize=(15,8))

    #get ranks for year
    df9=ur.get_rank_per_year(rank,year_range).dropna()
    grouped_r = df9.groupby("year")
    keys_r = grouped_r.groups.keys()

    #get criteria for year
    df10=uc.get_criterias_by_year(cr,None,year_range).dropna()
    grouped_c = df10.groupby("year")

    year=year_range[0]

    for ax in axs.ravel():
        if year <= year_range[1]:
            df_r = grouped_r.get_group(year)[["university_name", rank]]
            df_c = grouped_c.get_group(year)[["university_name", cr]]
            df_merged = df_r.merge(df_c, on="university_name")
            pear = stats.pearsonr(df_merged[cr], df_merged[rank])
            sns.regplot(data=df_merged, x=cr, y=rank, ax=ax)
            ax.title.set_text(year)
            ax.legend([" $R^2$=" + str(round(pear[0], 3))])
            year = year + 1
            print(df_merged)
        else:
            continue

    fig.suptitle("Correlation between "+cr+" and "+rank,fontsize='x-large')
    plt.tight_layout()
    plt.savefig(plt_path)
    plt.clf()





# vis_get_number_of_ranked_school_by_country_and_year(["Israel","USA"], 2012, 2015,True,False)

# vis_get_best_rank_of_ranked_school_by_country_and_year(["Israel","USA"], 2012, 2015,True,True)

# vis_get_all_times_stats(["Israel","USA"],False)

# vis_get_all_schools_rank_and_scores(["Israel","USA"], 2012, 2015,True,False)

# vis_get_criteria_trend(r"C:\Users\Ofri\Desktop\school\MSc\Big_Data\big-data-platform-hw1\trend1.jpeg",["Harvard University","University of Zurich","Kyoto University"], "shanghai_staff_award_score", (2011,2016))

# vis_get_school_trends(r"C:\Users\Ofri\Desktop\school\MSc\Big_Data\big-data-platform-hw1\trend2.jpeg","Harvard University", ["times_teaching_score","times_research_score"],(2011,2016))

# vis_get_criterias_correlation(r"C:\Users\Ofri\Desktop\school\MSc\Big_Data\big-data-platform-hw1\corr1.jpeg","times_teaching_score", "times_research_score",(2011,2016))

# build_line_plot_by_institute_and_rank_type(r"C:\Users\Ofri\Desktop\school\MSc\Big_Data\big-data-platform-hw1\ranks_shang.jpeg",'Israel',(2005,2015),"shanghai")

# build_line_plot_by_institute_and_rank_type(r"C:\Users\Ofri\Desktop\school\MSc\Big_Data\big-data-platform-hw1\ranks_times.jpeg",'Israel',(2011,2016),"times")

# build_line_plot_by_institute_and_rank_type(r"C:\Users\Ofri\Desktop\school\MSc\Big_Data\big-data-platform-hw1\ranks_cwur.jpeg",'Israel',(2012,2015),"cwur")

# corr_btw_criteria_and_rank (r"C:\Users\Ofri\Desktop\school\MSc\Big_Data\big-data-platform-hw1\corr4.jpeg","times_research_score","shanghai",(2011,2015))

