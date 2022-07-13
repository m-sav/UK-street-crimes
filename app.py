import os 
import matplotlib.pyplot as plt
import numpy as np
import argparse
from pandas import read_csv,DataFrame,concat


parser = argparse.ArgumentParser()
parser.add_argument("-mode",default='from_csv',help="mode = 'from_csv' or 'extract'")
parser.add_argument("-source",default='data/',help="provide a path to the data")

args = parser.parse_args()

all_files_path = args.source

def extract_data(files_path):
    print('initiating extract_data()')
    # INITIALIZE THE RESULT DATAFRAME
    columns = ['crimeID','districtName','latitude','longitude','crimeType','lastOutcome']
    result = DataFrame(columns = columns)

    try:
        # OPEN CSVS RECURSIVELY

        for root, dirs, files in os.walk(all_files_path, topdown=False):
            
            # GET DISTINCT DISTRICTS IN EACH FOLDER
            all_districts = [district for district in set(list(map(lambda x: '-'.join(x.split('-')[2:-1]),files)))]
            files = sorted(files) # files is a list of all files in a folder

            # GROUP files BY DISTRICT
            grouped_files = []  # grouped_files is a list of lists, each of them contains a pair of outcomes/street files
            for district in all_districts:
                files_per_district = [name for name in files if district in name]
                grouped_files.append(files_per_district)

            # ITERATE OVER EACH PAIR OF OUTCOMES/STREET FILES
            for group in grouped_files: 
                street_df = DataFrame()
                outcomes_df = DataFrame()

                # EXTRACT THE DESIRED FIELDS FROM EACH FILE
                for file in group:
                    # print(file)
                    file_type = file.split('-')[-1].replace('.csv','') # filetype is either 'outcomes' or 'street'
                    district = '-'.join(file.split('-')[2:-1])

                    filepath = root + '/' + file

                    df = read_csv(filepath)
                    df_columns = df.columns.to_list()
                    new_df = DataFrame()

                    # EXTRACT DESIRED COLUMNS FROM DATAFRAME
                    new_df['crimeID'] = df['Crime ID']
                    districtName = ' '.join(file.split('-')[2:-1]) # extract the districtName from the filename
                    new_df['districtName'] = districtName 
                    new_df['latitude'] = df['Latitude']
                    new_df['longitude'] = df['Longitude']
                    new_df['fileType'] = df.apply(lambda x: file_type,axis=1)

                    if file_type == 'street':
                        new_df['crimeType'] =  df['Crime type'] 
                        new_df['lastOutcome'] =  df['Last outcome category']
                        street_df = concat([street_df,new_df],axis=0)
                    elif file_type == 'outcomes':
                        new_df['lastOutcome'] =  df['Outcome type']
                        outcomes_df = concat([outcomes_df,new_df],axis=0)

                # GET A MERGED DATAFRAME FOR EACH PAIR OF OUTCOMES/STREET FILES
                if not street_df.empty and not outcomes_df.empty:
                    merged_pair_df = street_df.merge(outcomes_df, on = ['crimeID','districtName','latitude','longitude'],how = 'outer').astype(str)

                    # OPTIONAL: filter out rows with no crimeID
                    merged_pair_df = merged_pair_df[merged_pair_df['crimeID'] != 'nan'] 

                    # CONSTRUCT lastOutcome COLUMN
                    def extract_lastOutcome(row):
                        # column lastOutcome_y represents the column 'Outcome type' from the <district>-outcomes.csv file
                        # column lastOutcome_x represents the column Last outcome category' from the <district>-street.csv file

                        if row['lastOutcome_y'] != 'nan' and row['lastOutcome_x'] != 'nan' :
                            return row['lastOutcome_y']
                        elif row['lastOutcome_y'] != 'nan' and row['lastOutcome_x'] == 'nan':
                            return row['lastOutcome_y']
                        elif row['lastOutcome_x'] != 'nan' and row['lastOutcome_y'] == 'nan':
                            return row['lastOutcome_x']
                    
                    merged_pair_df['lastOutcome'] = merged_pair_df.apply(lambda row: extract_lastOutcome(row),axis=1 ).astype(str)

                    # SELECT THE DESIRED COLUMNS
                    merged_pair_df = merged_pair_df.filter(columns)
                
                    # CONCAT EACH PAIR DATAFRAME WITH THE INITIAL RESULT DATAFRAME
                    result = concat([result,merged_pair_df],axis=0)
                elif not street_df.empty:
                    result = concat([result,street_df],axis=0) 
                else:
                    result = concat([result,outcomes_df],axis=0) 



            result = result.drop_duplicates().reset_index(drop=True)

        result = result.filter(columns)
        # SAVE RESULT TO CSV
        result.to_csv('final_structured_data.csv',index=False)
        return result

    except Exception as e:
        print(e)

<<<<<<< HEAD
if args.mode == 'extract':
    final_data = extract_data(all_files_path)
elif args.mode == 'from_csv':
    final_data = read_csv('final_structured_data.csv')

# VISUALIZE THE DATA TO GAIN SOME INSIGHTS

# 1) WHERE DO MOST CRIMES OCCUR?
no_of_crimes_per_district = final_data.groupby('districtName').size().sort_values(ascending = True) 
no_of_crimes_per_district.index = no_of_crimes_per_district.index.str.title()
no_of_crimes_per_district.plot.barh()
plt.ylabel('Number of crimes')
plt.xlabel('District')
plt.title('Number of crimes per district')
plt.savefig('Number of crimes per district.png')
# plt.show() ## uncomment to display the plot

## As we can see from the chart, most crimes occur in Metropolitan district. 
district_with_most_crimes = no_of_crimes_per_district.idxmax()
print(f"Most crimes occur in {district_with_most_crimes}.")

# 2) WHAT IS THE MOST COMMON CRIME IN METROPOLITAN DISTRICT? (see data per district below)
metropolitan_crimes = final_data[final_data.districtName == district_with_most_crimes.lower()].groupby('crimeType').size()
metropolitan_most_common_crime = metropolitan_crimes.idxmax()
metropolitan_least_common_crime = metropolitan_crimes.idxmin()

print(f"Most common crime in {district_with_most_crimes} is {metropolitan_most_common_crime}. \nLeast common crime in {district_with_most_crimes} is {metropolitan_least_common_crime} ")


# 3) WHAT IS THE MOST COMMON CRIME IN THE UK?
no_of_crime_type = final_data.groupby('crimeType').size().sort_values(ascending = True) 
no_of_crime_type.plot.barh()
plt.ylabel('Number of crimes')
plt.xlabel('Crime type')
plt.title('Number of crimes per crime type')
plt.savefig('Number of crimes per crime type.png')
# plt.show() ## uncomment to display the plot

most_common_crime = no_of_crime_type.idxmax()
least_common_crime = no_of_crime_type.idxmin()

print(f"Most common street crime in the UK is {most_common_crime}.")
print(f"Least common street crime in the UK is {least_common_crime}.")

# 4) WHAT IS THE MOST COMMON CRIME PER DISTRICT?
crimetypes_per_district = final_data.groupby(['districtName','crimeType']).size().reset_index(name = 'number_of_crimes')
crimetypes_per_district.districtName = crimetypes_per_district.districtName.str.title()

print(crimetypes_per_district)

most_crimes_per_district = crimetypes_per_district.groupby('districtName')[['crimeType', 'number_of_crimes']].max().sort_values(by=['number_of_crimes'],ascending = False)
print(most_crimes_per_district)

least_crimes_per_district = crimetypes_per_district.groupby('districtName')[['crimeType', 'number_of_crimes']].min().sort_values(by=['number_of_crimes'],ascending = False)
print(least_crimes_per_district)
=======
final_data = extract_data(all_files_path)
# final_data = read_csv('final_structured_data.csv')
# print(final_data)
>>>>>>> ef16f386ab1b8f40798f76f8652aa3458216b1b3
