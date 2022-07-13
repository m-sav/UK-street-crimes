import os 

from pandas import read_csv,DataFrame,concat

all_files_path = 'data/'
# all_files_path = 'sample_data/'


def extract_data(files_path):

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

final_data = extract_data(all_files_path)
# final_data = read_csv('final_structured_data.csv')
# print(final_data)