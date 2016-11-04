# -*- coding: utf-8 -*-
"""
Created on Fri Oct 28 22:55:54 2016

@author: Francesco
"""
import numpy as np
# create_hist_mod.py 
def creating_empty_dictionary(keys_list,return_dictionary_lenght=True):
    dictionary_aus={}
    for i in keys_list:
        dictionary_aus[i]=0
    if return_dictionary_lenght:
        print('dictionary lenght ', len(dictionary_aus))
    return dictionary_aus
def creating_dataframe_from_database_querying(query,database_name,return_shape=True):
    import sqlite3 
    import pandas as pd
    conn = sqlite3.connect(database_name)
    dataframe_new = pd.read_sql_query(query, conn)
    if return_shape:
        print('shape of created dataframe ',dataframe_new.shape)
    return dataframe_new
def counting_number_of_object_in_dataframe_per_bin_values(dataframe_serie_origin,
    dictionary_aus,bin_names,num_objects,return_corrispondence_not_found=True,\
    return_head_dataframe_from_dict=True):
    import pandas as pd
    count_exception=0
    count_found=0
    for i in dataframe_serie_origin:
        try:
            dictionary_aus[i]=dictionary_aus[i]+1
            count_found +=1
        except Exception:
            count_exception +=1
            pass
    if return_corrispondence_not_found:
        print('percentage of not matches found =',100*count_exception/float(count_found))
    # Convert dictionary to dataframe
    datafram_from_dict = pd.DataFrame.from_dict(dictionary_aus, orient='index').reset_index()
    datafram_from_dict.rename(columns={'index': bin_names, 0: num_objects}\
    , inplace=True)
    datafram_from_dict2=datafram_from_dict.sort_values(num_objects,inplace=False,ascending=False)
    if return_head_dataframe_from_dict:
        print('not_sorted_dataframe ',datafram_from_dict.head())
        print('--------------------------')
        print('sorted_dataframe ',datafram_from_dict2.head())
    return  datafram_from_dict
def read_poste_code_ccg(file_name_poste_code_ccg):
    import pandas as pd
    #----------------creating a list of unique post code per region
    poste_code_to_region = pd.read_csv(file_name_poste_code_ccg,sep=",",encoding = "ISO-8859-1")
    print('poste_code_to_region.shape' ,poste_code_to_region.shape)
    print('poste_code_to_region.dtypes ',poste_code_to_region.dtypes)
    list_unique_region=poste_code_to_region.groupby('ccg').ccg.nunique()
    return  poste_code_to_region,list_unique_region
def merging_dataframes(df1,df2,left_arg,right_arg):
    df_aus=df1
    df_aus = df_aus.merge(df2,left_on=left_arg, right_on=right_arg,how="left")
    df_aus=df_aus.dropna(subset=['ccg'])
    return df_aus
def merging_dataframes2(df1,df2,arg):
    df_aus=df1
    df_aus = df_aus.merge(df2,on=arg,how="left")
    return df_aus    
def counting_per_ccg(df,list_unique_region,items_to_group):
##    command_to_groupby='''df.groupby('ccg').'''+items_to_group+'.nunique()'
##    print(command_to_groupby)
##    count_per_ccg=exec(command_to_groupby)
    if items_to_group=='CONSTITUENT_ID':
        count_per_ccg=df.groupby('ccg').CONSTITUENT_ID.nunique()
    elif items_to_group=='TC_Name1':
         count_per_ccg=df.groupby('ccg').TC_Name1.nunique()
    elif items_to_group=='OrderNumber':
         count_per_ccg=df.groupby('ccg').OrderNumber.nunique()
    count_per_ccg2=pd.DataFrame(dict(list_unique_region =list_unique_region,\
    count_per_ccg=count_per_ccg)).reset_index()
    count_per_ccg2['count_per_ccg']=count_per_ccg2['count_per_ccg'].fillna(0)
    del count_per_ccg2['list_unique_region']
    return count_per_ccg2
def count_unique_n1(df,n1):
    def ff(x):
        x=1
        return x
    df_new=df[n1].apply(lambda x: ff(x))
    df['count_aus']=df_new
    count_unique=df.groupby(n1).count_aus.sum()
    count_unique=count_unique[1:]
    return count_unique
def mean_n2_on_unique_n1(df,n1,n2):
    df[n2]=df[n2].apply(lambda x: float(x))
    sum_n2=df.pivot_table(index=n1, values=n2, aggfunc=np.sum)
    sum_n2=sum_n2[1:]
    return sum_n2
    
    
    