# -*- coding: utf-8 -*-
"""
Created on Thu Nov 03 18:48:31 2016

@author: francesco
"""
import create_hist_mod as ch
import pandas as pd
def trasform_string_to_numer(x):
    if x !=1:
        x=2
    return x
database_name='raisers_edge_data.db'
#'''
#-----Extracts a dataframe with the information about the volunteer-----------
#----------------------------------------------------------------------------
#----------------------------------------------------------------------------'''
#query=''' select CONSTITUENT_ID,Status_Desc
#from Analysis_Volunteer_Types_View   ;'''
#
#''' the following module extracts a dataframe from the database_name.
#the attributes that it needs are: database_name,query '''
#volunters=ch.creating_dataframe_from_database_querying\
#(query,database_name,return_shape=True)
#volunters=volunters.rename(columns={'CONSTITUENT_ID': 'CID'})
#'''-------------------------------------------------------------'''
#''' this part of the code reshapes the dataframe and gives a dataframe 
#with uniques CID rows and assigns numerical value to the different volunteer
#status: 
#    2 'Active' or 'Acting'
#    1 other cases
#    0 when merging with other dataframe will mean never been volunter
#
#'''
#check=True # if true writes all the checks
#print('volunters.shape not unique', volunters.shape)
#list_unique=volunters['Status_Desc'].unique()
#if check:
#    print('list_unique',list_unique)
#volunters_aus=volunters[(volunters['Status_Desc']=='Active')
#|(volunters['Status_Desc']=='Acting')]
#volunters_aus=volunters_aus.drop_duplicates(subset=['CID'])
#volunters_aus=volunters_aus.rename(columns={'Status_Desc': 'VOL_ACTIVE'})
#volunters_new=volunters_aus
#volunters_aus=volunters[(volunters['Status_Desc']!='Active')
#&(volunters['Status_Desc']!='Acting') ]
#volunters_aus=volunters_aus.drop_duplicates(subset=['CID'])
#volunters_aus=volunters_aus.rename(columns={'Status_Desc': 'VOL_NOACTIVE'})
#volunters_new=volunters_new.merge(volunters_aus,\
#        on='CID', how="outer")
#volunters=volunters_new
#volunters['VOL_ACTIVE']=volunters['VOL_ACTIVE'].fillna(1)
#del volunters['VOL_NOACTIVE']
#volunters['VOL_ACTIVE']=volunters['VOL_ACTIVE'].apply(lambda x: trasform_string_to_numer(x))
#print('volunters.shape unique',volunters.shape) 
#if check:  
#    print('volunters.head()',volunters.head())
#    print('volunters.dtypes',volunters.dtypes)
#    
#'''
#---------------------------------------------------------------------------
#-----Extracts a dataframe with the information about the members-----------
#----------------------------------------------------------------------------
#----------------------------------------------------------------------------'''
#query=''' select CONSTITUENT_ID,Current_Standing
#from Analysis_Membership_View   ;'''
#
#''' the following module extracts a dataframe from the database_name.
#the attributes that it needs are: database_name,query '''
#members=ch.creating_dataframe_from_database_querying\
#(query,database_name,return_shape=True)
#members=members.rename(columns={'CONSTITUENT_ID': 'CID'})
#'''-------------------------------------------------------------'''
#''' this part of the code reshapes the dataframe and gives a dataframe 
#with uniques CID rows and assigns numerical value to the different members
#status: 
#    2 'Active' 
#    1 other cases
#    0 when merging with other dataframe will mean never been volunter
#
#'''
#check=True # if true writes all the checks
#print('members.shape not unique', members.shape)
#list_unique=members['Current_Standing'].unique()
#if check:
#    print('list_unique',list_unique)
#members_aus=members[(members['Current_Standing']=='Active')]
#members_aus=members_aus.drop_duplicates(subset=['CID'])
#members_aus=members_aus.rename(columns={'Current_Standing': 'MEM_ACTIVE'})
#members_new=members_aus
#members_aus=members[(members['Current_Standing']!='Active')]
#members_aus=members_aus.drop_duplicates(subset=['CID'])
#members_aus=members_aus.rename(columns={'Current_Standing': 'MEM_NOACTIVE'})
#members_new=members_new.merge(members_aus,\
#        on='CID', how="outer")
#members=members_new
#members['MEM_ACTIVE']=members['MEM_ACTIVE'].fillna(1)
#del members['MEM_NOACTIVE']
#members['MEM_ACTIVE']=members['MEM_ACTIVE'].apply(lambda x: trasform_string_to_numer(x))
#print('members.shape unique',members.shape)     
#'''ADD to the dataframe members with unique values other columns and trasform 
#that in numeric types'''  
#query=''' select CONSTITUENT_ID,PrimaryMember
#,TimesRenewed,ConsecYears,TotalYears
#from Analysis_Membership_View   ;''' 
#''' the following module extracts a dataframe from the database_name.
#the attributes that it needs are: database_name,query '''
#members2=ch.creating_dataframe_from_database_querying\
#(query,database_name,return_shape=True)
#members2=members2.rename(columns={'CONSTITUENT_ID': 'CID'})
#members=members.merge(members2,\
#        on='CID', how="left")
#if check:  
#    print('members.head()',members.head()) 
#    print('members.dtypes',members.dtypes)
#def convertStr(s):
#    """Convert string to either int or float."""
#    ret=0.0
#    try:
#        ret = int(s)
#    except ValueError:
#        ret=float(s)
#    return ret
#members['PrimaryMember']=members['PrimaryMember'].fillna(-2)
#members['PrimaryMember']=members['PrimaryMember'].apply(lambda x: int(x))
#members['TimesRenewed']=members['TimesRenewed'].fillna(0)
#members['TimesRenewed']=members['TimesRenewed'].apply(lambda x: int(x))
#members['ConsecYears']=members['ConsecYears'].fillna(0)
#members['ConsecYears']=members['ConsecYears'].apply(lambda x: int(x))
#'''PROBLEM IN CONVERISON STRING TO FLOAT
#members['TotalYears']=members['TotalYears'].fillna(0)
#members['TotalYears']=members['TotalYears'].apply(lambda x: convertStr(x)) '''
#del members['TotalYears']
#if check:  
#    print('members.head()',members.head()) 
#    print('members.dtypes',members.dtypes)
####--------------mosaic--------------------------------
import pickle
documents_f = open("Mosaic_dataframe.pickle", "rb") ####
mosaic = pickle.load(documents_f)
documents_f.close()
##print(df_mosaic.head())
mosaic=mosaic.rename(columns={'TC_Most Recent RE Constituent ID': 'CID'})
mosaic=mosaic[['CID',
'TOD_Product Category']]
mosaic=mosaic.dropna(subset=['CID'])
print('mosaic.shape',mosaic.shape)
list_unique=mosaic["TOD_Product Category"].unique()
if check:
    print(list_unique)
mosaic_aus=mosaic[(mosaic['TOD_Product Category']=='Daily Living Aids')
|(mosaic['TOD_Product Category']=='PUK Essentials')]
mosaic_aus['N_PRODUCTS_PK'] = mosaic_aus.groupby('CID')['TOD_Product Category'].transform(pd.Series.value_counts)
mosaic_aus=mosaic_aus.drop_duplicates(subset=['CID'])
del mosaic_aus['TOD_Product Category']
mosaic_new=mosaic_aus
mosaic_aus=mosaic[(mosaic['TOD_Product Category']!='Daily Living Aids')
&(mosaic['TOD_Product Category']!='PUK Essentials')]
mosaic_aus['N_PRODUCTS_NO_PK'] = mosaic_aus.groupby('CID')['TOD_Product Category'].transform(pd.Series.value_counts)
mosaic_aus=mosaic_aus.drop_duplicates(subset=['CID'])
del mosaic_aus['TOD_Product Category']
mosaic_new=mosaic_new.merge(mosaic_aus,\
        on='CID', how="outer")
mosaic=mosaic_new
print(mosaic.shape,mosaic.head())
#volunters_aus=volunters_aus.drop_duplicates(subset=['CID'])
#volunters_aus=volunters_aus.rename(columns={'Status_Desc': 'VOL_ACTIVE'})
#volunters_new=volunters_aus
#tuple_aus=[]
#def find_PK(x):
#    if x=='Daily Living Aids' or x=='PUK Essentials':
#        x=0
#    else:
#        x=1
#    return x
#df_mosaic_new2["TOD_Product Category"].fillna(0,inplace=True)
#df_mosaic_new2["TOD_Product Category"]=df_mosaic_new2["TOD_Product Category"]\
#.apply(lambda x: find_PK(x))
#df_mosaic_new2["TC_Most Recent RE Constituent ID"]\
#.apply(lambda x: int(x))
#df_mosaic_new2=df_mosaic_new2.rename(columns={'TC_Most Recent RE Constituent ID': 'CID'})
##print(df_mosaic_new2.head(),df_mosaic_new2.shape)
#products_PK=df_mosaic_new2[df_mosaic_new2['TOD_Product Category']==1]
#products_PK['Events'] = products_PK.groupby('CID')['TOD_Product Category'].transform(pd.Series.value_counts)
#products_PK=products_PK.drop_duplicates(subset=['CID'])
#products_PK=products_PK.rename(columns={'TOD_Product Category': 'PRODUCT'})
#products_PK=products_PK.rename(columns={'TC_Postcode': 'POST_C'})
#products_PK=products_PK.rename(columns={'Events': 'NUM_P'})
#print(products_PK.head(),products_PK.shape)
#list_unique=products_PK["CID"].unique()
#print(len(list_unique))
#list_dataframes['products_PK']=products_PK
'''------------------------------------------------------------------'''
'''-----Extracts a dataframe with the information about the gifts-----------
----------------------------------------------------------------------------
----------------------------------------------------------------------------'''  
#query=''' select CONSTITUENT_ID,Gift_Amount
#from Analysis_Gift_Split_View ;'''
#df_gift=ch.creating_dataframe_from_database_querying\
#(query,database_name,return_shape=True)
#if check:
#    print('df_gift.head()',df_gift.head())
#    print('df_gift.shape',df_gift.shape)
#number_of_gift_per_CID=ch.count_unique_n1\
#(df_gift,n1='CONSTITUENT_ID')
#if check:
#    print('number_of_gift_per_CID.head()',
#          number_of_gift_per_CID.head())
#    print('number_of_gift_per_CID.shape',
#          number_of_gift_per_CID.shape)
#gift_amount_per_CID=mean_n2_on_unique_n1\
#(df_gift,n1='CONSTITUENT_ID',n2='Gift_Amount')
#if check:      
#    print('gift_amount_per_CID.head()',
#          gift_amount_per_CID.head())
#    print('gift_amount_per_CID.shape',
#          gift_amount_per_CID.shape)
#gifts=pd.DataFrame(dict(number_of_gift =
#number_of_gift_per_CID , mean_gift = 
#gift_amount_per_CID)).reset_index()
#gifts=gifts.rename(columns={'CONSTITUENT_ID': 'CID'})
#gifts=gifts.rename(columns={'mean_gift': 'AVG_gift'})
#gifts=gifts.rename(columns={'number_of_gift': 'NUM_gift'})
#print('gifts.head()',gifts.head())
#print('gifts.shape',gifts.shape)   
#list_unique=gifts["CID"].unique()
#if check:
#    print(len(list_unique))  

