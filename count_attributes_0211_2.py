# -*- coding: utf-8 -*-
"""
Created on Wed Nov 02 18:51:11 2016

@author: francesco
 ADD COMMENTS ------code without comments ----
 ADD COMMENTS ------code without comments ----
 ADD COMMENTS ------code without comments ----
"""
import pandas as pd
import create_hist_mod as ch
database_name='raisers_edge_data.db'
list_dataframes={}
####------------gifts-------------------------------------
query=''' select CONSTITUENT_ID,Gift_Amount
from Analysis_Gift_Split_View ;'''
df_gift=ch.creating_dataframe_from_database_querying\
(query,database_name,return_shape=True)
#print('df_gift.head()',df_gift.head())
#print('df_gift.shape',df_gift.shape)
number_of_gift_per_CID=ch.count_unique_n1\
(df_gift,n1='CONSTITUENT_ID')
#print('number_of_gift_per_CID.head()',
#      number_of_gift_per_CID.head())
#print('number_of_gift_per_CID.shape',
#      number_of_gift_per_CID.shape)
gift_amount_per_CID=mean_n2_on_unique_n1\
(df_gift,n1='CONSTITUENT_ID',n2='Gift_Amount')      
#print('gift_amount_per_CID.head()',
#      gift_amount_per_CID.head())
#print('gift_amount_per_CID.shape',
#      gift_amount_per_CID.shape)
gifts=pd.DataFrame(dict(number_of_gift =
number_of_gift_per_CID , mean_gift = 
gift_amount_per_CID)).reset_index()
gifts=gifts.rename(columns={'CONSTITUENT_ID': 'CID'})
gifts=gifts.rename(columns={'mean_gift': 'AVG_gift'})
gifts=gifts.rename(columns={'number_of_gift': 'NUM_gift'})
print('gifts.head()',gifts.head())
print('gifts.shape',gifts.shape)   
list_unique=gifts["CID"].unique()
print(len(list_unique))  
list_dataframes['gifts']=gifts
print('--------------------------------------------------')
####-----------volunters---------------------------------
query=''' select CONSTITUENT_ID,Status_Desc,Volunteer_Type_Desc
from Analysis_Volunteer_Types_View   ;'''
df_volunter=ch.creating_dataframe_from_database_querying\
(query,database_name,return_shape=True)
df_volunter_active=df_volunter[df_volunter['Status_Desc']=='Active']
df_volunter_active=df_volunter_active.drop_duplicates(subset=['CONSTITUENT_ID'])
volunters_active=df_volunter_active.rename(columns={'CONSTITUENT_ID': 'CID'})
volunters_active=volunters_active.rename(columns={'Volunteer_Type_Desc': 'TYPE_VOL'})
del volunters_active['Status_Desc']
print('volunters_active.head()',volunters_active.head())
print('volunters_active.shape',volunters_active.shape)
list_unique_type=volunters_active["TYPE_VOL"].unique()
#print(list_unique_type)
list_aus=[]
for i,j in enumerate(list_unique_type):
    list_aus.append((j,i+1))
print(type(list_aus))
dict_aus = {key: value for (key, value) in list_aus}
volunters_active["TYPE_VOL"]=volunters_active["TYPE_VOL"].apply(lambda x : dict_aus[x])
print('volunters_active.head()',volunters_active.head())
print('volunters_active.shape',volunters_active.shape)
list_unique=volunters_active["CID"].unique()
print(len(list_unique)) 
list_dataframes['volunters_active']=volunters_active
print('--------------------------------------------------')
####--------------mosaic--------------------------------
import pickle
documents_f = open("Mosaic_dataframe.pickle", "rb") ####
df_mosaic = pickle.load(documents_f)
documents_f.close()
##print(df_mosaic.head())
df_mosaic_new=df_mosaic[['TC_Most Recent RE Constituent ID',
'TOD_Product Category','TC_Postcode']]
df_mosaic_new2=df_mosaic_new.dropna(subset=['TC_Most Recent RE Constituent ID'])
print(df_mosaic_new2.shape)
list_unique=df_mosaic_new2["TOD_Product Category"].unique()
print(list_unique)
tuple_aus=[]
def find_PK(x):
    if x=='Daily Living Aids' or x=='PUK Essentials':
        x=0
    else:
        x=1
    return x
df_mosaic_new2["TOD_Product Category"].fillna(0,inplace=True)
df_mosaic_new2["TOD_Product Category"]=df_mosaic_new2["TOD_Product Category"]\
.apply(lambda x: find_PK(x))
df_mosaic_new2["TC_Most Recent RE Constituent ID"]\
.apply(lambda x: int(x))
df_mosaic_new2=df_mosaic_new2.rename(columns={'TC_Most Recent RE Constituent ID': 'CID'})
#print(df_mosaic_new2.head(),df_mosaic_new2.shape)
products_PK=df_mosaic_new2[df_mosaic_new2['TOD_Product Category']==1]
products_PK['Events'] = products_PK.groupby('CID')['TOD_Product Category'].transform(pd.Series.value_counts)
products_PK=products_PK.drop_duplicates(subset=['CID'])
products_PK=products_PK.rename(columns={'TOD_Product Category': 'PRODUCT'})
products_PK=products_PK.rename(columns={'TC_Postcode': 'POST_C'})
products_PK=products_PK.rename(columns={'Events': 'NUM_P'})
print(products_PK.head(),products_PK.shape)
list_unique=products_PK["CID"].unique()
print(len(list_unique))
list_dataframes['products_PK']=products_PK
print('--------------------------------------------------')
###-----------Members---------------------------------
query=''' select CONSTITUENT_ID,Current_Standing
from Analysis_Membership_View   ;'''
df_member=ch.creating_dataframe_from_database_querying\
(query,database_name,return_shape=True)
df_member_active=df_member[df_member['Current_Standing']=='Active']
members_active=df_member_active.drop_duplicates(subset=['CONSTITUENT_ID'])
members_active=members_active.rename(columns={'CONSTITUENT_ID': 'CID'})
members_active=members_active.rename(columns={'Current_Standing': 'MEMB_STA'})
def f_string_to_num(x):
    if x=='Active':
        x=1
    return x
members_active['MEMB_STA']=members_active['MEMB_STA'].apply(lambda x: f_string_to_num(x))
print('df_member.head()',members_active.head())
print('df_member.shape',members_active.shape)
list_unique=members_active["CID"].unique()
print(len(list_unique))
list_dataframes['members_active']=members_active 
print('--------------------------------------------------')
##------------events-------------------------------------
query=''' select CONSTITUENT_ID,Participant_Has_Attended
from Analysis_Events_View  ;'''
df_event=ch.creating_dataframe_from_database_querying\
(query,database_name,return_shape=True)
df_event=df_event[df_event['Participant_Has_Attended']=='Yes']
print('df_event.head()',df_event.head())
print('df_event.shape',df_event.shape)
number_of_event_per_CID=ch.count_unique_n1\
(df_event,n1='CONSTITUENT_ID')
print('number_of_event_per_CID.head()',
      number_of_event_per_CID.head())
print('number_of_gift_per_CID.shape',
      number_of_event_per_CID.shape)
events=pd.DataFrame(dict(number_of_events =
number_of_event_per_CID )).reset_index()
events=events.rename(columns={'CONSTITUENT_ID': 'CID'})
events=events.rename(columns={'number_of_events': 'NUM_EV'})
print('events.head()',events.head())
print('events.shape',events.shape)    
list_unique=events["CID"].unique()
print(len(list_unique))       
list_dataframes['events']=events
#print('--------------------------------------------------')

#combined=list_dataframes['gifts']
#for i,j in enumerate(list_dataframes):
#    print(i,j,type(list_dataframes[j]))
#    if i>0:
#        combined = combined.merge(list_dataframes[j],\
#        on='CID', how="left")
#print(combined.shape)
#print(combined.head())
#-------------------------------------------------------------------------
poste_code_to_region = pd.read_csv("postcodeccg.csv",sep=",",\
encoding = "ISO-8859-1")
print(poste_code_to_region.shape)
print(poste_code_to_region.head())
#--------------------------------------------------------------------------
query=''' select CONSTITUENT_ID,POST_CODE
from Analysis_Address_View ;'''
df_address=ch.creating_dataframe_from_database_querying\
(query,database_name,return_shape=True)
print('df_address.head()',df_address.head())
print('df_address.shape',df_address.shape)
address=df_address
print(address.dtypes)
address=address.rename(columns={'CONSTITUENT_ID': 'CID'})
address['CID']=address['CID'].apply(lambda x: int(x))
print(address.dtypes)
address.drop_duplicates(subset='CID',inplace=True)
print('address.head()',address.head())
print('address.shape',address.shape)
list_unique=address["CID"].unique()
print(len(list_unique)) 
list_dataframes['address']=address
#-----------------------------------------------
save_list_dataframes=open("list_dataframes.pickle","wb")
pickle.dump(list_dataframes,save_list_dataframes)
save_list_dataframes.close()