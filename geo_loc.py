# -*- coding: utf-8 -*-
"""
Created on Fri Oct 28 10:57:52 2016

@author: francesco
"""

import sqlite3 
import pandas as pd
conn = sqlite3.connect("raisers_edge_data.db")
tables_name='''SELECT name FROM sqlite_master
WHERE type='table'; '''
print(conn.execute(tables_name).fetchall())
query='''select POST_CODE from Analysis_Address_View'''
df_address = pd.read_sql_query(query, conn)
print(df_address.shape)
#print(df_address.head(50))
post_code_geo=pd.read_excel('Postcode mapping - Oct 2016.xlsx')
post_code_dict={}
for i in post_code_geo['Postcode']:
    post_code_dict[i]=0
def split_post_code(x):
    x_aus=x.split(' ')[0]
    return x_aus
df_address['post_new']=df_address['POST_CODE'].apply(lambda x : split_post_code(x))
#print (df_address['post_new'].head(5))
for i in df_address['post_new']:
    try:
        post_code_dict[i]=post_code_dict[i]+1
#        print(i)
    except Exception:
#        print(i),'exception'
        pass
# Convert to dataframe
df = pd.DataFrame.from_dict(post_code_dict, orient='index').reset_index()
df.rename(columns={'index': 'post_code', 0: 'num_members'}, inplace=True)
df.sort_values('num_members',inplace=True,ascending=False)
print(df.head())
#------
query='''select Analysis_Membership_View.CONSTITUENT_ID 
from Analysis_Membership_View 
where Analysis_Membership_View.Current_Standing != 'Dropped' ; '''
df_membership = pd.read_sql_query(query, conn)
print(df_membership.shape)
print(df_membership.dtypes)
#print(df_membership.head(5))
#schema = conn.execute("pragma table_info(Analysis_Volunteer_Types_View);").fetchall()
#print(schema)
query='''select Analysis_Membership_View.CONSTITUENT_ID
from Analysis_Membership_View 
left join  Analysis_Address_View 
on Analysis_Membership_View.CONSTITUENT_ID
=Analysis_Address_View.CONSTITUENT_ID 
where Analysis_Membership_View.Current_Standing != 'Dropped' ; '''
df_membership = pd.read_sql_query(query, conn)
print(df_membership.shape)
print(df_membership.dtypes)

#
#
#'''query select a.CONSTITUENT_ID, a.IMPORT_ID, 
#a.Last_Name, b.City, b.Country_Name, c.Appeal_Date
# from Analysis_Contacts_View a INNER JOIN Analysis_Address_View 
# b ON a.CONSTITUENT_ID=b.CONSTITUENT_ID INNER JOIN Analysis_Appeals_View 
# c on c.CONSTITUENT_ID=b.CONSTITUENT_ID Limit 1000'''
#query='''select POST_CODE from Analysis_Address_View'''
#df_address = pd.read_sql_query(query, conn)