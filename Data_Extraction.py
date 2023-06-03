## Data Extraction

#Extract data from the Phonepe pulse Github repository through scripting and clone it..
#In command prompt, "git clone https://github.com/PhonePe/pulse.git"
#Finshed cloning Phonepe Pulse

## Data Transformation
#Required libraries for the program

import json
import os
#import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import pymysql

# Connect to the database
def DBConnection():
    
    host='localhost'
    user='root'
    db='phonepe_pulse'
    port = 3306
    password='Logpassword'
    engine = create_engine('mysql+mysqlconnector://root:Logpassword@localhost:3306/phonepe_pulse', echo=False)
    return engine


#This is to extract the data's to create a dataframe for Aggregated Transactions
path_Agg_Trans = "F:/DataScience_Projects/PhonePe_Visualization/pulse/data/aggregated/transaction/country/india/state/"
Agg_state_list=os.listdir(path_Agg_Trans)
clm_Agg_Trans={'State':[], 'Year':[],'Quater':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}
for i in Agg_state_list:
    p_i= path_Agg_Trans + i + "/"
    Agg_yr=os.listdir(p_i)    
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)        
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['transactionData']:
              Name=z['name']
              count=z['paymentInstruments'][0]['count']
              amount=z['paymentInstruments'][0]['amount']
              clm_Agg_Trans['Transacion_type'].append(Name)
              clm_Agg_Trans['Transacion_count'].append(count)
              clm_Agg_Trans['Transacion_amount'].append(amount)
              clm_Agg_Trans['State'].append(i)
              clm_Agg_Trans['Year'].append(j)
              clm_Agg_Trans['Quater'].append(int(k.strip('.json')))
#Succesfully created a dataframe
df_Agg_Trans = pd.DataFrame(clm_Agg_Trans)


#This is to extract the data's to create a dataframe for Aggregated Users
path_Agg_Users = "F:/DataScience_Projects/PhonePe_Visualization/pulse/data/aggregated/user/country/india/state/"
Agg_state_list=os.listdir(path_Agg_Users)
clm_Agg_Users={'State':[], 'Year':[],'Quater':[],'Brand':[], 'Brand_count':[], 'Brand_percentage':[]}
for i in Agg_state_list:
    p_i= path_Agg_Users + i + "/"
    Agg_yr=os.listdir(p_i)    
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)        
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            #print(p_k)
            D=json.load(Data)
            if(D['data']['usersByDevice'] != None):
                for z in D['data']['usersByDevice']:
                    brand=z['brand']
                    count=z['count']
                    percentage=z['percentage']
                    clm_Agg_Users['Brand'].append(brand)
                    clm_Agg_Users['Brand_count'].append(count)
                    clm_Agg_Users['Brand_percentage'].append(percentage)
                    clm_Agg_Users['State'].append(i)
                    clm_Agg_Users['Year'].append(j)
                    clm_Agg_Users['Quater'].append(int(k.strip('.json')))
df_Agg_User = pd.DataFrame(clm_Agg_Users)


path_Map_Trans = "F:/DataScience_Projects/PhonePe_Visualization/pulse/data/map/transaction/hover/country/india/state/"
#This is to extract the data's to create a dataframe for map Transactions
map_state_list=os.listdir(path_Map_Trans)
clm_Map_Trans={'State':[], 'Year':[],'Quater':[], 'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}
for i in map_state_list:
    p_i= path_Map_Trans + i + "/"
    Map_yr=os.listdir(p_i)    
    for j in Map_yr:
        p_j=p_i+j+"/"
        Map_yr_list=os.listdir(p_j)        
        for k in Map_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['hoverDataList']:
                    district = z['name']
                    transaction_type = z['metric'][0]['type']
                    transaction_count = z['metric'][0]['count']
                    transaction_amount = z['metric'][0]['amount']
                    clm_Map_Trans['Transaction_type'].append(transaction_type)
                    clm_Map_Trans['Transaction_count'].append(transaction_count)
                    clm_Map_Trans['Transaction_amount'].append(transaction_amount)
                    clm_Map_Trans['State'].append(i)
                    clm_Map_Trans['Year'].append(j)
                    clm_Map_Trans['Quater'].append(int(k.strip('.json')))
#Succesfully created a dataframe
df_Map_Trans = pd.DataFrame(clm_Map_Trans)


path_Map_Users = "F:/DataScience_Projects/PhonePe_Visualization/pulse/data/map/user/hover/country/india/state/"
#This is to extract the data's to create a dataframe for map Transactions
map_state_list=os.listdir(path_Map_Users)
clm_Map_User={'State':[], 'Year':[], 'Quater':[], 'District':[], 'Registered_user':[], 'App_opening' :[]}
for i in map_state_list:
    p_i= path_Map_Users + i + "/"
    Map_yr=os.listdir(p_i)    
    for j in Map_yr:
        p_j=p_i+j+"/"
        Map_yr_list=os.listdir(p_j)        
        for k in Map_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['hoverData']:
                    district = z
                    registered_user =  D['data']['hoverData'][z]['registeredUsers']
                    app_opening = D['data']['hoverData'][z]['appOpens']
                    clm_Map_User['District'].append(district)
                    clm_Map_User['Registered_user'].append(registered_user)
                    clm_Map_User['App_opening'].append(app_opening)
                    clm_Map_User['State'].append(i)
                    clm_Map_User['Year'].append(j)
                    clm_Map_User['Quater'].append(int(k.strip('.json')))
#Succesfully created a dataframe
df_Map_User = pd.DataFrame(clm_Map_User)


#This is to extract the data's to create a dataframe for Top Transactions
path_Top_Trans = "F:/DataScience_Projects/PhonePe_Visualization/pulse/data/top/transaction/country/india/state/"
Top_state_list=os.listdir(path_Top_Trans)
clm_Top_Trans={'State':[], 'Year':[],'Quater':[], 'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}
for i in Top_state_list:
    p_i= path_Top_Trans + i + "/"
    Top_yr=os.listdir(p_i)    
    for j in Top_yr:
        p_j=p_i+j+"/"
        Top_yr_list=os.listdir(p_j)        
        for k in Top_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['districts']:
                    transaction_type = z['metric']['type']
                    transaction_count = z['metric']['count']
                    transaction_amount = z['metric']['amount']
                    clm_Top_Trans['Transaction_type'].append(transaction_type)
                    clm_Top_Trans['Transaction_count'].append(transaction_count)
                    clm_Top_Trans['Transaction_amount'].append(transaction_amount)
                    clm_Top_Trans['State'].append(i)
                    clm_Top_Trans['Year'].append(j)
                    clm_Top_Trans['Quater'].append(int(k.strip('.json')))
#Succesfully created a dataframe
df_Top_Trans = pd.DataFrame(clm_Top_Trans)

path_Top_Users = "F:/DataScience_Projects/PhonePe_Visualization/pulse/data/top/user/country/india/state/"
#This is to extract the data's to create a dataframe for map Transactions
top_state_list=os.listdir(path_Top_Users)
clm_Top_User={'State':[], 'Year':[], 'Quater':[], 'District':[], 'Registered_user':[]}
for i in top_state_list:
    p_i= path_Top_Users + i + "/"
    Top_yr=os.listdir(p_i)    
    for j in Top_yr:
        p_j=p_i+j+"/"
        Top_yr_list=os.listdir(p_j)        
        for k in Top_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['districts']:
                    registered_user = [z][0]['registeredUsers']
                    district = [z][0]['name']
                    clm_Top_User['District'].append(district)
                    clm_Top_User['Registered_user'].append(registered_user)
                    #clm_Top_User['App_opening'].append(app_opening)
                    clm_Top_User['State'].append(i)
                    clm_Top_User['Year'].append(j)
                    clm_Top_User['Quater'].append(int(k.strip('.json')))
#Succesfully created a dataframe
df_Top_User = pd.DataFrame(clm_Top_User)


print(df_Agg_Trans.head())
print(df_Map_Trans.head())
print(df_Top_Trans.head())
print(df_Agg_User.head())
print(df_Map_User.head())
print(df_Top_User.head())


#DATA CLEANING
#inserting data into MYSQL database
#Connecting to Database
conn = DBConnection()
print(conn)

#Taking back up of Dataframe
df_Agg_Trans_copy = df_Agg_Trans.copy()
df_Agg_User_copy = df_Agg_User.copy()
df_Map_Trans_copy = df_Map_Trans.copy()
df_Map_User_copy= df_Map_User.copy()
df_Top_Trans_copy = df_Top_Trans.copy()
df_Top_User_copy =df_Top_User.copy()

#Writting dataframe to MYSQL tables
try:
    df_Agg_Trans.to_sql(name='aggregated_transaction',con=conn,index=True, if_exists='replace')
    df_Agg_User.to_sql(name='aggregated_user',con=conn,index=True, if_exists='replace')
    df_Map_Trans.to_sql(name='map_transaction',con=conn,index=True, if_exists='replace') 
    df_Map_User.to_sql(name='map_user',con=conn,index=True, if_exists='replace') 
    df_Top_Trans.to_sql(name='top_transaction',con=conn,index=True, if_exists='replace')  
    df_Top_User.to_sql(name='top_user',con=conn,index=True, if_exists='replace')
    print('Sucessfully written to phonepe_pulse Database!!!')

except Exception as e:
    print(e)