import streamlit as st
from PIL import Image
from streamlit_option_menu import option_menu
import pandas as pd
import mysql.connector
import plotly.express as px

# -------------------------------------------------- Mysql server connection using sqlalchemy ---------------------------------------------
# --------------------------------------------------- Must install pymysql -----------------------------------------------------------------
def DBConnection():
    # Connect to the database
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        database='phonepe_pulse',
        port = 3306,
        password='Logpassword'
    )
    return connection


phn=Image.open('F:/DataScience_Projects/PhonePe_Visualization/images/phonepe-logo-icon.png')
phn1=Image.open('F:/DataScience_Projects/PhonePe_Visualization/images/phonepe.png')

st.set_page_config(page_title='PhonePe Pulse',page_icon=phn,layout='wide')
st.title(':violet[ PhonePe Pulse Data Visualization]')

# ---------------------------------------------------- Fetching datas from Mysql using pandas -----------------------------------------------
db = DBConnection()
cursor = db.cursor()

# Creating Options in app
#with st.sidebar:
SELECT = option_menu(
        menu_title = None,
        options = ["Home","Search"],
        icons =["house","search"],
        orientation="horizontal",
        styles={
            "container": {"padding": "0!important", "background-color": "white","size":"cover"},
            "icon": {"color": "black", "font-size": "20px"},
            "nav-link": {"font-size": "20px", "text-align": "left", "margin": "-1px", "--hover-color": "#7F36BC"},
            "nav-link-selected": {"background-color": "#7F36BC"}
        }

    )

if SELECT == "Home":
    st.text(
        "PhonePe  is an Indian digital payments and financial technology company headquartered in Bengaluru, Karnataka, India. \nPhonePe was founded in December 2015, by Sameer Nigam, Rahul Chari and Burzin Engineer. The PhonePe app, based on the \nUnified Payments Interface (UPI), went live in August 2016. It is owned by Flipkart, a subsidiary of Walmart.")
    col1,col2, = st.columns(2)
    with col1:
        st.image(phn)
    with col2:
        st.download_button("DOWNLOAD THE APP NOW", "https://www.phonepe.com/app-download/")

if SELECT =="Search":
    Topic = ["--select--","Brand", "Transactions"]
    choice_topic = st.selectbox("Search by",Topic)
#creating functions for query search in sqlite to get the data
    def type_(type):
        cursor.execute(f"SELECT DISTINCT state,Quater,Year,Transaction_type,Transaction_amount FROM aggregated_transaction WHERE Transaction_type = '{type}' ORDER BY state,Quater,Year");
        df = pd.DataFrame(cursor.fetchall(), columns=['state','Quater', 'Year', 'Transaction_type', 'Transaction_amount'])
        return df
    def type_year(year):
        cursor.execute(f"SELECT DISTINCT state,Year,Quater,Transacion_type,Transacion_amount FROM aggregated_transaction WHERE Year = '{year}' ORDER BY state ");
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'Year',"Quater", 'Transacion_type', 'Transacion_amount'])
        return df
    def type_state(state,year):
        cursor.execute(f"SELECT Transacion_type,sum(Transacion_amount) FROM aggregated_transaction WHERE state = '{state}' And Year = '{year}' GROUP BY Transacion_type");
        df = pd.DataFrame(cursor.fetchall(), columns=['Transacion_type', 'Transacion_amount'])
        return df
    def brand_(brand_type):
        cursor.execute(f"SELECT state,year,quater,brand,brand_percentage FROM aggregated_user WHERE brand='{brand_type}' ORDER BY state,year,quater DESC");
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'year','quater', 'brand', 'Percentage'])
        return df
    def brand_year(brand_type,year):
        cursor.execute(f"SELECT state,Year,Quater,brand,brand_Percentage FROM aggregated_user WHERE Year = '{year}' AND brand='{brand_type}' ORDER BY state,Year,Quater DESC");
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'Year',"Quater", 'brand', 'Percentage'])
        return df
    def brand_state(state,brand_type,year):
        cursor.execute(f"SELECT state,year,quater,brand,brand_percentage FROM aggregated_user WHERE state = '{state}' AND brand='{brand_type}' AND year = '{year}' ORDER BY state,year,quater DESC");
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'year',"quater", 'brand', 'percentage'])
        return df
    def registered_user_state(_state):
        cursor.execute(f"SELECT state,Year,Quater,District,RegisteredUser FROM map_user WHERE state = '{_state}' ORDER BY state,Year,Quater,District")
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'Year',"Quater", 'District', 'RegisteredUser'])
        return df
    def registered_user_year(_state,_year):
        cursor.execute(f"SELECT state,Year,Quater,District,RegisteredUser FROM map_user WHERE Year = '{_year}' AND state = '{_state}' ORDER BY state,Year,Quater,District")
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'Year',"Quater", 'District', 'RegisteredUser'])
        return df
    def registered_user_district(_state,_year,_dist):
        cursor.execute(f"SELECT state,Year,Quater,District,RegisteredUser FROM map_user WHERE Year = '{_year}' AND state = '{_state}' AND District = '{_dist}' ORDER BY state,Year,Quater,District")
        df = pd.DataFrame(cursor.fetchall(), columns=['state', 'Year',"Quater", 'District', 'RegisteredUser'])
        return df
    
    if choice_topic == "Transactions":
        col1, col2 = st.columns(2)
        with col1:
            st.text(" SELECT YEAR ")
            choice_year = st.selectbox("Year", ["", "2018", "2019", "2020", "2021", "2022"], 0)
            if choice_year:
                df = type_year(choice_year)                    
                fig = px.pie(df, values='Transacion_amount', names='Transacion_type', title=f" Plotly view of transactions in {choice_year} ")
                st.plotly_chart(fig, theme=None, use_container_width=True)
        with col2:
            st.text(" SELECT STATE ")
            menu_state = ['', 'andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh', 'assam', 'bihar', 'chandigarh', 'chhattisgarh','dadra-&-nagar-haveli-&-daman-&-diu', 
                              'delhi', 'goa', 'gujarat', 'haryana', 'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand', 'karnataka', 'kerala', 'ladakh','lakshadweep', 'madhya-pradesh', 
                              'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland', 'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim', 
                              'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal']                
            choice_state = st.selectbox("state", menu_state, 0)
            if choice_state and choice_year:
                df = type_state(choice_state, choice_year)
                fig = px.bar(df, x="Transacion_type", y="Transacion_amount",title=f" {choice_state} : Plotly view of transactions in {choice_year} ",color="Transacion_type")
                st.plotly_chart(fig, theme=None, use_container_width=True)

    if choice_topic == "Brand":
        col1, col2, col3 = st.columns(3)
        with col1:
            st.text(" SELECT BRAND ")
            mobiles =  ['', 'Apple', 'Asus', 'COOLPAD', 'Gionee', 'HMD Global', 'Huawei', 'Infinix', 'Lava', 'Lenovo', 
                           'Lyf', 'Micromax', 'Motorola', 'OnePlus', 'Oppo', 'Others', 'Realme', 'Samsung', 'Tecno', 'Vivo', 'Xiaomi']
            brand_type = st.selectbox("search by", mobiles, 0)
            if brand_type:
                df=brand_(brand_type)                    
                fig = px.bar(df, x="state", y="Percentage",title=f" {brand_type} Users ",color='state')
                st.plotly_chart(fig, theme=None, use_container_width=True)
        with col2:
            st.text(" SELECT YEAR")
            choice_year = st.selectbox("Year", ["", "2018", "2019", "2020", "2021", "2022"], 0)
            if brand_type and choice_year:
                df=brand_year(brand_type, choice_year)
                fig = px.bar(df, x="state", y="Percentage",title=f"{brand_type} Users in {choice_year}",color='Quater')
                st.plotly_chart(fig, theme=None, use_container_width=True)
        with col3:
            st.text(" SELECT STATE ")
            menu_state = ['', 'andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh', 'assam', 'bihar', 'chandigarh', 'chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu', 
                              'delhi', 'goa', 'gujarat', 'haryana', 'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh', 
                              'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland', 'odisha',
                              'puducherry', 'punjab', 'rajasthan', 'sikkim', 'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal']
            choice_state = st.selectbox("state", menu_state, 0)
            if brand_type and choice_state and choice_year:
                df=brand_state(choice_state, brand_type, choice_year)
                fig = px.bar(df, x="brand", y="year",title=f"{brand_type} Users in {choice_year} at {choice_state}")
                st.plotly_chart(fig, theme=None, use_container_width=True)
