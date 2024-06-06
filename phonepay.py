import streamlit as st
from streamlit_option_menu import option_menu
import PIL
from PIL import Image
import os
import json
import pandas as pd
import numpy as np
import requests
# [SQL libraries]
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
import pymysql
import plotly.express as px


#**Connection to SQL**

connect=pymysql.connect(host="localhost",
                                user="root",
                                password="",
                                database="phonepe_pulse_data",
                                port=3306)
cursor=connect.cursor()
cursor.execute('CREATE DATABASE IF NOT EXISTS phonepe_pulse_data')
cursor.close()
connect.close()

engine = create_engine("mysql+pymysql://root:@localhost:3306/phonepe_pulse_data")
connection = engine.connect()

conn = pymysql.connect(host='localhost', user='root', password='', database='phonepe_pulse_data')
cursor = conn.cursor()


#insurance trans
path1 = (r"C:/Users/hp/Desktop/phonepay/pulse\data/aggregated/insurance/country/india\state/")
insurancetranalist=os.listdir(path1)

columns1={"States":[],"Years":[],"Quarter":[],"Transaction_name":[],"Transaction_count":[],"Transaction_amount":[]}

for state in insurancetranalist:
    currentstates=path1+state+"/"
    insuranceyearlist=os.listdir(currentstates) 

    for year in insuranceyearlist:
        currentyear=currentstates+year+"/"
        insurancefilelist=os.listdir(currentyear) 

        for file in insurancefilelist:
            currentfile=currentyear+file
            data=open(currentfile,'r')

            A1=json.load(data)
            for i in A1['data']['transactionData']:
                name=i['name']
                count=i["paymentInstruments"][0]["count"]
                amount=i["paymentInstruments"][0]["amount"]
                columns1['Transaction_name'].append(name)
                columns1['Transaction_count'].append(count)
                columns1['Transaction_amount'].append(amount)
                columns1['States'].append(state)
                columns1['Years'].append(year)
                columns1['Quarter'].append(int(file.strip('.json')))
AGG_INSURANCETRANSACTION=pd.DataFrame(columns1)


#agg_trans

path2=(r"C:/Users/hp/Desktop/phonepay/pulse/data/aggregated/transaction/country/india\state/")
aggregatetranslist=os.listdir(path2)

columns2={"States":[],"Years":[],"Quarter":[],"Transaction_name":[],"Transaction_count":[],"Transaction_amount":[]}

for state in aggregatetranslist:
    currentstates=path2+state+"/"
    aggregateyearlist=os.listdir(currentstates) 

    for year in aggregateyearlist :
        currentyear=currentstates+year+"/"
        aggregatefilelist=os.listdir(currentyear) 

        for file in aggregatefilelist:
            currentfile=currentyear+file
            data=open(currentfile,'r')

            A2=json.load(data)
            for i in A2['data']['transactionData']:
                name=i['name']
                count=i["paymentInstruments"][0]["count"]
                amount=i["paymentInstruments"][0]["amount"]
                columns2['Transaction_name'].append(name)
                columns2['Transaction_count'].append(count)
                columns2['Transaction_amount'].append(amount)
                columns2['States'].append(state)
                columns2['Years'].append(year)
                columns2['Quarter'].append(int(file.strip('.json')))

AGGREGATE_TRANSACTION_DATA=pd.DataFrame(columns2)

#aggregate_user
path3=(r"C:/Users/hp\Desktop/phonepay/pulse/data/aggregated/user/country/india/state/")
aggregateuserlist=os.listdir(path3)

columns3={"States":[],"Years":[],"Quarter":[],"Brands":[],"Transaction_count":[],"Percentage":[]}
for state in aggregateuserlist:
    currentstates=path3+state+"/"
    aggregateyearlist=os.listdir(currentstates) 

    for year in aggregateyearlist :
        currentyear=currentstates+year+"/"
        aggregatefilelist=os.listdir(currentyear) 

        for file in aggregatefilelist:
            currentfile=currentyear+file
            data=open(currentfile,'r')

            A3=json.load(data)
            try:
                for i in A3['data']['usersByDevice']:
                    brand=i['brand']
                    count=i["count"]
                    percentage=i["percentage"]
                    columns3['Brands'].append(brand)
                    columns3['Transaction_count'].append(count)
                    columns3['Percentage'].append(percentage * 100)
                    columns3['States'].append(state)
                    columns3['Years'].append(year)
                    columns3['Quarter'].append(int(file.strip('.json')))
            except:
                pass

AGGREGATE_USER_DATA=pd.DataFrame(columns3)

#mapfolderinsurancehover
path4=(r"C:/Users/hp/Desktop/phonepay/pulse/data/map/insurance/hover/country/india/state/")
maphoverlist=os.listdir(path4)

columns4={"States":[],"Years":[],"Quarter":[],"Transaction_name":[],"Transaction_count":[],"Transaction_amount":[]}

for state in maphoverlist:
    currentstates=path4+state+"/"
    insuranceyearlist=os.listdir(currentstates) 

    for year in insuranceyearlist :
        currentyear=currentstates+year+"/"
        insurancefilelist=os.listdir(currentyear) 

        for file in insurancefilelist:
            currentfile=currentyear+file
            data=open(currentfile,'r')

            A5=json.load(data)
            for i in A5['data']['hoverDataList']:
                name=i['name']
                count=i["metric"][0]["count"]
                amount=i["metric"][0]["amount"]
                columns4['Transaction_name'].append(name)
                columns4['Transaction_count'].append(count)
                columns4['Transaction_amount'].append(amount)
                columns4['States'].append(state)
                columns4['Years'].append(year)
                columns4['Quarter'].append(int(file.strip('.json')))

MAP_INSURANCE_HOVER_DATA=pd.DataFrame(columns4)

#maptransaction
path5=(r"C:/Users/hp/Desktop/phonepay/pulse/data/map/transaction/hover/country/india/state/")
maptransactionlist=os.listdir(path5)


columns5={"States":[],"Years":[],"Quarter":[],"District":[],"Transaction_count":[],"Transaction_amount":[]}
for state in maptransactionlist:
    currentstates=path5+state+"/"
    maptransactionyearlist=os.listdir(currentstates) 

    for year in insuranceyearlist :
        currentyear=currentstates+year+"/"
        maptranasctionfilelist=os.listdir(currentyear) 

        for file in insurancefilelist:
            currentfile=currentyear+file
            data=open(currentfile,'r')

            A6=json.load(data)
            for i in A6['data']['hoverDataList']:
                District=i['name']
                count=i["metric"][0]["count"]
                amount=i["metric"][0]["amount"]
                columns5['District'].append(District)
                columns5['Transaction_count'].append(count)
                columns5['Transaction_amount'].append(amount)
                columns5['States'].append(state)
                columns5['Years'].append(year)
                columns5['Quarter'].append(int(file.strip('.json')))


MAP_TRANSACTION_DATA=pd.DataFrame(columns5)

#mapuser
path6=(r"C:/Users/hp/Desktop/phonepay/pulse/data/map/user/hover/country/india/state/")
maphoveruserlist=os.listdir(path6)

columns6={"States":[],"Years":[],"Quarter":[],"Districts":[],"RegisteredUser":[],"AppOpens":[]}
for state in maphoveruserlist:
    currentstates=path6+state+"/"
    maphoveryearlist=os.listdir(currentstates) 

    for year in maphoveryearlist :
        currentyear=currentstates+year+"/"
        maphoverfilelist=os.listdir(currentyear) 

        for file in maphoverfilelist:
            currentfile=currentyear+file
            data=open(currentfile,'r')

            A7=json.load(data)
        
            for i in A7["data"]["hoverData"].items():
                district = i[0]
                registereduser = i[1]["registeredUsers"]
                appopens = i[1]["appOpens"]
                columns6["Districts"].append(district)
                columns6["RegisteredUser"].append(registereduser)
                columns6["AppOpens"].append(appopens)
                columns6["States"].append(state)
                columns6["Years"].append(year)
                columns6["Quarter"].append(int(file.strip(".json")))
            

MAP_USER_DATA=pd.DataFrame(columns6)

#topinsurance
path7=(r"C:/Users/hp/Desktop/phonepay/pulse/data/top/insurance/country/india\state/")
topinsurancelist=os.listdir(path7)

columns7 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}

for state in topinsurancelist:
    currentstates=path7+state+"/"
    topinsuranceyearlist=os.listdir(currentstates) 

    for year in topinsuranceyearlist :
        currentyear=currentstates+year+"/"
        topinsurancefilelist=os.listdir(currentyear) 

        for file in topinsurancefilelist:
            currentfile=currentyear+file
            data=open(currentfile,'r')

            A8=json.load(data)
            for i in A8["data"]["pincodes"]:
                entityName = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                columns7["Pincodes"].append(entityName)
                columns7["Transaction_count"].append(count)
                columns7["Transaction_amount"].append(amount)
                columns7["States"].append(state)
                columns7["Years"].append(year)
                columns7["Quarter"].append(int(file.strip(".json")))

TOP_INSURANCE_DATA=pd.DataFrame(columns7)

#toptransaction
path8=(r"C:/Users/hp/Desktop/phonepay/pulse/data/top/transaction/country/india/state/")
toptransactionlist=os.listdir(path8)

columns8 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}

for state in toptransactionlist:
    currentstates=path8+state+"/"
    toptransactionyearlist=os.listdir(currentstates) 

    for year in toptransactionyearlist :
        currentyear=currentstates+year+"/"
        toptransactionfilelist=os.listdir(currentyear) 

        for file in toptransactionfilelist:
            currentfile=currentyear+file
            data=open(currentfile,'r')

            A9=json.load(data)
            for i in A9["data"]["pincodes"]:
                entityName = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                columns8["Pincodes"].append(entityName)
                columns8["Transaction_count"].append(count)
                columns8["Transaction_amount"].append(amount)
                columns8["States"].append(state)
                columns8["Years"].append(year)
                columns8["Quarter"].append(int(file.strip(".json")))

TOP_TRANSACTION_DATA=pd.DataFrame(columns8)

#topuser

path9=(r"C:/Users/hp/Desktop/phonepay/pulse/data/top/user/country/india/state/")
topuserlist=os.listdir(path9)

columns9 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "RegisteredUsers":[]}
for state in topuserlist:
    currentstates=path9+state+"/"
    topuseryearlist=os.listdir(currentstates) 

    for year in topuseryearlist :
        currentyear=currentstates+year+"/"
        topuserfilelist=os.listdir(currentyear) 

        for file in topuserfilelist:
            currentfile=currentyear+file
            data=open(currentfile,'r')

            A10=json.load(data)
        
            for i in A10["data"]["pincodes"]:
                name = i["name"]
                registeredusers = i["registeredUsers"]
                columns9["Pincodes"].append(name)
                columns9["RegisteredUsers"].append(registeredusers)
                columns9["States"].append(state)
                columns9["Years"].append(year)
                columns9["Quarter"].append(int(file.strip(".json")))

TOP_USER_DATA=pd.DataFrame(columns9)

#**Connection to SQL**

connect=pymysql.connect(host="localhost",
                                user="root",
                                password="",
                                database="phonepe_pulse_data",
                                port=3306)
cursor=connect.cursor()
cursor.execute('CREATE DATABASE IF NOT EXISTS phonepe_pulse_data')
cursor.close()
connect.close()

engine = create_engine("mysql+pymysql://root:@localhost:3306/phonepe_pulse_data")
connection = engine.connect()

#1
AGG_INSURANCETRANSACTION.to_sql(name="aggregated_insurance", con=engine,schema="phonepe_pulse_data",if_exists = 'replace', index=False,
                                 dtype={'States': sqlalchemy.types.VARCHAR(length=100),
                                       'Years': sqlalchemy.types.Integer,
                                       'Quarter': sqlalchemy.types.Integer,
                                       'Transaction_name': sqlalchemy.types.VARCHAR(length=100),
                                       'Transaction_count': sqlalchemy.types.Integer,
                                       'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

#2
AGGREGATE_TRANSACTION_DATA.to_sql(name="aggregated_transaction", con=engine,schema="phonepe_pulse_data",if_exists = 'replace', index=False,
                                 dtype={'States': sqlalchemy.types.VARCHAR(length=100),
                                       'Years': sqlalchemy.types.Integer,
                                       'Quarter': sqlalchemy.types.Integer,
                                       'Transaction_name': sqlalchemy.types.VARCHAR(length=100),
                                       'Transaction_count': sqlalchemy.types.Integer,
                                       'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

#3
AGGREGATE_USER_DATA.to_sql(name="aggregated_user", con=engine,schema="phonepe_pulse_data",if_exists = 'replace', index=False,
                     dtype={'States': sqlalchemy.types.VARCHAR(length=50),
                            'Years': sqlalchemy.types.Integer,
                            'Quarter': sqlalchemy.types.Integer,
                            'Brands': sqlalchemy.types.VARCHAR(length=50),
                            'Transaction_count': sqlalchemy.types.Integer,
                            'Percentage': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

#4
MAP_INSURANCE_HOVER_DATA.to_sql(name="map_insurance", con=engine,schema="phonepe_pulse_data",if_exists = 'replace', index=False,
                     dtype={'States': sqlalchemy.types.VARCHAR(length=100),
                            'Years': sqlalchemy.types.Integer,
                            'Quarter': sqlalchemy.types.Integer,
                            'Transaction_name': sqlalchemy.types.VARCHAR(length=100),
                            'Transaction_count': sqlalchemy.types.Integer,
                            'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})


#5
MAP_TRANSACTION_DATA.to_sql(name="map_transaction", con=engine,schema="phonepe_pulse_data",if_exists = 'replace', index=False,
                          dtype={'States': sqlalchemy.types.VARCHAR(length=100),
                            'Years': sqlalchemy.types.Integer,
                            'Quarter': sqlalchemy.types.Integer,
                            'Transaction_name': sqlalchemy.types.VARCHAR(length=100),
                            'Transaction_count': sqlalchemy.types.Integer,
                            'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})


#6
MAP_USER_DATA.to_sql(name="map_user", con=engine,schema="phonepe_pulse_data",if_exists = 'replace', index=False,
                    dtype={'States': sqlalchemy.types.VARCHAR(length=100),
                            'Years': sqlalchemy.types.Integer,
                            'Quarter': sqlalchemy.types.Integer,
                            "Districts": sqlalchemy.types.VARCHAR(length=100),
                            "RegisteredUser":sqlalchemy.types.Integer,
                            "AppOpens":sqlalchemy.types.Integer})

#7
TOP_INSURANCE_DATA.to_sql(name="top_insurance", con=engine,schema="phonepe_pulse_data",if_exists = 'replace', index=False,
                        dtype={'States': sqlalchemy.types.VARCHAR(length=100),
                            'Years': sqlalchemy.types.Integer,
                            'Quarter': sqlalchemy.types.Integer,
                            "Pincodes": sqlalchemy.types.Integer,
                            "Transaction_count":sqlalchemy.types.Integer,
                            "Transaction_amount":sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

#8
TOP_TRANSACTION_DATA.to_sql(name="top_transaction", con=engine,schema="phonepe_pulse_data",if_exists = 'replace', index=False,
                        dtype={'States': sqlalchemy.types.VARCHAR(length=100),
                            'Years': sqlalchemy.types.Integer,
                            'Quarter': sqlalchemy.types.Integer,
                            "Pincodes": sqlalchemy.types.Integer,
                            "Transaction_count":sqlalchemy.types.Integer,
                            "Transaction_amount":sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})     


#9
TOP_USER_DATA.to_sql(name="top_user", con=engine,schema="phonepe_pulse_data",if_exists = 'replace', index=False,
                        dtype={'States': sqlalchemy.types.VARCHAR(length=250),
                            'Years': sqlalchemy.types.Integer,
                            'Quarter': sqlalchemy.types.Integer,
                            "Pincodes": sqlalchemy.types.Integer,
                            "RegisteredUsers":sqlalchemy.types.Integer})


engine = create_engine("mysql+pymysql://root:@localhost:3306/phonepe_pulse_data")
connection = engine.connect()


# ============================================       /     STREAMLIT DASHBOARD      /       ================================================= #

# Configuring Streamlit GUI

st.set_page_config(layout="wide")

selected = option_menu(None,
                       options = ["About","Home","Analysis","Insights",],
                       icons = ["at","house","toggles","bar-chart"],
                       default_index=0,
                       orientation="horizontal",
                       styles={"container": {"width": "100%"},
                               "icon": {"color": "white", "font-size": "24px"},
                               "nav-link": {"font-size": "24px", "text-align": "center", "margin": "-2px"},
                               "nav-link-selected": {"background-color": "#6F36AD"}})


# ABOUT TAB
if selected == "About":
    col1, col2, = st.columns(2)
    
    col1.image(Image.open(r"C:\Users\hp\Desktop\phonepay\images.jpg"), width=500)
    with col1:
        st.subheader(
            "PhonePe  is an Indian digital payments and financial technology company headquartered in Bengaluru, Karnataka, India. PhonePe was founded in December 2015, by Sameer Nigam, Rahul Chari and Burzin Engineer. The PhonePe app, based on the Unified Payments Interface (UPI), went live in August 2016. It is owned by Flipkart, a subsidiary of Walmart.")
        st.markdown("[DOWNLOAD APP](https://www.phonepe.com/app-download/)")

    with col2:
        st.image(Image.open(r"C:\Users\hp\Desktop\phonepay\download.jpg"),width=500)



# HOME TAB
if selected == "Home":
    col1,col2 = st.columns(2)
    with col1:
        st.image(Image.open(r"C:\Users\hp\Desktop\phonepay\images (2).jpg"), width=700)
    with col2:
        st.title(':violet[PHONEPE PULSE DATA VISUALISATION]')
        st.subheader(':violet[Phonepe Pulse]:')
        st.write('PhonePe Pulse is a feature offered by the Indian digital payments platform called PhonePe.PhonePe Pulse provides users with insights and trends related to their digital transactions and usage patterns on the PhonePe app.')
        st.subheader(':violet[Phonepe Pulse Data Visualisation]:')
        st.write('Data visualization refers to the graphical representation of data using charts, graphs, and other visual elements to facilitate understanding and analysis in a visually appealing manner.'
                 'The goal is to extract this data and process it to obtain insights and information that can be visualized in a user-friendly manner.')
        st.markdown("## :violet[Done by] : Vinitha velu")
        st.markdown("[Inspired from](https://www.phonepe.com/pulse/)")
        st.markdown("[Githublink](https://github.com/vinitha3699)")
        st.markdown("[LinkedIn](https://www.linkedin.com/in/vinitha-velu-bb28b2222/)")
    st.write("---")


    # ANALYSIS TAB
if selected == "Analysis":
    st.title(':violet[ANALYSIS]')
    st.subheader('Analysis done on the basis of All India ,States and Top categories between 2018 and 2023')
    select = option_menu(None,
                         options=["INDIA", "STATES", "TOP CATEGORIES" ],
                         default_index=0,
                         orientation="horizontal",
                         styles={"container": {"width": "100%"},
                                   "nav-link": {"font-size": "20px", "text-align": "center", "margin": "-2px"},
                                   "nav-link-selected": {"background-color": "#6F36AD"}})
    conn = pymysql.connect(host='localhost', user='root', password='', database='phonepe_pulse_data')
    cursor = conn.cursor()

    if select == "INDIA":
        tab1, tab2,tab3 = st.tabs(["INSURANCE","TRANSACTION","USER"])

        # INSURANCE TAB
        with tab1:
            col1, col2= st.columns(2)
            with col1:
                insur_yr = st.selectbox('**Select Year**', ('2021', '2022','2023','2020'), key='insur_yr')
            with col2:
                insur_qtr = st.selectbox('**Select Quarter**', ('1', '2', '3', '4'), key='insur_qtr')
        

            # SQL Query
            conn = pymysql.connect(host='localhost', user='root', password='', database='phonepe_pulse_data')
            cursor = conn.cursor()


            # Insurance Analysis bar chart query
            cursor.execute(
                f"SELECT States, Transaction_amount FROM aggregated_insurance WHERE Years = '{insur_yr}' AND Quarter = '{insur_qtr}';")
            insur_tab_query_result = cursor.fetchall()
            df_insur_tab_query_result = pd.DataFrame(insur_tab_query_result, columns=['States', 'Transaction_amount'])
            df_insur_tab_query_result1 = df_insur_tab_query_result.set_index(pd.Index(range(1, len(df_insur_tab_query_result) + 1)))

            
            # Insurance Analysis table query
            cursor.execute(
                f"SELECT States, Transaction_count, Transaction_amount FROM aggregated_insurance WHERE Years = '{insur_yr}' AND Quarter = '{insur_qtr}';")
            insur_analysis_tab_query_result = cursor.fetchall()
            df_insur_analysis_tab_query_result = pd.DataFrame(insur_analysis_tab_query_result,
                                                      columns=['States', 'Transaction_count', 'Transaction_amount'])
            df_insur_analysis_tab_query_result1 = df_insur_analysis_tab_query_result.set_index(
                pd.Index(range(1, len(df_insur_analysis_tab_query_result) + 1)))


            # Total Transaction Amount table query
            cursor.execute(
                f"SELECT SUM(Transaction_amount), AVG(Transaction_amount) FROM aggregated_insurance WHERE Years = '{insur_yr}' AND Quarter = '{insur_qtr}';")
            insur_trans_amt_qry_rslt = cursor.fetchall()
            df_insur_trans_amt_qry_rslt = pd.DataFrame(insur_trans_amt_qry_rslt, columns=['Total', 'Average'])

            # Total Transaction Count table query
            cursor.execute(
                f"SELECT SUM(Transaction_count), AVG(Transaction_count) FROM aggregated_insurance WHERE Years = '{insur_yr}' AND Quarter = '{insur_qtr}';")
            insur_trans_count_qry_rslt = cursor.fetchall()
            df_insur_trans_count_qry_rslt = pd.DataFrame(insur_trans_count_qry_rslt, columns=['Total', 'Average'])

            # GEO VISUALISATION
            # Drop a State column from df_in_tr_tab_qry_rslt
            df_insur_tab_query_result.drop(columns=['States'], inplace=True)

            # Clone the gio data
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data1 = json.loads(response.content)

            # Extract state names and sort them in alphabetical order
            state_names_tra = [feature['properties']['ST_NM'] for feature in data1['features']]
            state_names_tra.sort()

            # Create a DataFrame with the state names column
            df_state_names_tra = pd.DataFrame({'States': state_names_tra})

            # Combine the Gio State name with df_in_tr_tab_qry_rslt
            df_state_names_tra['Transaction_amount'] = df_insur_tab_query_result

            # convert dataframe to csv file
            df_state_names_tra.to_csv('State_trans.csv', index=False)

            # Read csv
            df_tra = pd.read_csv('State_trans.csv')

            # Geo plot
            fig_tra = px.choropleth(
                df_tra,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM', locations='States', color='Transaction_amount',
                color_continuous_scale='thermal', title='Transaction Analysis')
            fig_tra.update_geos(fitbounds="locations", visible=False)
            fig_tra.update_layout(title_font=dict(size=33), title_font_color='#AD71EF', height=800)
            st.plotly_chart(fig_tra, use_container_width=True)

            # ---------   /   All India Transaction Analysis Bar chart  /  ----- #
            df_insur_tab_query_result1['States'] = df_insur_tab_query_result1['States'].astype(str)
            df_insur_tab_query_result1['Transaction_amount'] = df_insur_tab_query_result1['Transaction_amount'].astype(float)
            df_insur_trans_tab_query_rslt1_fig = px.bar(df_insur_tab_query_result1, x='States', y='Transaction_amount',
                                                color='Transaction_amount', color_continuous_scale='thermal',
                                                title='Transaction Analysis Chart', height=700, )
            df_insur_trans_tab_query_rslt1_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
            st.plotly_chart(df_insur_trans_tab_query_rslt1_fig, use_container_width=True)

            # -------  /  All India Total Transaction calculation Table   /   ----  #
            st.header(':violet[Total calculation]')

            col4, col5 = st.columns(2)
            with col4:
                st.subheader(':violet[Transaction Analysis]')
                st.dataframe(df_insur_analysis_tab_query_result)
            with col5:
                st.subheader(':violet[Transaction Amount]')
                st.dataframe(df_insur_trans_amt_qry_rslt)
                st.subheader(':violet[Transaction Count]')
                st.dataframe(df_insur_trans_count_qry_rslt)


        # TRANSACTION TAB
        with tab2:
            col1, col2, col3 = st.columns(3)
            with col1:
                in_tr_yr = st.selectbox('**Select Year**', ('2018', '2019', '2020', '2021', '2022','2023'), key='in_tr_yr')
            with col2:
                in_tr_qtr = st.selectbox('**Select Quarter**', ('1', '2', '3', '4'), key='in_tr_qtr')
            with col3:
                in_tr_tr_typ = st.selectbox('**Select Transaction type**',
                                            ('Recharge & bill payments', 'Peer-to-peer payments',
                                             'Merchant payments', 'Financial Services', 'Others'), key='in_tr_tr_typ')
            # SQL Query

            # Transaction Analysis bar chart query
            cursor.execute(
                f"SELECT States, Transaction_amount FROM aggregated_transaction WHERE Years= '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transaction_name = '{in_tr_tr_typ}';")
            in_tr_tab_qry_rslt = cursor.fetchall()
            df_in_tr_tab_qry_rslt = pd.DataFrame(np.array(in_tr_tab_qry_rslt), columns=['States', 'Transaction_amount'])
            df_in_tr_tab_qry_rslt1 = df_in_tr_tab_qry_rslt.set_index(pd.Index(range(1, len(df_in_tr_tab_qry_rslt) + 1)))

            # Transaction Analysis table query
            cursor.execute(
                f"SELECT States, Transaction_count, Transaction_amount FROM aggregated_transaction WHERE Years = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transaction_name = '{in_tr_tr_typ}';")
            in_tr_anly_tab_qry_rslt = cursor.fetchall()
            df_in_tr_anly_tab_qry_rslt = pd.DataFrame(np.array(in_tr_anly_tab_qry_rslt),
                                                      columns=['States', 'Transaction_count', 'Transaction_amount'])
            df_in_tr_anly_tab_qry_rslt1 = df_in_tr_anly_tab_qry_rslt.set_index(
                pd.Index(range(1, len(df_in_tr_anly_tab_qry_rslt) + 1)))

            # Total Transaction Amount table query
            cursor.execute(
                f"SELECT SUM(Transaction_amount), AVG(Transaction_amount) FROM aggregated_transaction WHERE Years = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transaction_name = '{in_tr_tr_typ}';")
            in_tr_am_qry_rslt = cursor.fetchall()
            df_in_tr_am_qry_rslt = pd.DataFrame(np.array(in_tr_am_qry_rslt), columns=['Total', 'Average'])
            

            # Total Transaction Count table query
            cursor.execute(
                f"SELECT SUM(Transaction_count), AVG(Transaction_count) FROM aggregated_transaction WHERE Years = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transaction_name = '{in_tr_tr_typ}';")
            in_tr_co_qry_rslt = cursor.fetchall()
            df_in_tr_co_qry_rslt = pd.DataFrame(np.array(in_tr_co_qry_rslt), columns=['Total', 'Average'])
            

            # GEO VISUALISATION
            # Drop a State column from df_in_tr_tab_qry_rslt
            df_in_tr_tab_qry_rslt.drop(columns=['States'], inplace=True)
            # Clone the gio data
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data1 = json.loads(response.content)
            # Extract state names and sort them in alphabetical order
            state_names_tra = [feature['properties']['ST_NM'] for feature in data1['features']]
            state_names_tra.sort()
            # Create a DataFrame with the state names column
            df_state_names_tra = pd.DataFrame({'States': state_names_tra})
            # Combine the Gio State name with df_in_tr_tab_qry_rslt
            df_state_names_tra['Transaction_amount'] = df_in_tr_tab_qry_rslt
            # convert dataframe to csv file
            df_state_names_tra.to_csv('State_trans.csv', index=False)
            # Read csv
            df_tra = pd.read_csv('State_trans.csv')
            # Geo plot
            fig_tra = px.choropleth(
                df_tra,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM', locations='States', color='Transaction_amount',
                color_continuous_scale='thermal', title='Transaction Analysis')
            fig_tra.update_geos(fitbounds="locations", visible=False)
            fig_tra.update_layout(title_font=dict(size=33), title_font_color='#AD71EF', height=800)
            st.plotly_chart(fig_tra, use_container_width=True)

            # ---------   /   All India Transaction Analysis Bar chart  /  ----- #
            df_in_tr_tab_qry_rslt1['States'] = df_in_tr_tab_qry_rslt1['States'].astype(str)
            df_in_tr_tab_qry_rslt1['Transaction_amount'] = df_in_tr_tab_qry_rslt1['Transaction_amount'].astype(float)
            df_in_tr_tab_qry_rslt1_fig = px.bar(df_in_tr_tab_qry_rslt1, x='States', y='Transaction_amount',
                                                color='Transaction_amount', color_continuous_scale='thermal',
                                                title='Transaction Analysis Chart', height=700, )
            df_in_tr_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
            st.plotly_chart(df_in_tr_tab_qry_rslt1_fig, use_container_width=True)

            # -------  /  All India Total Transaction calculation Table   /   ----  #
            st.header(':violet[Total calculation]')

            col4, col5 = st.columns(2)
            with col4:
                st.subheader(':violet[Transaction Analysis]')
                st.dataframe(df_in_tr_anly_tab_qry_rslt1)
            with col5:
                st.subheader(':violet[Transaction Amount]')
                st.dataframe(df_in_tr_am_qry_rslt)
                st.subheader(':violet[Transaction Count]')
                st.dataframe(df_in_tr_co_qry_rslt)

        # USER TAB
        with tab3:
            col1, col2 = st.columns(2)
            with col1:
                in_us_yr = st.selectbox('**Select Year**', ('2018', '2019', '2020', '2021', '2022'), key='in_us_yr')
            with col2:
                in_us_qtr = st.selectbox('**Select Quarter**', ('1', '2', '3', '4'), key='in_us_qtr')

            # SQL Query

            # User Analysis Bar chart query
            cursor.execute(f"SELECT States, SUM(Transaction_Count) FROM aggregated_user WHERE Years = '{in_us_yr}' AND Quarter = '{in_us_qtr}' GROUP BY States;")
            in_us_tab_qry_rslt = cursor.fetchall()
            df_in_us_tab_qry_rslt = pd.DataFrame(np.array(in_us_tab_qry_rslt), columns=['States', 'Transaction_Count'])
            df_in_us_tab_qry_rslt1 = df_in_us_tab_qry_rslt.set_index(pd.Index(range(1, len(df_in_us_tab_qry_rslt) + 1)))

            # Total User Count table query
            cursor.execute(f"SELECT SUM(Transaction_Count), AVG(Transaction_Count) FROM aggregated_user WHERE Years = '{in_us_yr}' AND Quarter = '{in_us_qtr}';")
            in_us_co_qry_rslt = cursor.fetchall()
            df_in_us_co_qry_rslt = pd.DataFrame(np.array(in_us_co_qry_rslt), columns=['Total', 'Average'])
            df_in_us_co_qry_rslt1 = df_in_us_co_qry_rslt.set_index(['Average'])




            # GEO VISUALIZATION FOR USER

            # Drop a State column from df_in_us_tab_qry_rslt
            df_in_us_tab_qry_rslt.drop(columns=['States'], inplace=True)
            # Clone the gio data
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data2 = json.loads(response.content)
            # Extract state names and sort them in alphabetical order
            state_names_use = [feature['properties']['ST_NM'] for feature in data2['features']]
            state_names_use.sort()
            # Create a DataFrame with the state names column
            df_state_names_use = pd.DataFrame({'States': state_names_use})
            # Combine the Gio State name with df_in_tr_tab_qry_rslt
            df_state_names_use['Transaction_count'] = df_in_us_tab_qry_rslt
            # convert dataframe to csv file
            df_state_names_use.to_csv('State_user.csv', index=False)
            # Read csv
            df_use = pd.read_csv('State_user.csv')
            # Geo plot
            fig_use = px.choropleth(
                df_use,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM', locations='States', color='Transaction_count',
                color_continuous_scale='thermal', title='User Analysis')
            fig_use.update_geos(fitbounds="locations", visible=False)
            fig_use.update_layout(title_font=dict(size=33), title_font_color='#AD71EF', height=800)
            st.plotly_chart(fig_use, use_container_width=True)

            # ----   /   All India User Analysis Bar chart   /     -------- #
            df_in_us_tab_qry_rslt1['States'] = df_in_us_tab_qry_rslt1['States'].astype(str)
            df_in_us_tab_qry_rslt1['Transaction_count'] = df_in_us_tab_qry_rslt1['Transaction_Count'].astype(int)
            df_in_us_tab_qry_rslt1_fig = px.bar(df_in_us_tab_qry_rslt1, x='States', y='Transaction_count', color='Transaction_count',
                                                color_continuous_scale='thermal', title='User Analysis Chart',
                                                height=700, )
            df_in_us_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
            st.plotly_chart(df_in_us_tab_qry_rslt1_fig, use_container_width=True)

            # -----   /   All India Total User calculation Table   /   ----- #
            st.header(':violet[Total calculation]')

            col3, col4 = st.columns(2)
            with col3:
                st.subheader(':violet[User Analysis]')
                st.dataframe(df_in_us_tab_qry_rslt1)
            with col4:
                st.subheader(':violet[User Count]')
                st.dataframe(df_in_us_co_qry_rslt1)

    # STATE TAB
    conn = pymysql.connect(host='localhost', user='root', password='', database='phonepe_pulse_data')
    cursor = conn.cursor()

    if select == "STATES":
        tab3 ,tab4 = st.tabs(["TRANSACTION","USER"])

        #TRANSACTION TAB FOR STATE
        with tab3:
            col1, col2, col3 = st.columns(3)
            with col1:
                st_tr_st = st.selectbox('**Select State**', (
                'andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh', 'assam', 'bihar',
                'chandigarh', 'chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
                'haryana', 'himachal-pradesh',
                'jammu-&-kashmir', 'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh',
                'maharashtra', 'manipur',
                'meghalaya', 'mizoram', 'nagaland', 'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
                'tamil-nadu', 'telangana',
                'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal'), key='st_tr_st')
            with col2:
                st_tr_yr = st.selectbox('**Select Year**', ('2018', '2019', '2020', '2021', '2022'), key='st_tr_yr')
            with col3:
                st_tr_qtr = st.selectbox('**Select Quarter**', ('1', '2', '3', '4'), key='st_tr_qtr')


            # SQL QUERY

            #Transaction Analysis bar chart query
            cursor.execute(f"SELECT Transaction_name, Transaction_amount FROM aggregated_transaction WHERE States = '{st_tr_st}' AND Years = '{st_tr_yr}' AND Quarter = '{st_tr_qtr}';")
            st_tr_tab_bar_qry_rslt = cursor.fetchall()
            df_st_tr_tab_bar_qry_rslt = pd.DataFrame(np.array(st_tr_tab_bar_qry_rslt),
                                                     columns=['Transaction_name', 'Transaction_amount'])
            df_st_tr_tab_bar_qry_rslt1 = df_st_tr_tab_bar_qry_rslt.set_index(
                pd.Index(range(1, len(df_st_tr_tab_bar_qry_rslt) + 1)))

            # Transaction Analysis table query
            cursor.execute(f"SELECT Transaction_name, Transaction_count, Transaction_amount FROM aggregated_transaction WHERE States = '{st_tr_st}' AND Years = '{st_tr_yr}' AND Quarter = '{st_tr_qtr}';")
            st_tr_anly_tab_qry_rslt = cursor.fetchall()
            df_st_tr_anly_tab_qry_rslt = pd.DataFrame(np.array(st_tr_anly_tab_qry_rslt),
                                                      columns=['Transaction_name', 'Transaction_count',
                                                               'Transaction_amount'])
            df_st_tr_anly_tab_qry_rslt1 = df_st_tr_anly_tab_qry_rslt.set_index(
                pd.Index(range(1, len(df_st_tr_anly_tab_qry_rslt) + 1)))

            # Total Transaction Amount table query
            cursor.execute(f"SELECT SUM(Transaction_amount), AVG(Transaction_amount) FROM aggregated_transaction WHERE States = '{st_tr_st}' AND Years = '{st_tr_yr}' AND Quarter = '{st_tr_qtr}';")
            st_tr_am_qry_rslt = cursor.fetchall()
            df_st_tr_am_qry_rslt = pd.DataFrame(np.array(st_tr_am_qry_rslt), columns=['Total', 'Average'])
            

            # Total Transaction Count table query
            cursor.execute(f"SELECT SUM(Transaction_count), AVG(Transaction_count) FROM aggregated_transaction WHERE States = '{st_tr_st}' AND Years ='{st_tr_yr}' AND Quarter = '{st_tr_qtr}';")
            st_tr_co_qry_rslt = cursor.fetchall()
            df_st_tr_co_qry_rslt = pd.DataFrame(np.array(st_tr_co_qry_rslt), columns=['Total', 'Average'])
        



            # -----    /   State wise Transaction Analysis bar chart   /   ------ #

            df_st_tr_tab_bar_qry_rslt1['Transaction_name'] = df_st_tr_tab_bar_qry_rslt1['Transaction_name'].astype(str)
            df_st_tr_tab_bar_qry_rslt1['Transaction_amount'] = df_st_tr_tab_bar_qry_rslt1['Transaction_amount'].astype(
                float)
            df_st_tr_tab_bar_qry_rslt1_fig = px.bar(df_st_tr_tab_bar_qry_rslt1, x='Transaction_name',
                                                    y='Transaction_amount', color='Transaction_amount',
                                                    color_continuous_scale='thermal',
                                                    title='Transaction Analysis Chart', height=500, )
            df_st_tr_tab_bar_qry_rslt1_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
            st.plotly_chart(df_st_tr_tab_bar_qry_rslt1_fig, use_container_width=True)

            # ------  /  State wise Total Transaction calculation Table  /  ---- #
            st.header(':violet[Total calculation]')

            col4, col5 = st.columns(2)
            with col4:
                st.subheader(':violet[Transaction Analysis]')
                st.dataframe(df_st_tr_anly_tab_qry_rslt1)
            with col5:
                st.subheader(':violet[Transaction Amount]')
                st.dataframe(df_st_tr_am_qry_rslt)
                st.subheader(':violet[Transaction Count]')
                st.dataframe(df_st_tr_co_qry_rslt)


        # USER TAB FOR STATE
            
        with tab4:
            col5, col6 = st.columns(2)
            with col5:
                st_us_st = st.selectbox('**Select State**', (
                'andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh', 'assam', 'bihar',
                'chandigarh', 'chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
                'haryana', 'himachal-pradesh',
                'jammu-&-kashmir', 'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh',
                'maharashtra', 'manipur',
                'meghalaya', 'mizoram', 'nagaland', 'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
                'tamil-nadu', 'telangana',
                'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal'), key='st_us_st')
            with col6:
                st_us_yr = st.selectbox('**Select Year**', ('2018', '2019', '2020', '2021', '2022'), key='st_us_yr')
            # SQL QUERY

            # User Analysis Bar chart query
            cursor.execute(f"SELECT Quarter, SUM(Transaction_count) FROM aggregated_user WHERE States = '{st_us_st}' AND Years = '{st_us_yr}' GROUP BY Quarter;")
            st_us_tab_qry_rslt = cursor.fetchall()
            df_st_us_tab_qry_rslt = pd.DataFrame(np.array(st_us_tab_qry_rslt), columns=['Quarter', 'Transaction_count'])
            df_st_us_tab_qry_rslt1 = df_st_us_tab_qry_rslt.set_index(pd.Index(range(1, len(df_st_us_tab_qry_rslt) + 1)))

            # Total User Count table query
            cursor.execute(f"SELECT SUM(Transaction_count), AVG(Transaction_count) FROM aggregated_user WHERE States = '{st_us_st}' AND Years = '{st_us_yr}';")
            st_us_co_qry_rslt = cursor.fetchall()
            df_st_us_co_qry_rslt = pd.DataFrame(np.array(st_us_co_qry_rslt), columns=['Total', 'Average'])
        


            # -----   /   All India User Analysis Bar chart   /   ----- #
            df_st_us_tab_qry_rslt1['Quarter'] = df_st_us_tab_qry_rslt1['Quarter'].astype(int)
            df_st_us_tab_qry_rslt1['Transaction_count'] = df_st_us_tab_qry_rslt1['Transaction_count'].astype(int)
            df_st_us_tab_qry_rslt1_fig = px.bar(df_st_us_tab_qry_rslt1, x='Quarter', y='Transaction_count', color='Transaction_count',
                                                color_continuous_scale='thermal', title='User Analysis Chart',
                                                height=500, )
            df_st_us_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
            st.plotly_chart(df_st_us_tab_qry_rslt1_fig, use_container_width=True)

            # ------    /   State wise User Total User calculation Table   /   -----#
            st.header(':violet[Total calculation]')

            col3, col4 = st.columns(2)
            with col3:
                st.subheader(':violet[User Analysis]')
                st.dataframe(df_st_us_tab_qry_rslt1)
            with col4:
                st.subheader(':violet[User Count]')
                st.dataframe(df_st_us_co_qry_rslt)

    # TOP CATEGORIES
    conn = pymysql.connect(host='localhost', user='root', password='', database='phonepe_pulse_data')
    cursor = conn.cursor()

    if select == "TOP CATEGORIES":
        tab5,tab6,tab7 = st.tabs(["INSURANCE","TRANSACTION", "USER"])

        with tab5:
            top_insur_yr = st.selectbox('**Select Year**', ('2020', '2021', '2022','2023'), key='top_insur_yr')

            #SQL QUERY

            #Top Insurance Analysis bar chart query
            cursor.execute(
                f"SELECT States,SUM(Transaction_amount) As Transaction_amount FROM top_insurance WHERE Years = '{top_insur_yr}' GROUP BY States ORDER BY Transaction_amount DESC LIMIT 10;")
            top_insur_tab_qry_rslt = cursor.fetchall()
            df_top_insur_tab_qry_rslt = pd.DataFrame(np.array(top_insur_tab_qry_rslt),columns=['States','Transaction_amount'])
            df_top_insur_tab_qry_rslt1 = df_top_insur_tab_qry_rslt.set_index(pd.Index(range(1,len(df_top_insur_tab_qry_rslt) + 1)))

            # Top Insurance Analysis table query
            cursor.execute(
                f"SELECT States,SUM(Transaction_amount) as Transaction_amount, SUM(Transaction_count) as Transaction_count FROM top_insurance WHERE Years = '{top_insur_yr}' GROUP BY States ORDER BY Transaction_amount DESC LIMIT 10;")
            top_insur_anly_tab_qry_rslt = cursor.fetchall()
            df_top_insur_anly_tab_qry_rslt = pd.DataFrame(np.array(top_insur_anly_tab_qry_rslt),columns=['States','Transaction_amount','Transaction_count'])
            df_top_insur_anly_tab_qry_rslt1 = df_top_insur_anly_tab_qry_rslt.set_index(pd.Index(range(1, len(df_top_insur_anly_tab_qry_rslt) + 1)))



            # All India Insurance Analysis Bar chart
            df_top_insur_tab_qry_rslt1['States'] = df_top_insur_tab_qry_rslt1['States'].astype(str)
            df_top_insur_tab_qry_rslt1['Transaction_amount'] = df_top_insur_tab_qry_rslt1['Transaction_amount'].astype(float)
            df_top_insur_tab_qry_rslt1_fig = px.bar(df_top_insur_tab_qry_rslt1, x='States', y='Transaction_amount',
                                                 color='Transaction_amount', color_continuous_scale='turbo',
                                                 title='Top Insurance Analysis Chart', height=600, )
            df_top_insur_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
            st.plotly_chart(df_top_insur_tab_qry_rslt1_fig, use_container_width=True)


            #All India Total Insurance calculation Table
            st.header(':violet[Total calculation]')
            st.subheader('Top Insurance Analysis')
            st.dataframe(df_top_insur_anly_tab_qry_rslt1)


        # Overall top transaction
        #TRANSACTION TAB
        with tab6:
            top_tr_yr = st.selectbox('**Select Year**', ('2018', '2019', '2020', '2021', '2022'), key='top_tr_yr')

            #SQL QUERY

            #Top Transaction Analysis bar chart query
            cursor.execute(
                f"SELECT States, SUM(Transaction_amount) As Transaction_amount FROM top_transaction WHERE Years = '{top_tr_yr}' GROUP BY States ORDER BY Transaction_amount DESC LIMIT 10;")
            top_tr_tab_qry_rslt = cursor.fetchall()
            df_top_tr_tab_qry_rslt = pd.DataFrame(np.array(top_tr_tab_qry_rslt),
                                                  columns=['States', 'Transaction_amount'])
            df_top_tr_tab_qry_rslt1 = df_top_tr_tab_qry_rslt.set_index(
                pd.Index(range(1, len(df_top_tr_tab_qry_rslt) + 1)))

            # Top Transaction Analysis table query
            cursor.execute(
                f"SELECT States, SUM(Transaction_amount) as Transaction_amount, SUM(Transaction_count) as Transaction_count FROM top_transaction WHERE Years = '{top_tr_yr}' GROUP BY States ORDER BY Transaction_amount DESC LIMIT 10;")
            top_tr_anly_tab_qry_rslt = cursor.fetchall()
            df_top_tr_anly_tab_qry_rslt = pd.DataFrame(np.array(top_tr_anly_tab_qry_rslt),
                                                       columns=['States', 'Transaction_amount',
                                                                'Transaction_count'])
            df_top_tr_anly_tab_qry_rslt1 = df_top_tr_anly_tab_qry_rslt.set_index(
                pd.Index(range(1, len(df_top_tr_anly_tab_qry_rslt) + 1)))



            # All India Transaction Analysis Bar chart
            df_top_tr_tab_qry_rslt1['States'] = df_top_tr_tab_qry_rslt1['States'].astype(str)
            df_top_tr_tab_qry_rslt1['Transaction_amount'] = df_top_tr_tab_qry_rslt1[
                'Transaction_amount'].astype(float)
            df_top_tr_tab_qry_rslt1_fig = px.bar(df_top_tr_tab_qry_rslt1, x='States', y='Transaction_amount',
                                                 color='Transaction_amount', color_continuous_scale='thermal',
                                                 title='Top Transaction Analysis Chart', height=600, )
            df_top_tr_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
            st.plotly_chart(df_top_tr_tab_qry_rslt1_fig, use_container_width=True)


            #All India Total Transaction calculation Table
            st.header(':violet[Total calculation]')
            st.subheader('Top Transaction Analysis')
            st.dataframe(df_top_tr_anly_tab_qry_rslt1)

        # OVERALL TOP USER DATA
        # USER TAB
        with tab7:
            top_us_yr = st.selectbox('**Select Year**', ('2018', '2019', '2020', '2021', '2022'), key='top_us_yr')

            #SQL QUERY

            #Top User Analysis bar chart query
            cursor.execute(f"SELECT States, SUM(RegisteredUsers) AS Top_user FROM top_user WHERE Years='{top_us_yr}' GROUP BY States ORDER BY Top_user DESC LIMIT 10;")
            top_us_tab_qry_rslt = cursor.fetchall()
            df_top_us_tab_qry_rslt = pd.DataFrame(np.array(top_us_tab_qry_rslt), columns=['States', 'Transaction_count'])
            df_top_us_tab_qry_rslt1 = df_top_us_tab_qry_rslt.set_index(
                pd.Index(range(1, len(df_top_us_tab_qry_rslt) + 1)))



            #All India User Analysis Bar chart
            df_top_us_tab_qry_rslt1['States'] = df_top_us_tab_qry_rslt1['States'].astype(str)
            df_top_us_tab_qry_rslt1['Transaction_count'] = df_top_us_tab_qry_rslt1['Transaction_count'].astype(float)
            df_top_us_tab_qry_rslt1_fig = px.bar(df_top_us_tab_qry_rslt1, x='States', y='Transaction_count',
                                                 color='Transaction_count', color_continuous_scale='thermal',
                                                 title='Top User Analysis Chart', height=600, )
            df_top_us_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
            st.plotly_chart(df_top_us_tab_qry_rslt1_fig, use_container_width=True)

            #All India Total Transaction calculation Table
            st.header(':violet[Total calculation]')
            st.subheader('violet[Total User Analysis]')
            st.dataframe(df_top_us_tab_qry_rslt1)

#INSIGHTS TAB

conn = pymysql.connect(host='localhost', user='root', password='', database='phonepe_pulse_data')
cursor = conn.cursor()

if selected == "Insights":
    st.title(':violet[BASIC INSIGHTS]')
    st.subheader("The basic insights are derived from the Analysis of the Phonepe Pulse data. It provides a clear idea about the analysed data.")
    options = ["--select--",
               "Top 10 states based on year and amount of transaction",
               "Least 10 states based on year and amount of transaction",
               "Top 10 States and Districts based on Registered Users",
               "Least 10 States and Districts based on Registered Users",
               "Top 10 Districts based on the Transaction Amount",
               "Least 10 Districts based on the Transaction Amount",
               "Top 10 Districts based on the Transaction count",
               "Least 10 Districts based on the Transaction count",
               "Top Transaction types based on the Transaction Amount",
               "Top 10 Mobile Brands based on the User count of transaction"]
    select = st.selectbox(":violet[Select the option]",options)

    #1
    if select == "Top 10 states based on year and amount of transaction":
        cursor.execute(
            "SELECT DISTINCT States,Years, SUM(Transaction_amount) AS Total_Transaction_Amount FROM top_transaction GROUP BY States,Years ORDER BY Total_Transaction_Amount DESC LIMIT 10");

        data = cursor.fetchall()
        columns = ['States', 'Years', 'Transaction_amount']
        df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 states based on amount of transaction")
            st.bar_chart(data=df, x="Transaction_amount", y="States")

    #2
    elif select == "Least 10 states based on year and amount of transaction":
        cursor.execute(
            "SELECT DISTINCT States,Years, SUM(Transaction_amount) as Total FROM top_transaction GROUP BY States, Years ORDER BY Total LIMIT 10");
        data = cursor.fetchall()
        columns = ['States', 'Years', 'Transaction_amount']
        df = pd.DataFrame(data, columns=columns, index=range(1,len(data)+1))
        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Least 10 states based on amount of transaction")
            st.bar_chart(data=df, x="Transaction_amount", y="States")

    #3
    elif select == "Top 10 States and Districts based on Registered Users":
        cursor.execute("SELECT DISTINCT States, Pincodes,SUM(RegisteredUsers) AS Users FROM top_user GROUP BY States, Pincodes ORDER BY Users DESC LIMIT 10");
        data = cursor.fetchall()
        columns = ['States', 'Pincodes', 'RegisteredUsers']
        df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 States and Districts based on Registered Users")
            st.bar_chart(data=df, x="RegisteredUsers", y="States")

    #4
    elif select == "Least 10 States and Districts based on Registered Users":
        cursor.execute("SELECT DISTINCT States, Pincodes, SUM(RegisteredUsers) AS Users FROM top_user GROUP BY States, Pincodes ORDER BY Users ASC LIMIT 10");
        data = cursor.fetchall()
        columns = ['States', 'Pincodes', 'RegisteredUsers']
        df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))
        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Least 10 States and Districts based on Registered Users")
            st.bar_chart(data=df, x="RegisteredUsers", y="States")

    #5
    elif select == "Top 10 Districts based on the Transaction Amount":
        cursor.execute(
            "SELECT DISTINCT States ,District,SUM(Transaction_Amount) AS Total FROM map_transaction GROUP BY States ,District ORDER BY Total DESC LIMIT 10");
        data = cursor.fetchall()
        columns = ['States', 'District', 'Transaction_Amount']
        df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))
        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 Districts based on Transaction Amount")
            st.bar_chart(data=df, x="District", y="Transaction_Amount")

    #6
    elif select == "Least 10 Districts based on the Transaction Amount":
        cursor.execute(
            "SELECT DISTINCT States,District,SUM(Transaction_amount) AS Total FROM map_transaction GROUP BY States, District ORDER BY Total ASC LIMIT 10");
        data = cursor.fetchall()
        columns = ['States', 'District', 'Transaction_amount']
        df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Least 10 Districts based on Transaction Amount")
            st.bar_chart(data=df, x="District", y="Transaction_amount")

    #7
    elif select == "Top 10 Districts based on the Transaction count":
        cursor.execute(
            "SELECT DISTINCT States,District,SUM(Transaction_Count) AS Counts FROM map_transaction GROUP BY States ,District ORDER BY Counts DESC LIMIT 10");
        data = cursor.fetchall()
        columns = ['States', 'District', 'Transaction_Count']
        df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 Districts based on Transaction Count")
            st.bar_chart(data=df, x="Transaction_Count", y="District")

    #8
    elif select == "Least 10 Districts based on the Transaction count":
        cursor.execute(
            "SELECT DISTINCT States ,District,SUM(Transaction_Count) AS Counts FROM map_transaction GROUP BY States ,District ORDER BY Counts ASC LIMIT 10");
        data = cursor.fetchall()
        columns = ['States', 'District', 'Transaction_Count']
        df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 Districts based on the Transaction Count")
            st.bar_chart(data=df, x="Transaction_Count", y="District")

    #9
    elif select == "Top Transaction types based on the Transaction Amount":
        cursor.execute(
            "SELECT DISTINCT Transaction_name, SUM(Transaction_amount) AS Amount FROM aggregated_transaction GROUP BY Transaction_name ORDER BY Amount DESC LIMIT 5");
        data = cursor.fetchall()
        columns = ['Transaction_name', 'Transaction_amount']
        df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top Transaction Types based on the Transaction Amount")
            st.bar_chart(data=df, x="Transaction_name", y="Transaction_amount")

    #10
    elif select == "Top 10 Mobile Brands based on the User count of transaction":
        cursor.execute(
            "SELECT DISTINCT Brands,SUM(Transaction_count) as Total FROM aggregated_user GROUP BY Brands ORDER BY Total DESC LIMIT 10");
        data = cursor.fetchall()
        columns = ['Brands', 'Transaction_count']
        df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 Mobile Brands based on User count of transaction")
            st.bar_chart(data=df , x="Transaction_count", y="Brands")

 