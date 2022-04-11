 
import base64
import streamlit as st
import pandas as pd
import time


st.title("atuomated file process val avg")
st.markdown("Uppload a zip File with the following fileds:   TransactionDate ,TransactionTime ,CardIDbi ,RouteId ,RouteShortName ,Direction ,StopCode ,StopName ")

def process_data(df):
    try:   
         df= df.rename(columns={"ClusterId":"Cluster_code2"})
    
    except:
        pass

    df['Val_datetime']=pd.to_datetime(df['TransactionDate']+ " " +df['TransactionTime'])
    df['People_key']=df.CardIDbi.astype(str)+df.TransactionDate.astype(str)
    df['Day'] = pd.to_datetime(df['TransactionDate']).dt.day
    df['Hour']=df.Val_datetime.dt.hour
    df_after=df.groupby(['RouteId',"RouteShortName",'Direction','Day',"StopCode","StopName",'Hour']).size().groupby(['RouteId',"RouteShortName",'Direction',"StopCode","StopName",'Hour']).mean().to_frame("nov_mean").reset_index()  
    return df_after

def process_luz(luz,df):
    df_after = process_data(df)
    luz = luz.rename(columns={"מקט":"Line_code","כיוון":"Direction","שעת יציאה":"Hour","יום":"Day"})
 

    luz=luz.loc[luz.Line_code.isin(df_after.RouteId.unique())]
    luz = luz.rename(columns={"Line_code":"RouteId"}).rename(columns={"קו":"RouteShortName"})
    luz.Hour=luz.Hour.str[:2].astype(int)
    t_luz = luz.groupby(['RouteId',"RouteShortName", 'Direction', 'Day', 'Hour']).size().to_frame('cnt').groupby(['RouteId',"RouteShortName", 'Direction','Hour'])['cnt'].mean().to_frame('Luz_kaitz').reset_index()
    return t_luz

def merge_process(luz,df):
    t_luz = process_luz(luz,df)
    df_after=process_data(df)
    answer=pd.merge(df_after, t_luz, on=['RouteId',"RouteShortName",'Hour',"Direction"], how='outer')
    answer_new= answer.copy()
    answer_new['Mean_in_trip_nov']=answer_new.nov_mean/answer_new.Luz_kaitz
    return answer_new

def download_csv(luz,df):
    answer_new=merge_process(luz,df)
    csv = answer_new.to_csv(index=False)
    b64 = base64.b64encode(csv.encode('utf-8')).decode()
    href = f'<a href="data:file/csv;base64,{b64}">Download csv File</a> (right-click and save as &lt;some_name&gt;.csv)'
    return href


def main():
    file1 = st.file_uploader("Choose a file csv for val data")
    file2 = st.file_uploader("Choose a file excel for luz data")
    
    if file1 is not None and file2 is not None:
        df = pd.read_csv(file1,sep=';', error_bad_lines=True,engine='python', index_col=False, encoding="UTF-8-SIG")
        luz = pd.read_excel(file2)
        with st.spinner('Reading data zip and csv File and read luz excel file...'):
            st.success('Done!')




        st.subheader("Choose Address Columns from the Sidebar")
        st.info("Example correct data structe: TransactionDate:2022-02-22	 ,TransactionTime:18:50	 ,CardIDbi:18518533 ,RouteId:14139 ,RouteShortName:139 ,Direction:1 ,StopCode:681 ,StopName:מחלף גבעת שמאל	")
    
    if st.checkbox("data Formatted correctly (Example Above)"):

        with st.spinner('prcoess Hold tight...'):
            time.sleep(5)
            st.success('Done!')
            st.markdown(download_csv(luz=luz,df=df), unsafe_allow_html=True)
       
        
            


if __name__ == "__main__":
     main()


