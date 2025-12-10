import os
import pandas as pd
import numpy as np
 
def transform_data(raw_path):
    base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir=os.path.join(base_dir,"data","staged")
    os.makedirs(staged_dir,exist_ok=True)
    df=pd.read_csv(raw_path)

    df['TotalCharges']=pd.to_numeric(df['TotalCharges'],errors='coerce')
    df.TotalCharges.fillna(df.TotalCharges.median(),inplace=True)
    df['tenure_group']=pd.cut(x=df['tenure'],bins=[0,12,36,60,np.inf],labels=["New","Regular",'Loyal','Champion'],right=False)
    df['monthly_charge_segment']=pd.cut(x=df['MonthlyCharges'],bins=[0,30,70,np.inf],labels=['Low','Medium','High'],right=False)
    df['has_internet_service']=df['InternetService'].map({
    'No':0,
    'DSL':1,
    'Fiber optic':1
    })
    df['is_multi_line_user']=df['MultipleLines'].map({'Yes':1}).fillna(0)
    df['is_multi_line_user']=df['MultipleLines'].apply(lambda x:1 if x=='Yes' else 0)
    df['contract_type_code']=df['Contract'].map({
        'Month-to-month':0,
        'One year':1,
        'Two year':2
    })
    df.drop(columns=['customerID','gender'],inplace=True)

    staged_path=os.path.join(staged_dir,'churn_staged.csv')
    df.to_csv(staged_path,index=False)
    print(f"✅ Data transformed and saved at: {staged_path}")
    return staged_path

if __name__ == "__main__":
    from extract import extract_data
    raw_path = extract_data()
    transform_data(raw_path)
 