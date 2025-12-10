import pandas as pd
import os

def extract_data():
    base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    extract_dir=os.path.join(base_dir,"data","raw")
    os.makedirs(extract_dir,exist_ok=True)

    raw_path=os.path.join(extract_dir,"churn.csv")
    df=pd.read_csv("C:\\Users\\LENOVO\\ETL-Pipeline\\TELCO\\Telco-Customer-Churn.csv")
    df.to_csv(raw_path,index=False)
    print(f"✅ Data extracted and saved at: {raw_path}")
    return raw_path
if __name__=="__main__":
    extract_data()