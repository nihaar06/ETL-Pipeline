import os
import pandas as pd

def validate_data(staged_path):
    cdf=pd.read_csv(staged_path)
    odf=pd.read_csv("C:\\Users\\LENOVO\\ETL-Pipeline\\TELCO\\Telco-Customer-Churn.csv")
    print("Missing values in transformed data:[tenure, MonthlyCharges, TotalCharges]")
    print(cdf.isnull().sum())
    print("Count of unique rows in transformed and original data is same:")
    print(cdf.nunique().equals(odf.nunique()))
    print("Row count matches original table:")
    print(len(cdf)==len(odf))
    print(cdf.info())
    print("Contract type codes in transformed data:")
    print(cdf.contract_type_code.unique())
if __name__=='__main__':
    staged_csv_path=os.path.join("..","data","staged","churn_staged.csv")
    validate_data(staged_csv_path)