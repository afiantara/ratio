import os
import os.path
import pandas as pd
from pathlib import Path
import sqlite3
root_datas_fs_folder = './datas/financial_statement'
root_datas_is_folder = './datas/income_statement'

db_fs_names = [ os.path.join(root_datas_fs_folder,'financial_statement_life_insurance.db'),
            os.path.join(root_datas_fs_folder,'financial_statement_non_life_insurance.db'),
            os.path.join(root_datas_fs_folder,'financial_statement_reassurance.db'),
            os.path.join(root_datas_fs_folder,'financial_statement_social_insurance.db')]

tbl_fs_names=['financial_statement_life_insurance',
              'financial_statement_non_life_insurance',
              'financial_statment_reassuance',
              'financial_statement_social_insurance']
            
db_is_names = [ os.path.join(root_datas_is_folder,'income_statement_life_insurance.db'),
            os.path.join(root_datas_is_folder,'income_statement_non_life_insurance.db'),
            os.path.join(root_datas_is_folder,'income_statement_reassurance.db'),
            os.path.join(root_datas_is_folder,'income_statement_social_insurance.db')]

tbl_is_names=['income_statement_life_insurance',
              'income_statement_non_life_insurance',
              'income_statment_reassuance',
              'income_statement_social_insurance']


def write_sql(df,tablename):
    import pandas as pd
    import sqlite3 as sq

    table_name = tablename # table and file name
    db_name ='insurance'
    conn = sq.connect('./datas/{}.db'.format(db_name)) # creates file
    df.to_sql(table_name, conn, if_exists='replace', index=False) # writes to file
    conn.close() # good practice: close connection


if __name__=="__main__":
    
    from fs import getAccountByYear,specified_cols_df,swap_df
    #life insurance merge financial statement and income statement
    
    #financial statement
    con,d_fsLife = getAccountByYear(db_fs_names[0],tbl_fs_names[0],2018,2023)
    con.close()
    #income statement
    con,d_isLife = getAccountByYear(db_is_names[0],tbl_is_names[0],2018,2023)
    con.close()
    #call specific column from income statement.
    d_isLife=specified_cols_df(d_isLife)
    
    #merge here for life_insurance
    d_Life=pd.merge(d_fsLife, d_isLife, on="Periode") 
    d_Life['sector']='life_insurance'
    
    #non life insurance
    #financial statement
    con,dfs_NonLife = getAccountByYear(db_fs_names[1],tbl_fs_names[1],2018,2023)
    dfs_NonLife['Policy Loan']=None # Policy Loan not existing in Non Life
    con.close()
    #income statement
    con,dis_NonLife = getAccountByYear(db_is_names[1],tbl_is_names[1],2018,2023)
    con.close()
    
    dis_NonLife=swap_df(dis_NonLife)
    dis_NonLife=specified_cols_df(dis_NonLife)
    #merge here
    d_NonLife=pd.merge(dfs_NonLife, dis_NonLife, on="Periode") 
    d_NonLife['sector']='non_life_insurance'

    #calculate difference of all columns
    #create group by year
    d_Life['year'] = d_Life['Periode'].dt.year
    d_NonLife['year']=d_NonLife['Periode'].dt.year
    #calculate difference with prefix D_    
    accts=['Total Assets','Total Investment','Investment Yield','Income (Loss) Before Tax','Premium Income','Total Claims and Benefits','Total Operating Expenses','Total Net Premium Income','Reinsurance Income','Income (Loss) After Tax','General And Administration Expenses']
    
    for count in accts:
        #delta
        d_Life['D_' + count] = d_Life.groupby(['year'])[count].diff().fillna(d_Life[count])
        
        #moving average 12 bulan
        ma_12=d_Life[count].rolling(window=12)
        d_Life['Roll_' + count] =ma_12.mean()
        #percent change
        d_Life['Growth_'+count]=d_Life[count].pct_change(periods=12)*100
        
        #delta
        d_NonLife['D_' + count] = d_NonLife.groupby(['year'])[count].diff().fillna(d_NonLife[count])
        #moving average 12 bulan
        ma_12=d_NonLife[count].rolling(window=12)
        d_NonLife['Roll_' + count] =ma_12.mean()
        #percent change
        d_NonLife['Growth_'+count]=d_NonLife[count].pct_change(periods=12)*100
         
    #join life and non life
    df_join = pd.concat([d_Life, d_NonLife],axis=0)    
    
    #calculate ratio
    df_join['ROA'] = df_join['Roll_Income (Loss) Before Tax']/df_join['Roll_Total Assets']
    df_join['ROE'] = df_join['Roll_Income (Loss) Before Tax']/df_join['Total Equities']
    #df_join['Growth Premium Income'] = df_join['Chg_Premium Income']
    #df['Investment Growth'] = df['Total Investment'].pct_change(periods=12)*100
    #df['Assets Growth'] = df['Total Assets'].pct_change(periods=12)*100
    df_join['Investment Yield Ratio'] = df_join['Roll_Investment Yield'] /df_join['Roll_Total Investment']
    df_join['Loss Ratio'] = df_join['Roll_Total Claims and Benefits']/df_join['Roll_Total Net Premium Income']
    df_join['Expense Ratio'] = df_join['Roll_Total Operating Expenses']/df_join['Roll_Total Net Premium Income']
    df_join['Combined Ratio'] = df_join['Loss Ratio'] + df_join['Expense Ratio']
    df_join['Cession Ratio'] = df_join['Roll_Reinsurance Income'] / df_join['Roll_Premium Income']
    df_join['Retention Ratio'] = 1 - df_join['Cession Ratio']
    df_join['Net Income Ratio'] = df_join['Roll_Income (Loss) After Tax']/df_join['Roll_Total Net Premium Income']
    df_join['Liquid Ratio'] = (df_join['Cash and Bank'] + df_join['Time Deposit']) / df_join['Total Payable']
    df_join['Investment Adequacy Ratio'] = (df_join['Cash and Bank'] + df_join['Total Investment']) / df_join['Total Technical Reserve']
    df_join['Premium to Claim Ratio'] = df_join['Roll_Total Net Premium Income']/df_join['Roll_Total Claims and Benefits']
    df_join['Premium to Claim and G/A Ratio'] = df_join['Roll_Total Net Premium Income']/(df_join['Roll_Total Claims and Benefits'] + df_join['Roll_General And Administration Expenses'])

    write_sql(df_join,'life_non_life')
    write_sql(d_Life,'life_insurance')
    write_sql(d_NonLife,'non_life_insurance')
    
   
    