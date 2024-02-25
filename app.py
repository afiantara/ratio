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
    
    from fs import getAccountByYear,difference
    #life insurance merge financial statement and income statement
    
    #financial statement
    con,d_fsLife = getAccountByYear(db_fs_names[0],tbl_fs_names[0],2018,2023)
    con.close()
    #income statement
    con,d_isLife = getAccountByYear(db_is_names[0],tbl_is_names[0],2018,2023)
    con.close()
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
    #merge here
    d_NonLife=pd.merge(dfs_NonLife, dis_NonLife, on="Periode") 
    d_NonLife['sector']='non_life_insurance'
    
    #join_financial_statement(d_fsLife,dfs_NonLife)
    d_NonLife.rename(columns = {'Income After Tax':'Income (Loss) After Tax', 'Other Comprehensif Income':'Other Comprehensive Income', 
                              'Total Premium Income':'Premium Income','Total Reinsurance Premium':'Reinsurance Income',
                              'Total Claim Expenses':'Total Claims and Benefits',
                              'Total Operating Expense':'Total Operating Expenses',
                              'Decrease (Increase) In Premium Reserve':'Increase (decrease) in Premium Reserve',
                              'Decrease (Increase) In Catastrophic Reserve':'Increase (decrease) in Catastrophic Reserve',
                              'Increase (Decrease) in Claim Reserve':'Increase (decrease) in Claim Reserve'}, inplace = True) 
    
    #diperlukan ketika mau dijoin secara vertical
    # d_Life = d_Life.reset_index()
    #d_NonLife = d_NonLife.reset_index()
    #df_join = pd.concat([d_Life, d_NonLife],axis=1)    
    #write_sql(df_join,'life_non_life')
    #calculate difference of all columns
    
    #create group by year
    d_Life['year'] = d_Life['Periode'].dt.year
    d_NonLife['year']=d_NonLife['Periode'].dt.year
    
    #calculate difference with prefix D_    
    accts=['Income (Loss) Before Tax','Premium Income','Total Claims and Benefits','Total Operating Expenses','Total Net Premium Income','Reinsurance Income','Income (Loss) After Tax','General And Administration Expenses']
    
    for count in accts:
        #delta
        d_Life['D_' + count] = d_Life.groupby(['year'])[count].diff().fillna(d_Life[count])
        
        #moving average 12 bulan
        ma_12=d_Life[count].rolling(window=12)
        d_Life['Roll_' + count] =ma_12.mean()
        
        #delta
        d_NonLife['D_' + count] = d_NonLife.groupby(['year'])[count].diff().fillna(d_NonLife[count])
        #moving average 12 bulan
        ma_12=d_NonLife[count].rolling(window=12)
        d_NonLife['Roll_' + count] =ma_12.mean()
         
    
    d_Life['ROE'] = d_Life['Income (Loss) Before Tax']/d_Life['Total Equities']    
    d_NonLife['ROE'] = d_NonLife['Income (Loss) Before Tax']/d_NonLife['Total Equities']
    
    write_sql(d_Life,'life_insurance')
    write_sql(d_NonLife,'non_life_insurance')
    
   
    