import camelot
import tabula
import numpy as np
import pandas as pd
import os #creating and removing a directory (folder), fetching its contents, changing and identifying the current directory, etc.
import re,string #A regular expression (or RE) specifies a set of strings that matches it; the functions in this module let you check if a particular string matches a given regular expression (or if a given regular expression matches a particular string, which comes down to the same thing). String contains classes for and utility functions for string menipulation 
import sys
from dateutil.parser import parse #Fixing the dates

os.chdir('c:\\Users\\karln\\iCloudDrive\\DataScience Projects\\Chime statements')

print(os.getcwd())

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

#Path to bank statement
november = tabula.read_pdf('november21.pdf', pages = 'all')
print(november)
#table = camelot.read_pdf(november, pages='all')
#print("Total tables extracted: ", table.n)

def clean_trns_desc(text):
    test = text.lower()
    #removing anything within square brackets
    text = re.sub('\[.*?#-\]', '',text) #The '\' starts  the string match, and currently, its matching every symbol that's in the square bracket (?*.\)
    #if any of these punctuation marks in (string.punctuation) get rid of it
    text = re.sub('[%s]' % re.escape(string.punctuation),'',text)
    #getting rid of all the numbers
    text = re.sub('\d+','',text)
    return text


round1 = lambda x: clean_trns_desc(x)

try:
    df_list = tabula.read_pdf('november21.pdf', stream = True, guess = True, pages = 'all',
                              multiple_tables=True,
                              pandas_options={'header':None} 
                              )
except Exception as e: 
    print('The error is', e)

#Cleaning up each page before joining them 
df = []

for dfs in df_list:
    dfs = dfs[dfs.columns[dfs.isnull().mean() < 0.8]]
    #drop rows with empty cells
    dfs.dropna(axis=0, how='any', thresh = 2, subset = None, inplace = True)
    # dfs['Description'] = dfs.iloc[:,1].str.cat(dfs.iloc[:,2],sep=" ")
    if dfs.shape[1] > 5:
        dfs.drop(dfs.columns[-1], axis = 1, inplace = True)
        df.append(dfs)
    else:
        df.append(dfs)  


    # Join individual dataframes into one
    df_fin = pd.concat([df[1],df[2],df[3],df[4]], axis=0, sort=False) #FIX: make this part dynamic!
    df_fin = df_fin[~df_fin.iloc[:,0].str.contains("Date")]
    df_fin.columns = ['date',"trns_desc_1",'trns_type','amount','net_amount']