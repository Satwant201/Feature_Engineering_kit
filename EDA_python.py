import pandas as pd
import numpy as np
import os

%%javascript
IPython.OutputArea.prototype._should_scroll = function(lines) {
    return false;
}

%%capture
import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import pandas_profiling as pp

# Input data files are available in the "../input/" directory.
# For example, running this (by clicking run or pressing Shift+Enter) will list the files in the input directory

import os

#Set working directory

Project_dir = "abc/xyz"
os.chdir(Project_dir)

data_train = pd.read_csv("input_file.csv")

### pandas profiler gives interactive EDA results with different correlation matice
pp.ProfileReport(train)



#### writing own univariate and bivariate code because pandas_profiler fails with row count > 1 million 

def univariate(data):
	#Creating seperate dataframes for categorical and continuous variables
	data_cat = data.select_dtypes(include=[object])    
	data_cont = data.select_dtypes(exclude=[object])
	
    
	data_cont_univ = data_cont.describe(percentiles=[.001,.005,.01,.25,.5,.75,.95,.99,.995,.999]).transpose()
	
	data_cont_univ["blanks"] = data_cont.isna().sum(axis=0)
	
	data_cont_univ["zeros"] = (data_cont == 0).sum(axis=0)
	
	data_cont_univ.to_excel("univariate_continuous.xlsx")
	
    
	
	data_cat_univ = data_cat.describe().transpose()
	
	data_cat_univ["blanks"] = data_cat.isna().sum(axis=0)
	
	data_cat_univ["zeros"] = data_cat.isin(["0","00","000","0000","00000","000000"]).sum(axis=0)
	
	data_cat_univ.to_excel("univariate_categorical"+data+".xlsx")

univariate(data_train)

data_train.reset_index(drop=True,inplace=True)
data_train['rank'] = data_train.index + 1

def bivariate(data,id,event):
    
    #Subsetting categorical variables for bivariate analysis
    data_cat = data.select_dtypes(include=[object])    
    #Converting the event flag back to numeric for event calculation
    data_cat[event] = pd.to_numeric(data_cat[event])
    
    
    #This code block moves the variables for which bivariate is not required to the end of column sequence
    cols = list(data_cat.columns.values)
    cols.pop(cols.index('claim'))
    data_cat = data_cat[cols+['claim']] 
    
    
    
    #Looping bivariate analysis for all variables and appaending the results in a single dataframe
    #The len()-2 is to exclude the variables for which bivariate is not needed (as per the column sequence)
    appended_data = pd.DataFrame()
    for x in range(0, len(data_cat.columns)-1):
    
        data2 = pd.DataFrame({'1.Variable':data_cat.columns[x],
                              '2.Level':data_cat.groupby(data_cat.columns[x])[event].sum().index,
                              '3.Event':data_cat.groupby(data_cat.columns[x])[event].sum(),
                              '4.Volume':data_cat.groupby(data_cat.columns[x])[id].count(),
                              '5.Rate':((data_cat.groupby(data_cat.columns[x])[event].sum()/data_cat.groupby(data_cat.columns[x])[id].count())*100).round(2)})
    

        appended_data = appended_data.append(data2)
    #Output
    appended_data.to_excel("bivariate.xlsx", index = False) 
