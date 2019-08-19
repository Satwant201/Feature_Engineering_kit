
########### Feature engineering ##############
#### lookback / risk and COV  variables
#### libraries anf fxns

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.functions import count, avg,col,min,max,mean,stddev,lit,when,last_day,round,datediff, to_date, lit
import time
from dateutil.relativedelta import relativedelta
import datetime
import pandas as pd
import re
from pyspark.sql.functions import last_day
from pyspark.sql.functions import count, avg,col,min,max,mean,stddev,lit,when,last_day,round,datediff, to_date, lit
import time
from dateutil.relativedelta import relativedelta
import datetime
import copy
from pyspark.sql.functions import log
from pyspark.sql.functions import pow, col

import sys

##sys.path

##sys.path.insert(1,'/mapr/datalake/uhc/ei/pi_ara/kappa/users/Isaac/CnS_HDC/hdc_project_20190127/code/py')
##sys.path


####################################### join fxn #########################
def data_join(df1,df2,keys,join):
    df_join = df1.join(df2, on=keys, how=join)
    return df_join

####################### date filter fxn #################################
def date_filter_fxn(base_table,date_column,lookback_period):
	col_name  = "max("+date_column+")"
	dict_date={}
	dict_date[date_column]="max"
	row1 = base_table.agg(dict_date).collect()[0]
	try:
		erly_srvc_dt_latest = row1[col_name].strftime('%Y-%m-%d')
	except:
		erly_srvc_dt_latest = row1[col_name]
		print("data type of date column is string instead of date")
	end_date=(datetime.datetime.strptime(erly_srvc_dt_latest, "%Y-%m-%d")-relativedelta(months=lookback_period)).strftime("%Y-%m-%d")
	return base_table.filter(col(date_column)>=end_date)



########################## Aggregation fxn for mupliple grouping multiple target column and multiple aggregations types ###
def Aggregation_fxn(base_table,grp_clause,aggregation_col,aggregation_type):
	n=len(aggregation_type)
	k=[]
	a=aggregation_type
	for i in range(n):
		for j in a:
			k.append(j)
	if n==1:
		model_base_table=base_table
		col_name_list=aggregation_col
	else:
		col_name_list=[]
		for j in range(len(aggregation_col)):
			col_name=aggregation_col[j]
			for i in range(n):
				z=col_name+str(i)
				col_name_list.append(col_name+str(i))
				if i==0 and j==0:
					df1=base_table.withColumn(z,col(col_name))
				else:
					df1=df1.withColumn(z,col(col_name))
		model_base_table=df1
	grp=grp_clause
	dict={}
	for i in range(len(col_name_list)):
		dict[col_name_list[i]]=k[i]
	sample = model_base_table.groupBy(grp).agg(dict)
	st=''
	for i in range(len(grp)):
		if i ==0:
			st=st+grp[i]
		else:
			st=st+"_"+grp[i]
	agg_col_names=[]
	for key,value in dict.items():
		z=st+"__"+value+"_"+key
		agg_col_names.append(z)
	agg_results = [w for w in sample.schema.names if w not in grp]
	agg_mapping = []
	for a in agg_results:
		for b in agg_col_names:
			if b.find(re.split("\(",a)[0])>=0:
				agg_mapping.append(b)
				break
			elif b.find("std")>=0 and a.find("stddev")>=0:
				agg_mapping.append(b)
				break
			elif b.find("mean")>=0 and a.find("avg")>=0:
				agg_mapping.append(b)
				break
	oldColumns = sample.schema.names
	if len(aggregation_type)>1:
		agg_mapping1 = [w[0:len(w)-1] for w in agg_mapping]
	else:
		agg_mapping1 = [w for w in agg_mapping]
	newColumns=grp+agg_mapping1
	df = reduce(lambda sample, idx: sample.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), sample)
	return df





############################################## look back fxns using rolling logic ######################


def look_back_fxn(base_table,grp_clause_input,date_column_input,aggregation_input,aggregation_type_input,lookback_period_input,look_data_period_input):
	#### assign input values to function
	date_column=date_column_input
	lookback_period=lookback_period_input
	look_data_period=look_data_period_input
	grp_clause = copy.deepcopy(grp_clause_input)
	grp_clause.append("last_day")
	aggregation_col = []
	aggregation_col.append(aggregation_input)
	aggregation_type = copy.deepcopy(aggregation_type_input)
	temp_table=base_table.withColumn("last_day",last_day(date_column))
	temp_table.createOrReplaceTempView("temp_table")
	###### apply aggregation function
	abc=Aggregation_fxn(temp_table,grp_clause,aggregation_col,["min","max","sum","count"])
	####### changing the column names for output table
	oldColumns = abc.schema.names
	newColumns= [w+"_left" for w in oldColumns]
	abc_left = reduce(lambda abc, idx: abc.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), abc)
	columnsFirstDf = [w+"_left" for w in grp_clause_input]
	columnsSecondDf = grp_clause_input
	######## self join by applying date filter
	abc_self_join = date_filter_fxn(abc_left,"last_day_left",look_data_period).join(abc,[col(f) == col(s) for (f, s) in zip(columnsFirstDf, columnsSecondDf)],how="left")
	######## cache the table
	abc_self_join.cache()
	abc_self_join.count()
	abc_self_join= abc_self_join.withColumn("signal",when((datediff(col("last_day_left"),col("last_day"))>0) & (datediff(col("last_day_left"),col("last_day"))<(lookback_period*30+10)),1).otherwise(0))
	filtered_df= abc_self_join.filter(col("signal")==1)
	col_names = filtered_df.schema.names
	temp_cols = [w+"_left" for w in grp_clause]
	temp_cols1=[ w for w in col_names if (w.find("signal")==-1) & (w.find("left")==-1) & (w not in grp_clause)]
	final_cols = temp_cols+temp_cols1
	######## identifying the column names for aggregation fields
	count_col = [ w for w in temp_cols1 if w.find("__count")>0][0]
	sum_col = [ w for w in temp_cols1 if w.find("__sum")>0][0]
	max_col = [ w for w in temp_cols1 if w.find("__max")>0][0]
	min_col = [ w for w in temp_cols1 if w.find("__min")>0][0]
	dict_agg = {}
	dict_agg[count_col]="sum"
	dict_agg[sum_col]="sum"
	dict_agg[max_col]="max"
	dict_agg[min_col]="min"
	final_df = filtered_df.groupBy(temp_cols).agg(dict_agg)
	oldColumns = final_df.schema.names
	col_1= [ w for w in oldColumns if w not in temp_cols]
	newColumns_temp = temp_cols+["Pre"+str(lookback_period)+"_"+re.sub(r'_last_day','',re.sub(r'\)','',re.sub(r'.*\(','',w))) for w in col_1]
	newColumns=[re.sub(r'_left','',w) for w in newColumns_temp]
	final_df_1 = reduce(lambda final_df, idx: final_df.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), final_df)
	temp_cols_3 = final_df_1.schema.names
	count_col = [ w for w in temp_cols_3 if w.find("__count")>0][0]
	sum_col = [ w for w in temp_cols_3 if w.find("__sum")>0][0]
	max_col = [ w for w in temp_cols_3 if w.find("__max")>0][0]
	min_col = [ w for w in temp_cols_3 if w.find("__min")>0][0]
	final_df_1 = final_df_1.withColumn(sum_col.replace("sum","mean"),col(sum_col)/col(count_col))
	mean_col = [ w for w in final_df_1.schema.names if w.find("__mean")>0][0]
	drop_list = []
	drop_list.append(sum_col)
	drop_list.append(count_col)
	if "min" not in aggregation_type:
		drop_list.append(min_col)
	if "max" not in aggregation_type:
		drop_list.append(max_col)
	if "mean" not in aggregation_type:
		drop_list.append(mean_col)
	final_df_return_temp = final_df_1.drop(*drop_list)
	oldColumns = final_df_return_temp.schema.names
	newColumns=[re.sub(r'__','_',w) for w in oldColumns]
	final_df_return = reduce(lambda final_df_return_temp, idx: final_df_return_temp.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), final_df_return_temp)
	abc_self_join.unpersist()
	return final_df_return


#################################   risk score #######################################

def risk_score(base_table,categorical_col_list,dependent_variable):
	temp_table = base_table
	dependent_var=dependent_variable
	group_by_col=categorical_col_list
	a=''
	for w in categorical_col_list:
		a=a+w+"_"
	target_variable_name = "risk_"+a+"_"+dependent_variable
	df_stats = temp_table.select(mean(col(dependent_var)).alias('mean'),stddev(col(dependent_var)).alias('std')).collect()
	allw_amt_variance=temp_table
	overallcnt=allw_amt_variance.count()
	mean_overall = df_stats[0]['mean']
	std = df_stats[0]['std']
	overallvar=std*std
	allw_amt_variance.createOrReplaceTempView("allw_amt_variance")
	cpt_variance_temp = Aggregation_fxn(temp_table,group_by_col,[dependent_variable],["mean","std","count"])
	cpt_variance=cpt_variance_temp.fillna(0)
	oldColumns = cpt_variance.schema.names
	group_cols= [w for w in oldColumns if w.find("__")==-1]
	newColumns = group_cols + ["cntprocd","std_dev","procdavg"]
	cpt_variance = reduce(lambda cpt_variance, idx: cpt_variance.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), cpt_variance)
	cpt_variance= cpt_variance.withColumn("variance_procd",col("std_dev")*col("std_dev"))
	cpt_variance.createOrReplaceTempView("cpt_variance")
	cpt_variance= cpt_variance.withColumn("Overall_cnt",lit(overallcnt))
	cpt_variance= cpt_variance.withColumn("Overall_variance",lit(overallvar))
	score_proc= cpt_variance.withColumn("Overall_avg",lit(mean_overall))
	score_proc=score_proc.withColumn("weight_procd",(col('cntprocd') * col('Overall_variance'))/(col('variance_procd') + col('cntprocd') * col('Overall_variance')))
	score_proc=score_proc.withColumn('weight_procd', when(score_proc["weight_procd"].isNotNull(),col("weight_procd") ).otherwise(0))
	score_proc=score_proc.withColumn("Score_procd",(col("weight_procd") * col("procdavg") + (1 - col("weight_procd")) * col("Overall_avg")))
	score_proc=score_proc.withColumn(target_variable_name,round(col("Score_procd"),2))
	final_df= score_proc.drop(*["Score_procd","Overall_avg","overallcnt","Overall_variance","cntprocd","std_dev","procdavg","variance_procd","weight_procd","Overall_cnt"])
	categorical_col_list.append(target_variable_name)
	final_df_11= score_proc.select(categorical_col_list)
	final_df=final_df_11.dropDuplicates()
	oldColumns = final_df.schema.names
	newColumns=[re.sub(r'__','_',w) for w in oldColumns]
	final_df_return = reduce(lambda final_df, idx: final_df.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), final_df)
	return final_df_return

############################# COV fxn #######################################################################

def cov_fxn(base_table,categorical_col_list,dependent_variable):
	temp_table = base_table
	dependent_var=[]
	dependent_var.append(dependent_variable)
	group_by_col=categorical_col_list
	cov_table_temp = Aggregation_fxn(temp_table,group_by_col,dependent_var,["mean","std"])
	mean_col = [ w for w in cov_table_temp.schema.names if w.find("mean")>0][0]
	std_col = [ w for w in cov_table_temp.schema.names if w.find("std")>0][0]
	cov_table = cov_table_temp.withColumn(mean_col.replace("mean","cov"),(col(std_col)/col(mean_col))*100)
	drop_list=[]
	drop_list.append(mean_col)
	drop_list.append(std_col)
	cov_table_return1 = cov_table.drop(*drop_list)
	cov_table_return=cov_table_return1.fillna(0)
	return cov_table_return






################### weighted COV fxn ######################################################



def weighted_cov(base_table,weight_col_input,categorical_col_list,dependent_variable):
	temp_table = base_table
	dependent_var=[]
	dependent_var.append(dependent_variable)
	group_by_col=categorical_col_list
	weight_col=weight_col_input
	cov_table_temp = Aggregation_fxn(temp_table,group_by_col,dependent_var,["mean","std","sum"])
	mean_col = [ w for w in cov_table_temp.schema.names if w.find("mean")>0][0]
	std_col = [ w for w in cov_table_temp.schema.names if w.find("std")>0][0]
	sum_col = [ w for w in cov_table_temp.schema.names if w.find("sum")>0][0]
	cov_table = cov_table_temp.withColumn("COV_"+mean_col.replace("mean",""),(col(std_col)/col(mean_col)))
	cov_col = [ w for w in cov_table.schema.names if w.find("COV")>=0][0]
	weight_col_sum = Aggregation_fxn(temp_table,[weight_col],dependent_var,["sum"])
	sum_col_2 = [ w for w in weight_col_sum.schema.names if w.find("sum")>0][0]
	cov_table_join_wgt = data_join(cov_table,weight_col_sum,weight_col,"left")
	cov_table_join_wgt=cov_table_join_wgt.withColumn(cov_col,(col(sum_col)/col(sum_col_2))*col(cov_col))
	cov_table_join_wgt_imputed = cov_table_join_wgt.fillna(0)
	rollup_df = cov_table_join_wgt_imputed.groupby([weight_col]).agg({cov_col:"sum"})
	sum_col_final = [ w for w in rollup_df.schema.names if w.find("sum")>=0][0]
	balance = [ w for w in categorical_col_list if w !=weight_col ]
	q=''
	for i in range(len(balance)):
		if i==1:
			q=q+balance[i]
		else:
			q=q+"_"+balance[i]

	final_sum_col_temp= "COV_"+weight_col+"_"+q+"_"+dependent_variable
	final_sum_col=re.sub(r'__','_',final_sum_col_temp)
	oldColumns = rollup_df.schema.names
	newColumns= [weight_col]+[final_sum_col]
	cov_table_return = reduce(lambda rollup_df, idx: rollup_df.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), rollup_df)
	cov_table_return=cov_table_return.withColumn(final_sum_col,round(final_sum_col,3))
	return cov_table_return

def look_back_fxn_test(base_table,grp_clause_input,date_column_input,aggregation_input,aggregation_type_input,lookback_period_input,look_data_period_input):
	#### assign input values to function
	date_column=date_column_input
	lookback_period=lookback_period_input
	look_data_period=look_data_period_input
	grp_clause = copy.deepcopy(grp_clause_input)
	grp_clause.append("last_day")
	aggregation_col = []
	aggregation_col.append(aggregation_input)
	aggregation_type = copy.deepcopy(aggregation_type_input)
	temp_table=base_table.withColumn("last_day",last_day(date_column))
	temp_table.createOrReplaceTempView("temp_table")
	###### apply aggregation function
	abc=Aggregation_fxn(temp_table,grp_clause,aggregation_col,["min","max","sum","count"])
	####### changing the column names for output table
	oldColumns = abc.schema.names
	newColumns= [w+"_left" for w in oldColumns]
	abc_left = reduce(lambda abc, idx: abc.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), abc)
	columnsFirstDf = [w+"_left" for w in grp_clause_input]
	columnsSecondDf = grp_clause_input
	######## self join by applying date filter
	abc_self_join = date_filter_fxn(abc_left,"last_day_left",look_data_period).join(abc,[col(f) == col(s) for (f, s) in zip(columnsFirstDf, columnsSecondDf)],how="left")
	######## cache the table
	abc_self_join.cache()
	abc_self_join.count()
	abc_self_join= abc_self_join.withColumn("signal",when((datediff(col("last_day_left"),col("last_day"))>0) & (datediff(col("last_day_left"),col("last_day"))<(lookback_period*30+10)),1).otherwise(0))
	filtered_df= abc_self_join.filter(col("signal")==1)
	col_names = filtered_df.schema.names
	temp_cols = [w+"_left" for w in grp_clause]
	temp_cols1=[ w for w in col_names if (w.find("signal")==-1) & (w.find("left")==-1) & (w not in grp_clause)]
	final_cols = temp_cols+temp_cols1
	######## identifying the column names for aggregation fields
	count_col = [ w for w in temp_cols1 if w.find("__count")>0][0]
	sum_col = [ w for w in temp_cols1 if w.find("__sum")>0][0]
	max_col = [ w for w in temp_cols1 if w.find("__max")>0][0]
	min_col = [ w for w in temp_cols1 if w.find("__min")>0][0]
	dict_agg = {}
	dict_agg[count_col]="sum"
	dict_agg[sum_col]="sum"
	dict_agg[max_col]="max"
	dict_agg[min_col]="min"
	final_df = filtered_df.groupBy(temp_cols).agg(dict_agg)
	oldColumns = final_df.schema.names
	col_1= [ w for w in oldColumns if w not in temp_cols]
	newColumns_temp = temp_cols+["Pre"+str(lookback_period)+"_"+re.sub(r'_last_day','',re.sub(r'\)','',re.sub(r'.*\(','',w))) for w in col_1]
	newColumns=[re.sub(r'_left','',w) for w in newColumns_temp]
	final_df_1 = reduce(lambda final_df, idx: final_df.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), final_df)
	temp_cols_3 = final_df_1.schema.names
	count_col = [ w for w in temp_cols_3 if w.find("__count")>0][0]
	sum_col = [ w for w in temp_cols_3 if w.find("__sum")>0][0]
	max_col = [ w for w in temp_cols_3 if w.find("__max")>0][0]
	min_col = [ w for w in temp_cols_3 if w.find("__min")>0][0]
	final_df_1 = final_df_1.withColumn(sum_col.replace("sum","mean"),col(sum_col)/col(count_col))
	mean_col = [ w for w in final_df_1.schema.names if w.find("__mean")>0][0]
	drop_list = []
	if "min" not in aggregation_type:
		drop_list.append(min_col)
	if "max" not in aggregation_type:
		drop_list.append(max_col)
	if "mean" not in aggregation_type:
		drop_list.append(mean_col)
	if "sum" not in aggregation_type:
		drop_list.append(sum_col)
	if "count" not in aggregation_type:
		drop_list.append(count_col)
	final_df_return_temp = final_df_1.drop(*drop_list)
	oldColumns = final_df_return_temp.schema.names
	newColumns=[re.sub(r'__','_',w) for w in oldColumns]
	final_df_return = reduce(lambda final_df_return_temp, idx: final_df_return_temp.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), final_df_return_temp)
	abc_self_join.unpersist()
	return final_df_return

def entropy_lookback(base_table,parent_column_input,child_column_input,lookback_date,lookback_period,training_period):
	tall_table=base_table	
	parent_column=parent_column_input
	child_column=child_column_input
	cpt_mod_avg_lookback_6_1 = look_back_fxn_test(tall_table,[parent_column,child_column],lookback_date,child_column,["count"],lookback_period,training_period)
	cpt_mod_avg_lookback_6 = look_back_fxn_test(tall_table,[parent_column],lookback_date,child_column,["count"],lookback_period,training_period)
	cov_table_join_wgt = data_join(cpt_mod_avg_lookback_6_1,cpt_mod_avg_lookback_6,[parent_column,"last_day"],"inner") 
	num_count = [ w for w in cov_table_join_wgt.schema.names if w.find("_count")>0]
	if len(num_count[0])>len(num_count[1]):
		num_col=num_count[0]
		den_col=num_count[1]
	else:
		num_col=num_count[1]
		den_col=num_count[0]
	cov_table_join_wgt = cov_table_join_wgt.withColumn("Entropy_"+parent_column+"_"+child_column,((col(num_col)/col(den_col))*log((col(den_col)/col(num_col))))/lit(1))
	final_df = Aggregation_fxn(cov_table_join_wgt,[parent_column,"last_day"],["Entropy_"+parent_column+"_"+child_column],["sum"])
	num_count = [ w for w in final_df.schema.names if w.find("_sum")>0][0]
	oldColumns = final_df.schema.names
	newColumns=[parent_column,"last_day","Entropy_"+parent_column+"_"+child_column]
	final_df_return = reduce(lambda final_df, idx: final_df.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), final_df)
	return final_df_return


#base_table=df
#parent_column_input = "group_policy"
#child_column_input = "model_event"
#lookback_date="hd_import_date"

#risk_data = risk_standard111(base_table,parent_column_input,child_column_input,lookback_date,6,12)


def risk_standard(base_table,parent_column_input,child_column_input,lookback_date,lookback_period,training_period):
	tall_table=base_table	
	parent_column=parent_column_input
	child_column=child_column_input
	cpt_mod_avg_lookback_6_1 = look_back_fxn_test(tall_table,[parent_column,child_column],lookback_date,child_column,["count","sum"],lookback_period,training_period)
	cpt_mod_avg_lookback_6_1.createOrReplaceTempView("cpt_mod_avg_lookback_6_1")
	count_column = [ w for w in cpt_mod_avg_lookback_6_1.schema.names if w.find("_count")>0][0]
	event_colum =  [ w for w in cpt_mod_avg_lookback_6_1.schema.names if w.find("_sum")>0]
	risk_table = spark.sql('''
	select 
	a.'''+parent_column+''',
	a.last_day,
	(a.event_category + 0.5 )/(a.total_event_category + (0.5/(b.event_overall/b.total_population))) as risk_'''+parent_column+'''
	from 
	(
	select '''+parent_column+''',
	last_day, 
	sum('''+count_column+''') as total_event_category,
	sum('''+event_colum[0]+''') as event_category
	from cpt_mod_avg_lookback_6_1 group by '''+parent_column+''',last_day
	) a 
	left join 
	( select last_day,
	sum('''+count_column+''') as total_population,
	sum('''+event_colum[0]+''') as event_overall
	from cpt_mod_avg_lookback_6_1 group by last_day
	) b 
	on 
	trim(a.last_day)=trim(b.last_day)
	''')
	return risk_table

def risk_adjusted(base_table ,risk_column,event_column,dollar_column,lookback_column):
    df = base_table 
    df.createOrReplaceTempView("df")
    group_policy=risk_column
    hd_import_date = lookback_column 
    model_event = event_column
    amount_paid = dollar_column
    df2 = spark.sql('''  
    select 
    distinct 
    a.hd_import_mo_yr,
    a.'''+group_policy+''', 
    a.total_lines ,
    a.total_events, 
    a.total_events_paid, 
     b.hd_import_mo_yr_hist, 
    b.total_lines as total_lines_hist, 
    b.total_events as total_events_hist, 
    b.total_events_paid as total_events_paid_hist,
    (
    case when (floor(a.hd_import_mo_yr/100)=floor(b.hd_import_mo_yr_hist/100) and (b.hd_import_mo_yr_hist%100 between (a.hd_import_mo_yr%100)-6 and 
    (a.hd_import_mo_yr%100)-1) or  ((floor(a.hd_import_mo_yr/100)=1+floor(b.hd_import_mo_yr_hist/100)) and ((b.hd_import_mo_yr_hist%100)>(a.hd_import_mo_yr%100)+5)
    )) then 1 else 0 end) as Risk_Selection
    from
        (
        select 
        (year('''+hd_import_date+''')*100+month('''+hd_import_date+''')) as hd_import_mo_yr, 
        '''+group_policy+'''  ,
        count(*) as total_lines , 
        sum(case when '''+model_event+''' = 1 then 1 else 0 end) as total_events,
        sum(case when '''+model_event+''' = 1 then '''+amount_paid+''' else 0 end) as total_events_paid
        from  df
            group by (year('''+hd_import_date+''')*100+month('''+hd_import_date+''')) , '''+group_policy+'''
        ) a
    left join 
        (              
        select 
        (year('''+hd_import_date+''')*100+month('''+hd_import_date+''')) as hd_import_mo_yr_hist , '''+group_policy+''' , 
        count(*) as total_lines , sum(case when '''+model_event+''' = 1 then 1 else 0 end) as total_events,
        sum(case when '''+model_event+''' = 1 then '''+amount_paid+''' else 0 end) as total_events_paid
        from   df
            group by (year('''+hd_import_date+''')*100+month('''+hd_import_date+''')), '''+group_policy+'''
        ) b
    on a.'''+group_policy+''' = b.'''+group_policy)

    df2.createOrReplaceTempView("df2")
    df_2 = spark.sql('''select * from df2 where risk_selection = 1''')
    df_2.createOrReplaceTempView("df_2")

    df_grp_policy_risk = spark.sql(''' 
    select 
    a.hd_import_mo_yr,
    a.'''+group_policy+''',
    (a.total_errors_category/b.total_errors_history) as P2,
    (a.total_error_paid/a.total_occurances_category) as severity,
    (a.total_errors_category/b.total_errors_history)*(a.total_error_paid/a.total_occurances_category) as risk_score
    from 
    (	select 
        hd_import_mo_yr, '''+group_policy+''',  
        sum(total_events_hist) as total_errors_category,
        sum(total_events_paid_hist) as total_error_paid,
        sum(total_lines_hist) as total_occurances_category
            from  df_2
                group by hd_import_mo_yr, '''+group_policy+''' 
    ) a 
    left join 
    (
        select 
        hd_import_mo_yr, 
        sum(total_events_hist) as total_errors_history
            from  df_2
                group by hd_import_mo_yr 
    ) b 
    on
    trim(a.hd_import_mo_yr) = trim(b.hd_import_mo_yr)
    ''')

    return df_grp_policy_risk

def risk_standard(base_table,parent_column_input,child_column_input,lookback_date,lookback_period,training_period):
	tall_table=base_table	
	parent_column=parent_column_input
	child_column=child_column_input
	cpt_mod_avg_lookback_6_1 = look_back_fxn_test(tall_table,[parent_column,child_column],lookback_date,child_column,["count","sum"],lookback_period,training_period)
	cpt_mod_avg_lookback_6_1.createOrReplaceTempView("cpt_mod_avg_lookback_6_1")
	count_column = [ w for w in cpt_mod_avg_lookback_6_1.schema.names if w.find("_count")>0][0]
	event_colum =  [ w for w in cpt_mod_avg_lookback_6_1.schema.names if w.find("_sum")>0]
	risk_table = spark.sql('''
	select 
	a.'''+parent_column+''',
	a.last_day,
	(a.event_category + 0.5 )/(a.total_event_category + (0.5/(b.event_overall/b.total_population))) as risk_'''+parent_column+'''
	from 
	(
	select '''+parent_column+''',
	last_day, 
	sum('''+count_column+''') as total_event_category,
	sum('''+event_colum[0]+''') as event_category
	from cpt_mod_avg_lookback_6_1 group by '''+parent_column+''',last_day
	) a 
	left join 
	( select last_day,
	sum('''+count_column+''') as total_population,
	sum('''+event_colum[0]+''') as event_overall
	from cpt_mod_avg_lookback_6_1 group by last_day
	) b 
	on 
	trim(a.last_day)=trim(b.last_day)
	''')
	return risk_table



def lookback_median(base_table,risk_column,duration_column,lookback_column):
	df = base_table 
	df.createOrReplaceTempView("df")
	sbsb_id=risk_column
	hd_import_date = lookback_column 
	HospBillDuration = duration_column
	df_1 = spark.sql('''
	select 
	c.'''+sbsb_id+''',
	c.hd_import_mo_yr,
	round(percentile(c.total_lines_hist,0.5),3) as '''+sbsb_id+'''_'''+HospBillDuration+'''_p50 
	from 
	(
	select 
	distinct 
	a.hd_import_mo_yr,
	a.'''+sbsb_id+''', 
	b.hd_import_mo_yr_hist, 
	b.total_lines as total_lines_hist, 
	(
	case when 
	(
	(floor(a.hd_import_mo_yr/100)=floor(b.hd_import_mo_yr_hist/100) and (b.hd_import_mo_yr_hist%100 between (a.hd_import_mo_yr%100)-6 and 
	(a.hd_import_mo_yr%100)-1))
	or  
	((floor(a.hd_import_mo_yr/100)=1+floor(b.hd_import_mo_yr_hist/100)) and ((b.hd_import_mo_yr_hist%100)>(a.hd_import_mo_yr%100)+5))
	) 
	then 1 else 0 end) as Risk_Selection
	from
	(
	select distinct  
	(year('''+hd_import_date+''')*100+month('''+hd_import_date+''')) as hd_import_mo_yr, 
	'''+sbsb_id+'''
	from  df  
	) a
	left join 
	(
	select distinct 
	(year('''+hd_import_date+''')*100+month('''+hd_import_date+''')) as hd_import_mo_yr_hist, 
	'''+sbsb_id+''',
	'''+HospBillDuration+''' as total_lines 
	from   df  
	) b
	on a.'''+sbsb_id+'''=b.'''+sbsb_id+'''
	) c 
	where c.risk_selection = 1 
	group by 
	c.'''+sbsb_id+''',c.hd_import_mo_yr 
	''')
	return df_1

def Entropy_median_MVT(base_table,value_column,group_column):
	Entropy_map = base_table 
	Entropy_map.createOrReplaceTempView("Entropy_map")
	last_day = group_column 
	Entropy_MCTN_ID_line_of_business = value_column
	Entropy_MVT = spark.sql('''
	select 
	'''+last_day+''',
	round(percentile('''+Entropy_MCTN_ID_line_of_business+''',0.5),3) as '''+Entropy_MCTN_ID_line_of_business+'''_mvt_p50 
	from 
	Entropy_map group by '''+last_day+'''
	''')
	return Entropy_MVT
