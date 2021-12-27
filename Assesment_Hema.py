#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import os


# In[2]:


spark=SparkSession.builder.getOrCreate()


# In[3]:


spark.conf.set('spark.sql.legacy.timeParserPolicy','LEGACY')


# <b> RAW LAYER </b>

# In[4]:


def read_data(input_path):
    raw_df=spark.read.csv(input_path,header=True)
    return raw_df

def add_metadata(df):
    df=df.withColumn('file_name',F.element_at(F.split(F.input_file_name(),'/'),-1)).            withColumn('load_date_time',F.current_timestamp())
    cols_replaced=[colm.replace(' ','_').replace('-','_') for colm in df.columns]
    df=df.toDF(*cols_replaced)
    return df

def write_df(df,zone,output_path,category,write_mode,run_date):

    location=f'{output_path}/{zone}/{category}/Year={run_date.year}/Month={run_date.month}/Day={run_date.day}'
    df.coalesce(1).write.option('compression','GZIP').parquet(location,mode=write_mode)

def main():
    zone='raw'
    run_date=datetime.now().date()
    input_path='C:/Users/souvi/Downloads/hema/data.csv'
    output_path='C:/Users/souvi/Downloads/data lake' 
    raw_df=read_data(input_path)
    raw_df=add_metadata(raw_df)
    write_mode='append'
    category='sales_customers'
    write_df(raw_df,zone,output_path,category,write_mode,run_date)
    
if __name__=='__main__':
    main()


# In[5]:


df=spark.read.parquet('C:/Users/souvi/Downloads/data lake/raw/sales_customers')
df.show(1,vertical=True)


# <b> CURATED LAYER </b>

# In[6]:


def read_data(input_path):
    raw_df=spark.read.parquet(input_path)
    return raw_df

def apply_case(raw_df):
    
    cols=raw_df.columns
    lower_cols=[col.lower() for col in cols]
    col_list_updated=[]
    for colm in lower_cols:
        col_list=colm.split('_')
        if(len(col_list)>1):
            col_case=[colm.capitalize() for colm in col_list[1:]]
            col_case=list(col_list[0])+col_case
        else:
            col_case=col_list
        col_list_updated.append(''.join(col_case))
    curated_df=raw_df.toDF(*col_list_updated)
    return curated_df

def apply_schema(df,colm,expr):
    df=df.withColumn(colm,F.expr(expr))
    return df

def write_df(df,zone,output_path,category,write_mode,run_date):

    location=f'{output_path}/{zone}/{category}/Year={run_date.year}/Month={run_date.month}/Day={run_date.day}'
    df.coalesce(1).write.option('compression','GZIP').parquet(location,mode=write_mode)
    
def main():
    zone='curated'
    category='sales_customers'
    run_date=datetime.now().date()
    input_path=f'C:/Users/souvi/Downloads/data lake/raw/{category}/Year={run_date.year}/Month={run_date.month}/Day={run_date.day}'
    output_path='C:/Users/souvi/Downloads/data lake'
    raw_df=read_data(input_path)
    curated_df=apply_case(raw_df)
    date_cols=['orderDate','shipDate']
    for colm in date_cols:
        expr=f'''to_timestamp({colm},'dd/MM/yyyy') '''
        curated_df=apply_schema(curated_df,colm,expr)
    timstamp_cols=['loadDateTime']
    for colm in timstamp_cols:
        expr=f'''to_timestamp({colm},'dd/MM/yyyy hh:mm:ss') '''
        curated_df=apply_schema(curated_df,colm,expr)
    #write data to curated layer
    write_mode='append'
    write_df(curated_df,zone,output_path,category,write_mode,run_date)
    
if __name__=='__main__':
    main()


# In[7]:


curated_df=spark.read.parquet('C:/Users/souvi/Downloads/data lake/curated/sales_customers')


# In[8]:


curated_df.show(1,vertical=True,truncate=False)


# <b> CONSUMPTION LAYER </b>

# In[12]:


config='''

{
"SALES" : {
                "category_name" : "Sales",
                "query" : "select distinct orderId,orderDate,shipDate,shipMode,city,fileName,loadDateTime from sales_customers",
                "load_mode" : "append"
                },
"CUSTOMERS"   : {
                "category_name" : "Customers",
                "query" : "with 5_days as     (select customerId,     count(distinct orderId) as quantityOfOrders_5_days     from sales_customers where     date(orderDate) between date_sub(current_date,5) and date(current_date)     group by customerId),     15_days as     (select customerId ,     count(distinct orderId) as quantityOfOrders_15_days     from sales_customers where     date(orderDate) between date_sub(current_date,15) and date(current_date)     group by customerId),     30_days as     (select customerId,     count(distinct orderId) as quantityOfOrders_30_days     from sales_customers where     date(orderDate) between date_sub(current_date,30) and date(current_date)     group by customerId),     total as     (select customerId,      count(distinct orderId) as totalQuantityOfOrders     from sales_customers     group by customerId),customer_data as     (select c.customerId, c.customerName,     split(customerName,' ')[0] as customerFirstName,     split(customerName,' ')[1] as customerLastName,     c.segment as customerSegment,     c.country,     c.city,     c1.quantityOfOrders_5_days,     c2.quantityOfOrders_15_days,     c3.quantityOfOrders_30_days,     c4.totalQuantityOfOrders,     fileName,loadDateTime,row_number() over(partition by c.customerId order by c.loadDateTime desc) as rnk     from sales_customers c left outer join 5_days c1 on (c.customerId=c1.customerId)     left outer join 15_days c2 on (c.customerId=c1.customerId)     left outer join 30_days c3 on (c.customerId=c3.customerId)     left outer join total c4 on (c.customerId=c4.customerId) ) select customerId,customerName,customerFirstName,customerLastName,customerSegment,country,city,quantityOfOrders_5_days,quantityOfOrders_15_days,quantityOfOrders_30_days,totalQuantityOfOrders,fileName,loadDateTime from customer_data where rnk=1",
    "load_mode" : "overwrite"
                }            
}
'''


# In[13]:


import json

def read_data(input_path):
    raw_df=spark.read.parquet(input_path)
    return raw_df

def apply_transformation(df,curated_category,query):
    
    df.createOrReplaceTempView(f'{curated_category}')
    consumption_df=spark.sql(query)
    return consumption_df

def write_df(df,zone,output_path,category,write_mode,run_date):
    
    if(write_mode=='append'):
        location=f'{output_path}/{zone}/{category}/Year={run_date.year}/Month={run_date.month}/Day={run_date.day}'
    elif(write_mode=='overwrite'):
        location=f'{output_path}/{zone}/{category}/'
    df.coalesce(1).write.option('compression','GZIP').parquet(location,mode=write_mode)

def main(config):
    zone='consumption'
    curated_category='sales_customers'
    config= json.loads(config)
    consumption_categories=list(config.keys())
    run_date=datetime.now().date()
    input_path=f'C:/Users/souvi/Downloads/data lake/curated/{curated_category}/Year={run_date.year}/Month={run_date.month}/Day={run_date.day}'
    output_path='C:/Users/souvi/Downloads/data lake'
    curated_df=read_data(input_path)
    for category in consumption_categories:
        transformation_sql=config[category]['query']
        write_mode=config[category]['load_mode']
        consumption_df=apply_transformation(curated_df,curated_category,transformation_sql)
        write_df(consumption_df,zone,output_path,category,write_mode,run_date)

if __name__=='__main__':
    main(config)


# <b> Final Data validation </b>

# In[16]:


cdf=spark.read.parquet('C:/Users/souvi/Downloads/data lake/consumption/CUSTOMERS')


# In[17]:


cdf.printSchema()


# In[18]:


cdf.count()


# In[19]:


sdf=spark.read.parquet('C:/Users/souvi/Downloads/data lake/consumption/SALES')


# In[20]:


sdf.printSchema()


# In[21]:


sdf.count()


# In[22]:


raw_df=spark.read.csv('C:/Users/souvi/Downloads/hema/data.csv',header=True)


# In[23]:


raw_df.count()


# In[24]:


raw_df.select('Order ID').distinct().count()


# In[67]:


raw_df.select('Customer ID').distinct().count()


# In[68]:


curated_df=spark.read.parquet('C:/Users/souvi/Downloads/data lake/curated/sales_customers/')


# In[104]:


curated_df.count()

