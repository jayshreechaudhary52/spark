
# coding: utf-8

# In[1]:


sc


# In[8]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("london_crime").getOrCreate()


# In[9]:


spark


# In[93]:


df = spark.read.format("csv").option("header", True).option('inferSchema',True).load("londom_crime.csv")


# In[95]:


df.printSchema()
df.take(10)


# In[96]:


df.count()


# In[97]:


df.drop('NA')


# In[98]:


df_new.count()


# In[99]:


df = df_new.drop("lsoa_code")


# In[100]:


df.take(10)


# In[101]:


totaleBorough = df.select('borough').distinct()
totaleBorough.show()


# In[102]:


totaleBorough.count()


# In[103]:


hackeneydata = df.filter(df['borough']=='Hackney')
hackeneydata.count()


# In[104]:


yearfilterdf = df.filter((df['year'] == '2016') | (df['year'] == '2015'))
yearfilterdf.count()


# In[105]:


#using isin operator
yearIsInfilterdf=df.filter((df['year']).isin(['2015','2016']))
yearIsInfilterdf.count()


# In[106]:


df.filter(df['year'] >= 2014).show()


# In[107]:


## performing aggregation on data
#group all convictions by borough
df.groupBy('borough').count().show()


# In[108]:


boroughValueSum = df.groupBy('borough').agg({'value':'sum'})
boroughValueSum = df.groupBy('borough').agg({'value':'sum'}).withColumnRenamed('sum(value)','conviction')
boroughValueSum.show()


# In[109]:


totalconviction = boroughValueSum.agg({'conviction':'sum'})


# In[110]:


totalconviction.show()


# In[111]:


totalconcivtionno=totalconviction.collect()[0][0]


# In[112]:


totalconcivtionno


# In[117]:


#find % of conviction on per borough basis
processeddf = boroughValueSum.withColumn('percent',(boroughValueSum.conviction/totalconcivtionno)*100)
processeddf.orderBy(processeddf[2].desc()).show()


# In[118]:


df.take(10)


# In[122]:


monthlyGroupeddf = df.filter(df['year'] == 2014).groupBy(df['month']).agg({'value':'sum'}).withColumnRenamed('sum(value)','total')


# In[129]:


totalcovictions = monthlyGroupeddf.agg({'total':'sum'}).collect()[0][0]


# In[130]:


totalcovictions


# In[132]:


monthlyGroupeddf.withColumn('percent',(monthlyGroupeddf.total/totalcovictions)*100).show()


# In[137]:


import pyspark.sql.functions as func


# In[149]:


updateddf = monthlyGroupeddf.withColumn('percent',func.round((monthlyGroupeddf.total/totalcovictions)*100,2))
updateddf.printSchema()
updateddf.show()


# In[150]:


#other aggregations
## convictions based on category in london
df.show()


# In[156]:


df.groupBy('major_category').agg({'value':'sum'}).withColumnRenamed('sum(value)','totalValue').orderBy('totalValue').show()


# In[159]:


#use of min and max aggregations
df_year = df.select('year')
df_year.agg({'year':'min'}).show()
df_year.agg({'year':'max'}).show()


# In[161]:


df_year.describe().show()


# In[163]:


df.crosstab('borough','major_category').show()


# In[164]:


df.crosstab('borough','major_category').select('borough_major_category','Burglary','Fraud or Forgery','Robbery').show()


# In[166]:


import matplotlib.pyplot as plt
plt.style.use('ggplot')


# In[179]:


def describe_year(year):
    filtereddf = df.filter(df['year'] == year).agg({'value':'sum'}).withColumnRenamed('sum(value)','convictions')
    burough_list = [x[0] for x in filtereddf.toLocalIterator()]
    conviction_list = [x[1] for x in filtereddf.toLocalIterator() ]
   
    plt.title('Crime for the year:'+year,frontsize=30)
    plt.xlable('Boroughs',fontsize=30)
    plt.ylable('Convictions',fontsize=30)

    plt.xtics(rotation=90,frontsize=30)
    plt.ytics(frontsize=30)
    plt.autoscale()
    
    plt.figure(figsize=(33,10))
    plt.bar(burough_list,conviction_list)
    plt.xtic
    plt.show()
    
     
    


# In[197]:


def describe_year(year):
    filtereddf = df.filter(df['year'] == year).groupBy('borough').agg({'value':'sum'}).withColumnRenamed('sum(value)','convictions')
    burough_list = [x[0] for x in filtereddf.toLocalIterator()]
    conviction_list = [x[1] for x in filtereddf.toLocalIterator() ]
    
    plt.title('Crime for the year:'+year,frontsize=30)
    plt.xlable('Boroughs',fontsize=30)
    plt.ylable('Convictions',fontsize=30)

    plt.xtics(rotation=90,fontsize=30)
    plt.ytics(frontsize=30)
    plt.autoscale()
    
    plt.figure(figsize=(33,10))
    plt.bar(burough_list,conviction_list)
    plt.xtic
    plt.show()
   
   


# In[199]:


describe_year('2014')

