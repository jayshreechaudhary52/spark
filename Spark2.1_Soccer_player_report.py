
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("soccer_player").getOrCreate()


# In[6]:


df = spark.read.format("csv").option("inferSchema", True).option("header",True).load("player.csv")


# In[8]:


df.show()
df.printSchema()


# In[13]:


player_attr_df = spark.read.format("csv").option("header",True).option("inferSchema",True).load("player_attributes.csv")


# In[15]:


player_attr_df.show(5)
player_attr_df.printSchema()


# In[17]:


player_attr_df.count()


# In[18]:


df.count()


# In[47]:


df = df.drop('id','player_fifa_api_id')


# In[48]:


player_attr_df = player_attr_df.drop('id','player_fifa_api_id','preferred_foot','attacking_work_rate','defensive_work_rate','crossing','jumping','sprint_speed','balance','aggression','short_passing','potential')


# In[49]:


df.dropna()


# In[50]:


player_attr_df.dropna()


# In[51]:


from pyspark.sql.functions import udf
year_extract_udf = udf(lambda date : date.year)


# In[52]:


player_attr_df = player_attr_df.withColumn('year',year_extract_udf(player_attr_df.date))


# In[53]:


player_attr_df.show(2)


# In[54]:


player_attr_df = player_attr_df.drop('date')


# In[55]:


player_attr_df.columns


# In[56]:


## find best strikers based on some of the attributes
## then join with player df to get names of the players
## how to find best striker :- from recent data 2016 year {finishing : avg, shot_power : avg, acceleration : avg}
## weitage for considering each attribute for best striker is weight_finish=1,weight_shot_power=2,weight_acceleration=1
## total weight = sum of all weights


# In[57]:


filtered_plyr_attr = player_attr_df.filter(player_attr_df['year'] == 2016)


# In[58]:


filtered_plyr_attr.count()


# In[59]:


player_attr_df.count()


# In[60]:


#get distinct players count
filtered_plyr_attr.select('player_api_id').distinct().count()


# In[61]:


#group each player attr and find avg of each selecting attribute
filtered_plyr_attr = filtered_plyr_attr.groupBy('player_api_id').agg({'finishing':'avg','shot_power':'avg','acceleration':'avg'})


# In[70]:


filtered_plyr_attr.count()
filtered_plyr_attr = filtered_plyr_attr.withColumnRenamed('avg(finishing)','finishing').withColumnRenamed('avg(shot_power)','shot_power').withColumnRenamed('avg(acceleration)','acceleration')


# In[72]:


## finding score for each player
weight_finish=1
weight_shot_power=2
weight_acceleration=1
total_weight = weight_finish+weight_shot_power+weight_acceleration
filtered_plyr_attr.printSchema()


# In[75]:


filtered_plyr_attr = filtered_plyr_attr.withColumn('score',(filtered_plyr_attr['finishing']*weight_finish + filtered_plyr_attr['acceleration']*weight_acceleration+filtered_plyr_attr['shot_power']*weight_shot_power)/total_weight)


# In[76]:


filtered_plyr_attr.show(2)


# In[87]:


striker_details = filtered_plyr_attr.drop('finishing','acceleration','shot_power')


# In[88]:


striker_details.show(2)


# In[89]:


striker_details = striker_details.filter(filtered_plyr_attr['score'] > 70)


# In[90]:


best_strikers = striker_details.join(df,['player_api_id']).sort(striker_details['score'].desc())


# In[91]:


best_strikers.show(2)


# In[97]:


# performing join by broadcasting dataframes.
from pyspark.sql.functions import broadcast
best_strikers_1 = df.select(['player_api_id','player_name']).join(broadcast(striker_details),['player_api_id'],'inner')


# In[100]:


best_strikers_1.sort(striker_details.score.desc()).show(2)


# In[102]:


# check if height of player results in better heading accuracy
player_attr_df.count(), df.count()


# In[103]:


# join to get height and heading_accuracy
player_heading_acc = player_attr_df.select('player_api_id','heading_accuracy').join(broadcast(df),['player_api_id'],'inner')


# In[120]:


player_heading_acc.printSchema()
player_heading_acc.count()

player_heading_acc = player_heading_acc.dropna()


# In[121]:


# categorize each player into different height bucket
short_count = spark.sparkContext.accumulator(0)
medium_short_count = spark.sparkContext.accumulator(0)
medium_high_count = spark.sparkContext.accumulator(0)
tall_count = spark.sparkContext.accumulator(0)

# accumulator for heading accuracy count ha
short_ha_count = spark.sparkContext.accumulator(0)
medium_short_ha_count = spark.sparkContext.accumulator(0)
medium_high_ha_count = spark.sparkContext.accumulator(0)
tall_ha_count = spark.sparkContext.accumulator(0)


# In[122]:


def categorize (row, threashold_score):
    height = float(row.height)
    ha = float(row.heading_accuracy)
    
    if(height <= 175 ):
        short_count.add(1)
        if(ha > threashold_score):
            short_ha_count.add(1)
            
    if(height <= 183 and height > 175 ):
        medium_short_count.add(1)
        if(ha > threashold_score):
            medium_short_ha_count.add(1)
            
    if(height <= 195 and height > 183):
        medium_high_count.add(1)
        if(ha > threashold_score):
            medium_high_ha_count.add(1)
            
    if(height > 195 ):
        tall_count.add(1)
        if(ha > threashold_score):
            tall_ha_count.add(1)


# In[123]:


player_heading_acc.foreach(lambda x : categorize(x,60))


# In[125]:


total_acc = [short_count,medium_short_count,medium_high_count,tall_count]


# In[126]:


total_acc


# In[127]:


total_acc_ha = [short_ha_count,medium_short_ha_count,medium_high_ha_count,tall_ha_count]


# In[128]:


total_acc_ha


# In[136]:


# find the percent of accuracy
player_heading_acc.printSchema()
percent_with_value = [(short_ha_count.value/short_count.value)*100 , (medium_short_ha_count.value/medium_short_count.value)*100, (medium_high_ha_count.value/medium_high_count.value)*100,(tall_ha_count.value/tall_count.value)*100 ]


# In[137]:


percent_with_value


# In[138]:


# saving data as csv or json
plyr_2016 = player_attr_df.filter(player_attr_df['year'] == 2016)


# In[145]:


plyr_2016.select('player_api_id','overall_rating').coalesce(1).write.option('header',True).csv('player_2016.csv')


# In[146]:


plyr_2016.select('player_api_id','overall_rating').write.option('header',True).json('player_2016.json')


# In[150]:


## sql query on data spark
from pyspark.sql.types import Row
from datetime import datetime

record = sc.parallelize([Row(id=1,
                            name ="jill",
                            active= True,
                            clubs=['hockey','chess'],
                            subjects={'math':80,'english':56},
                            enrolled=datetime(2014,8,1,14,1,5)),
                        Row(id=2,
                            name="George",
                            active= False,
                            clubs=['soccer','chess'],
                            subjects={'math':60,'english':96},
                            enrolled=datetime(2015,3,21,8,2,5))])


# In[151]:


record_df = record.toDF()


# In[152]:


record_df.printSchema()


# In[153]:


record_df.show()


# In[154]:


record_df.createOrReplaceTempView('records')


# In[155]:


all_records_df = sqlContext.sql('select * from records')


# In[156]:


all_records_df.show()


# In[158]:


sqlContext.sql("select id, clubs[1],subjects['english'] from records").show()


# In[159]:


sqlContext.sql("select * from records where active = True").show()


# In[160]:


sqlContext.sql("select id, NOT active from records").show()


# In[161]:


# to access a table on all spark sessions in this cluster, register table as global table
record_df.createGlobalTempView('global_records')


# In[164]:


sqlContext.sql("select * from global_temp.global_records").show()


# In[169]:


# processing airline data with sql
airline = spark.read.format('csv').option('header',True).option('inferSchema',True).load('airlines.csv')
flights = spark.read.format('csv').option('header',True).option('inferSchema',True).load('flights.csv')


# In[170]:


airline.createOrReplaceTempView('airlines')
flights.createOrReplaceTempView('flights')


# In[173]:


sqlContext.sql("select * from airlines").show()


# In[174]:


flights.count(),airline.count()


# In[175]:


sqlContext.sql("select count(*) from airlines").show()


# In[181]:


spark.sql("select distance from flights").agg({'distance':'sum'}).withColumnRenamed('sum(distance)', 'total').show()


# In[183]:


# get all delayed flight in 2012
spark.sql("select date, airlines, flight_number from flights where departure_delay > 0 and year(date) = 2012").show()


# In[185]:


# get all delayed flight in 2014
spark.sql("select date, airlines, flight_number, date from flights where departure_delay > 0 and year(date) = 2014").show()


# In[186]:


#inferred and explicit schema
lines = sc.textFile('students.txt')
lines = lines.map(lambda x : x.split(','))


# In[187]:


lines


# In[188]:


lines.collect()


# In[199]:


rdd = lines.map(lambda x : Row(name=x[0],math=int(x[1]),science=int(x[2]),english=int(x[3])))


# In[200]:


rdd


# In[201]:


rdd.collect()


# In[202]:


# converting rdd to DF
df = rdd.toDF()


# In[203]:


df.printSchema()


# In[204]:


dfUsingSpark = spark.createDataFrame(rdd)


# In[205]:


dfUsingSpark.printSchema()


# In[206]:


dfUsingSpark.columns


# In[207]:


dfUsingSpark.schema


# In[208]:


df.schema


# In[212]:


from pyspark.sql.types import StructType, StructField, StringType, LongType


# In[213]:


fields = [StructField("name",StringType(),True),
          StructField('math',LongType(),True), 
          StructField('english',LongType(),True),
          StructField('science',LongType(),True)]


# In[215]:


schema = StructType(fields)


# In[216]:


rdd.collect()


# In[219]:


withSchema = rdd.toDF(schema)


# In[221]:


withSchema.printSchema()


# In[223]:


spark.createDataFrame(rdd,schema).show()

