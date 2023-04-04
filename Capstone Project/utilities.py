# Do all imports and installs here
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql.functions import udf,to_date
from pyspark.sql.functions import isnan, when, count, col
import pyspark.sql.functions as f


def check_duplicates(df):
    """
    Given a dataframe check if it contain duplication
    """
    gp_dup = df.groupBy(df.columns).agg((f.count("*")>1).cast("int").alias("e")).where(col("e")>0)
    if gp_dup.count()==0:
        return False
    else:
        return True

def check_quality(df,name):
    '''
    Input: spark dataframe
    
    Output: print the check output
    
    '''
    
    nb_line = df.count()
    if nb_line == 0:
        print("Data quality check failed for this dataframe {} with zero records".format(name))
    else:
        print("Data quality check passed for this dataframe {} with {} records".format(name,nb_line))

def plot_missing_values(nulldf,df):
    """Given a dataframe , visualize the missing values by columns
    :param: two dfs : 1. first dataframe contains contains column names and number of null values 2. the main dataframe used to  count how many rows we are working with 
    :return: line chart
    """
    # calculate % missing values
    nulldf['% missing values'] = 100*nulldf['values']/df.count()
    plt.rcdefaults()
    plt.figure(figsize=(10,5))
    ax = sns.barplot(x="cols", y="% missing values", data=nulldf)
    ax.set_ylim(0, 100)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
    plt.show()


def columns_missing_values_90(nulldf,df):
    """Given a dataframe return column names that has a percentage greater than or equal to 90% of missing values
    """
    # calculate % missing values
    nulldf['% missing values'] = 100*nulldf['values']/df.count()
    return nulldf[nulldf['% missing values']>=90]['cols'].tolist()