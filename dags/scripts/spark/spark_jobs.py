"import required pyspark libraries"
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
'import path libraries'
import os
from os.path import dirname as up
import pandas as pd

def get_data(filename):
    """Function to get the json files

    Args:
        filename (string): name of the json file to read

    Retruns:
        df(spark dataframe): requested dataframe
    """
    #get the the file path and read the file
    filepath = up(up(os.getcwd())) + '/data/' + filename
    df = spark.read.json(filepath)
    
    return df


def transform_users(output):
    """Function to transform users.json into csv.

    Args:
        output (string): name of the output location
    """
    #get the file and filter it
    df_users = get_data('user.json')
    df_users = df_users.select(
        'user_id', 'name', 'review_count', 'yelping_since','average_stars')

    #output path
    output = output + '/user.csv'
    #save the file
    df_users.toPandas().to_csv(output, index = False)


def transform_business(output):
    """Function to transform business.json into csv.

    Args:
        output (string): name of the output location
    """
    #get the file and filter it
    df_business = get_data('business.json')
    df_business = df_business.select(
                            'business_id', 'name', 'city', 'state',
                            'stars', 'review_count', 'is_open'
                            )
    #output path
    output = output + '/business.csv'
    #save the file
    df_business.toPandas().to_csv(output, index = False)


def transform_review(output):
    """Function to transform review.json into csv.

    Args:
        output (string): name of the output location
    """
    df_review = get_data('review.json')
    df_review = df_review.limit(1000).select(
                                    'business_id', 'user_id', 
                                    'date', 'text')
    #output path
    output = output + '/review.csv'
    #save the file
    df_review.toPandas().to_csv(output, index = False)


def transform_tips(output):
    """Function to transform tips.json into csv.

    Args:
        output (string): name of the output location
    """
    df_tip = get_data('tips.json')
    df_tip = df_tip.select('business_id', 'user_id', 'date', 'text')

    #output path
    output = output + '/tip.csv'
    #save the file
    df_tip.toPandas().to_csv(output, index = False)


def map_files():
    """Function to map user id and business id
    """

    folderpath = up(up(os.getcwd())) + '/clean/'
    #read the files
    df_users = pd.read_csv(folderpath + 'user.csv')
    df_business = pd.read_csv(folderpath + 'business.csv')
    df_review = pd.read_csv(folderpath + 'review.csv')
    df_tip = pd.read_csv(folderpath + 'tip.csv')

    #create dict
    business_dict = dict(zip(df_business.business_id, df_business.index))
    user_dict = dict(zip(df_users.user_id, df_users.index))
    
    #map the values
    df_business['business_id'] = df_business.business_id.map(business_dict)
    df_users['user_id'] = df_users.user_id.map(user_dict)
    df_tip['business_id'] = df_tip.business_id.map(business_dict)
    df_tip['user_id'] = df_tip.user_id.map(user_dict)
    df_review['business_id'] = df_review.business_id.map(business_dict)
    df_review['user_id'] = df_review.user_id.map(user_dict)

    #map dates
    df_users['yelping_since'] = pd.to_datetime(df_users['yelping_since']).dt.date
    df_tip['date'] = pd.to_datetime(df_tip['date']).dt.date
    df_review['date'] = pd.to_datetime(df_review['date']).dt.date
    
    #save the files
    df_business.to_csv(folderpath + 'business.csv' ,index = False, header=False)
    df_users.to_csv(folderpath + 'user.csv' ,index = False, header=False)
    df_tip.to_csv(folderpath + 'tip.csv' ,index = False, header=False)
    df_review.to_csv(folderpath + 'review.csv' ,index = False, header=False)

def main():
    """Main function to call all other functions
    """
    output = up(up(os.getcwd())) + '/clean/'
    transform_users(output)
    transform_business(output)
    transform_tips(output)
    transform_review(output)
    map_files()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Random Text Classifier").getOrCreate()
    main()
    spark.stop()