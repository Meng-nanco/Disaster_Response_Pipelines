import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

def load_data(messages_filepath, categories_filepath):
    '''
    Load messages.csv and categories.csv into dataframes, and merge them to one
    dataset based on common id.

    Parameters
    ----------
    messages_filepath : string
        the filepath of messages.csv
    categories_filepath : string
        the filepath of categories.csv

    Returns
    -------
    df : pandas.DataFrame
        The combined dataset
    '''
    messages = pd.read_csv(messages_filepath)
    categories = pd.read_csv(categories_filepath)
    df = pd.merge(messages, categories, how='inner', on='id')

    return df


def clean_data(df):
    '''
    Clean the dataframe by spliting categories column into separate category
    columns, and remove duplicated data.

    Parameters
    ----------
    df : pandas.DataFrame
        the data

    Returns
    -------
    df : pandas.DataFrame
        the cleaned data
    '''
    #Create new DataFrame 'categories' with separate category columns
    categories = df.categories.str.split(';', expand=True)

    #Rename column names for categories
    categories.columns = categories.iloc[0].apply(lambda x: x[:-2]).to_list()

    #Convert category values to just numbers 0 or 1
    for column in categories:
        categories[column] = categories[column].apply(lambda x: x[-1]).astype(int)

    #Replace the original categories column with new categories dataframe
    df = df.drop(columns=['categories'])
    df = pd.concat([df, categories], axis=1)

    #Remove duplicates
    df = df.drop_duplicates()

    return df



def save_data(df, database_filepath, tablename):
    '''
    Save the clean dataset into an sqlite database.

    Parameters
    ----------
    df : pandas.DataFrame
        the cleaned dataset
    database_filepath : string
        the filepath of the database
    tablename : string
        the tablename for the cleaned data to be saved in the database
    '''
    engine = create_engine('sqlite:///{}'.format(database_filepath))
    df.to_sql(tablename, engine, index=False, if_exists='replace')


def main():
    if len(sys.argv) == 5:

        messages_filepath, categories_filepath, database_filepath, tablename = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)

        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        print('TABLENAME: {}'.format(tablename))
        save_data(df, database_filepath, tablename)

        print('Cleaned data saved to database!')

    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database and tablename to save the cleaned data '\
              'to as the third and fourth argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db disaster_messages')


if __name__ == '__main__':
    main()
