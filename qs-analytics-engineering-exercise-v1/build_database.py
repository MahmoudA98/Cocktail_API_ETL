"""
Author: Mahmoud
Date: 26/05/2022
Description: 
    - Reads in the datafiles
    - Imports the relevant data from the cocktails database API
    - Generates the data for the database
    - Creates the database and tables using the data_tables SQL script
    - Imports the data to the database
    - Runs the poc_tables SQL script
"""

import os
import pandas as pd
from requests import request
from sqlalchemy import create_engine

def execute_sql_from_file(path):
    """Reads commands from a .sql file and executes them. requires a pre defined engine"""
    data_tables = open('data_tables.sql', 'r')
    sqlFile = data_tables.read()
    data_tables.close()
    sqlCommands = sqlFile.split(';')
    for command in sqlCommands:
        connection = engine.raw_connection()
        cursor = connection.cursor()
        cursor.execute(command)
        connection.commit()
        connection.close()

def request_data(search_term):
    """Imports data from the cocktails database API. returns a Dataframe"""
    url = r"https://www.thecocktaildb.com/api/json/v1/1/search.php?s="+search_term
    response = request("GET", url)
    api_data = response.json()
    df_main = pd.DataFrame(api_data['drinks'])
    df_main.columns = [c[3:] if c.startswith(('str')) else c for c in df_main.columns] #Just removing 'str' from the front of those columns headers.
    df_main.drop(columns=['IBA', 'Video','ImageSource', 'ImageAttribution', 'DrinkAlternate'], inplace=True) # can be derived from other columns or unnescessary.
    return(df_main)

def create_drinks_df(df_in):
    """Takes in the Dataframe from the API and returns a drinks Dataframe"""
    df_drinks = df_in[['idDrink', 'Drink', 'Category', 'Alcoholic','Glass']]
    df_drinks['Alcoholic'] = df_drinks['Alcoholic'].map({'Alcoholic': 1, 'Non_Alcoholic': 0})
    return df_drinks

def create_tags_df(df_in):
    """Takes a Dataframe and returns a tags dataframe"""
    df = df_in[['idDrink', 'Tags']]
    df= df.set_index(df.columns.drop('Tags',1).tolist()).Tags.str.split(',', expand=True).stack().reset_index().rename(columns={0:'Tags'}).loc[:, df.columns]
    df.rename(columns={'idDrink':'FK_DrinkID'}, inplace = True)
    return df

def create_instructions_df(df_in):
    """Takes in a drinks Dataframe from the API and returns an instuctions Dataframe"""
    if 'idDrink' not in df_in.columns:
        raise KeyError("Dataframe missing idDrink column")
    inst_table_columns = [c for c in df_in.columns if 'Instructions' in c]
    inst_table_columns.append('idDrink')
    df_temp = df_in[inst_table_columns]
    df_temp = pd.melt(frame=df_temp,id_vars=["idDrink"])
    df_temp.rename(columns={'variable':'Language', 'value':'Instructions'}, inplace = True)
    df_temp = df_temp[df_temp.Instructions.notnull()] # I would also change the Language column to Language, but will come back to this later.
    df_temp.rename(columns={'idDrink':'FK_DrinkID'}, inplace = True)
    return df_temp

def create_recipe_df(df_in):
    """Takes in a drinks Dataframe from the API and returns a measurments Dataframe"""
    if 'idDrink' not in df_in.columns:
        raise KeyError("Dataframe missing idDrink column")
    #process ingredients
    ingredient_cols = [c for c in df_in.columns if 'Ingredient' in c]
    ingredient_cols.append('idDrink')
    df_ingredient = df_in[ingredient_cols]
    df_ingredient.columns = [c[10:] if 'Ingredient' in c else c for c in df_ingredient.columns ]
    df_ingredient = pd.melt(frame=df_ingredient,id_vars=["idDrink"])
    df_ingredient.rename(columns={'variable':'Step', 'value':'Ingredient'}, inplace = True)
    #process measurements
    measure_cols = [c for c in df_in.columns if 'Measure' in c]
    measure_cols.append('idDrink')
    df_measurements = df_in[measure_cols]
    df_measurements.columns = [c[7:] if 'Measure' in c else c for c in df_measurements.columns]
    df_measurements = pd.melt(frame=df_measurements,id_vars=["idDrink"])
    df_measurements.rename(columns={'variable':'Step', 'value':'Measurement'}, inplace = True)
    # join
    df_combined = pd.merge(left=df_ingredient, right= df_measurements, how='inner',on=['idDrink', 'Step'])
    df_combined = df_combined[df_combined.Ingredient.notnull()]
    df_combined.rename(columns={'idDrink':'FK_DrinkID'}, inplace = True)
    return df_combined

def Create_Glass_tables(glass_type):
    """Takes an array of glass names and creates a df with GlassID & Name"""
    df = pd.DataFrame({"Name": list(set(glass_type))})
    df.drop_duplicates(inplace=True)
    df["GlassID"] = df.index + 1
    return(df)

def create_stock_df(df_in):
    """takes the df from bar_data and returns a normalised df"""
    bar_df = df_in.copy()
    bar_df['bar'] = bar_df['bar'].map({'london': 1, 'budapest': 2, 'new york': 3})
    glass_lkp = pd.read_sql_query('select * from Glass', engine)
    bar_df = pd.merge(left = bar_df, right= glass_lkp, how='left',left_on='glass_type', right_on='Name')
    bar_df = bar_df[['GlassID', 'bar', 'stock']]
    bar_df.rename(columns={'GlassID':'FK_GlassID', 'bar':'FK_BarID'}, inplace = True)
    return bar_df

#@lru_cache(maxsize=None)
def read_transaction_data():
    """Reads Transactions and returns a Dataframe"""
    col_names = ['TransactionID', 'TransactionTime', 'Item', 'Amount']
    budapest = pd.read_csv('data/budapest.csv.gz', compression='gzip',skiprows=1, names=col_names)
    london = pd.read_csv('data/london_transactions.csv.gz',delimiter='\t', compression='gzip',names=col_names)
    ny = pd.read_csv('data/ny.csv.gz', compression='gzip',skiprows=1,names=col_names)
    london['FK_BarID'] =1
    budapest['FK_BarID'] =2
    ny['FK_BarID'] =3
    Transactions_df = pd.concat([budapest, london, ny])
    Transactions_df.drop(columns=['TransactionID'], inplace=True)
    Transactions_df.sort_values(by='TransactionTime', inplace=True)
    return Transactions_df


if __name__ == '__main__':
    path = os.getcwd()
    engine = create_engine(f'sqlite:////{path}/data/Bar.db')
    execute_sql_from_file('data_tables.sql') #creates the tables
    # fetch the data from the api
    df_main = request_data('margarita') # just getting a sample for now
    # read the provided data from the data folder
    bar_data = pd.read_csv('data/bar_data.csv')

    # start with the glass table
    glass_df = Create_Glass_tables(bar_data["glass_type"])
    glass_df.to_sql(name = 'Glass', con=engine, index=False, if_exists='append')

    # now the stock table
    stock_df = create_stock_df(bar_data)
    stock_df.to_sql(name = 'Stock', con=engine, index=False, if_exists='append')

    # now the drinks table -- I would change Glass column to glassID but cleaning needed and I will come back to this later
    drinks_df = create_drinks_df(df_main)
    drinks_df.to_sql(name = 'Drink', con=engine, index=False, if_exists='append')

    # now the tags table
    tags_df = create_tags_df(df_main)
    tags_df.to_sql(name = 'Drink_Tags', con=engine, index=False, if_exists='append')

    # now the instructions table
    instructions_df = create_instructions_df(df_main)
    instructions_df.to_sql(name = 'Drink_Instructions', con=engine, index=False, if_exists='append')

    # now the recipe table
    recipe_df = create_recipe_df(df_main)
    recipe_df.to_sql(name = 'Drink_Recipe', con=engine, index=False, if_exists='append')

    # now read the transactions data from the data folder
    Transactions_df = read_transaction_data()
    Transactions_df.to_sql(name = 'Transactions', con=engine, index=False, if_exists='append')
    print('In all honesty, I already spent >3 hours on this so far. I value the rest of my sunday too much. there is ALOT I would change even before getting started on the PoC table.')