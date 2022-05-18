#!/usr/bin/env python
# coding: utf-8

import os
import glob
import pandas as pd

raw_files_per_city = []

cwd = os.path.curdir
root_dir = os.path.abspath(os.path.join(cwd, ".."))
# use absolute paths to avoid confusion, even though they require a little more cleanup at times
data_path = os.path.join(root_dir, "raw")
data_path


pattern = os.path.join(data_path, "Vornamen_*.csv")
file_paths = [os.path.split(x) for x in glob.glob(pattern)]
file_paths

cities = list(set([x[1].split("_")[1] for x in file_paths]))

# this is the standardized column format, that all other formats will be converted to
std_columns = ['name', 'gender', 'count', 'position',
               'state', 'district', 'city', 'borough',
               'year', 'sourcefile']

def convert(city, year, df):
    if city == 'Berlin':
        return convert_berlin(year, df)
    elif city == 'Muenchen':
        return convert_muenchen(year, df)
    elif city == 'Koeln':
        return convert_koeln(year, df)
    elif city == 'Leipzig':
        return convert_leipzig(year, df)
    else:
        raise Exception("unknown city '" + city + "'")

def verify_columns(columns):
    cols = list(columns)
    cols.sort()

    std = list(std_columns)
    std.sort()
    if cols != std:
        raise Exception("Converted columns dont match the standard column format! Expected: " +
                        " ".join(std) + 
                        ", Actual: " + " ".join(cols))

def convert_berlin(year, df):
    mapping_2012_till_2016 = {
        "vorname": "name",
        "geschlecht": "gender",
        "anzahl": "count",
        # "jahr": "year",
        "File": "borough", # TODO: not nice, but this currently contains the original source filename, which is the berlin borough name
        "sourcefile": "sourcefile",
    }
    if 'jahr' in df.columns:
        df.drop(columns=['jahr'], inplace=True)
    columns_2012_till_2016 = list(mapping_2012_till_2016.keys())
    columns_2012_till_2016.sort()
    mapping_2017_till_2021 = {"position": "position"}
    mapping_2017_till_2021.update(mapping_2012_till_2016)
    columns_2017_till_2021 = list(mapping_2017_till_2021.keys())
    columns_2017_till_2021.sort()
    
    columns = list(df.columns)
    columns.sort()
    
    # print("columns", columns, "\ncolumns_2012_till_2016", columns_2012_till_2016, "\nmapping_2017_till_2021", mapping_2017_till_2021)

    if int(year) in [2012, 2013, 2014, 2015, 2016]:
        df_ = df.rename(columns=mapping_2012_till_2016, copy=True)
    elif int(year) in [2017, 2018, 2019, 2020, 2021]:
        df_ = df.rename(columns=mapping_2017_till_2021, copy=True)
    else:
        raise Exception("Unknown columns for Berlin in the year " + str(year) + "! Got: " + " ".join(columns))
    df_['state'] = 'Berlin'
    df_['district'] = None
    df_['city'] = 'Berlin'
    
    verify_columns(df_.columns)
    return df_


def convert_muenchen(year, df):
    if int(year) == 2015:
        df.rename(columns={"vornamen": "vorname"}, inplace=True)
    
    mapping = {
        "vorname": "name",
        "geschlecht": "gender",
        "anzahl": "count",
        "jahr": "year",    
    }
    df_ = df.rename(columns=mapping, copy=True)
    df_['state'] = 'Bayern'
    df_['district'] = 'Oberbayern'
    df_['borough'] = None
    verify_columns(df_.columns)
    return df_

def convert_koeln(year, df):
    mapping = {
        "vorname": "name",
        "geschlecht": "gender",
        "anzahl": "count",
        "jahr": "year",
    }
    df_ = df.rename(columns=mapping, copy=True)
    df_['state'] = 'Nordrhein-Westfalen'
    df_['district'] = None
    df_['borough'] = None
    verify_columns(df_.columns)
    return df_

def convert_leipzig(year, df):
    # Leipzig has an especially annoying format because they thought of the data as rankings
    
    """
    Leipzig 2014 ('D:\\tmp\\data-prenames-germany\\raw', 'Vornamen_Leipzig_2014.csv')
            Columns: 'Rang' 'Anzahl' 'Vorname' 'Geschlecht' '-' 'Rang.1' 'Anzahl.1' 'Vorname.1' 'Geschlecht.1'
    ...
    Leipzig 2019 ('D:\\tmp\\data-prenames-germany\\raw', 'Vornamen_Leipzig_2019.csv')
            Columns: 'Rang' 'Anzahl' 'Vorname' 'Geschlecht' '-' 'Rang.1' 'Anzahl.1' 'Vorname.1' 'Geschlecht.1'
    Leipzig 2020 ('D:\\tmp\\data-prenames-germany\\raw', 'Vornamen_Leipzig_2020.csv')
            Columns: 'Rang' 'Anzahl' 'Vorname' 'Geschlecht' '-' 'Rang1' 'Anzahl1' 'Vorname1' 'Geschlecht1'
    Leipzig 2021 ('D:\\tmp\\data-prenames-germany\\raw', 'Vornamen_Leipzig_2021.csv')
            Columns: 'anzahl' 'vorname' 'geschlecht' 'position'"""
    
    if int(year) in [2014, 2015, 2016, 2017, 2018, 2019, 2020]:
        df.drop(columns=['-'], inplace=True)

        # this is built exactly like the previous years, but they renamed righ side of the columns
        if int(year) == 2020:
            df.rename(columns={"Anzahl1": "Anzahl.1", "Vorname1": "Vorname.1", "Geschlecht1": "Geschlecht.1", "Rang1": "Rang.1"}, inplace=True)
        
        df.drop(columns=['Rang', 'Rang.1'], inplace=True)
        
        # now we need to get the data from the right side, to the bottom into the same columns
        
        # first clean up the left side
        df_left = df.drop(columns=["Anzahl.1", "Vorname.1", "Geschlecht.1"])
        
        # create the right side and adjust column names
        df_right = df.drop(columns=["Anzahl", "Vorname", "Geschlecht"])
        df_right.rename(columns={"Anzahl.1": "Anzahl", "Vorname.1": "Vorname", "Geschlecht.1": "Geschlecht"}, inplace=True)
        
        df_ = df_left.append(df_right, ignore_index=True, sort=True)
        
        # this is a little weird, could partially rename directly in previous steps, but this way
        # it's a little more understandable, I think
        df_.rename(columns={"Anzahl": "count", "Vorname": "name", "Geschlecht": "gender"}, inplace=True)
    elif int(year) == 2021:
        df_ = df.rename(columns={'anzahl': 'count', 'vorname': 'name', 'geschlecht': 'gender'}, copy=True)
    else:
        raise Exception("unknown year " + year + " for leipzig, fix the code!")
    df_['borough'] = None
    df_['district'] = None
    df_['state'] = 'Sachsen'
   
    verify_columns(df_.columns)
    return df_

dfs_by_city = {}
for f in file_paths:
    parts = f[1].split("_")
    city = parts[1]
    year = int(parts[2].rstrip(".csv"))
    print(city, year, f)
    fullpath = os.path.join(f[0], f[1])
    df = pd.read_csv(fullpath, sep=None, engine='python')
    print("            Columns: '" + "' '".join(df.columns) + "'")
    df['year'] = year
    df['city'] = city
    df['sourcefile'] = fullpath
    if 'position' not in df.columns:
        df['position'] = None
    
    # convert to standardized columns:
    df_standardized = convert(city, year, df)
    
    if not city in dfs_by_city:
        dfs_by_city[city] = []
    dfs_by_city[city].append(df_standardized)

dfs_full_by_city = {}
dfs_list = []
for city, dfs in dfs_by_city.items():
    print(city, len(dfs))
    dfs_full_by_city[city] = pd.concat(dfs, ignore_index=True)
    dfs_list.append(dfs_full_by_city[city])
    output_file = os.path.join(root_dir, "Vornamen_" + city + "_Gesamt.csv")
    print("writing", output_file, "...")
    dfs_full_by_city[city].to_csv(output_file, index=False)

dfs_all = pd.concat(dfs_list, ignore_index=True)

d = dfs_all.drop(columns="sourcefile")

output_file = os.path.join(root_dir, "Vornamen_Deutschland_Gesamt.csv")
print("writing", output_file, "...")
d.to_csv(output_file, index=False)
