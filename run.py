## INCLUDE MODULES
import logging
from glob import glob
import os
import sys
import pandas as pd
from sqlalchemy import create_engine
import yaml

# Reading Configuration from Yaml file
with open('config.yaml') as f:
    cfg = yaml.load(f, Loader=yaml.FullLoader)
    f.close()

#Setting the threshold of logger
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

verifiedFiles = []
unverifiedFiles = []

# Reading csv and txt files from input directory
allFiles = glob(os.path.join(cfg["INPUTDIR"], "*.csv")) + glob(os.path.join(cfg["INPUTDIR"], "*.txt"))
logging.info("Found %d files in Dir: %s" % (len(allFiles), os.getcwd()))

if (not len(allFiles)):
    logging.info("Exiting Application Now")
    sys.exit(0)

# Setting Warning level to true
ignoreWarning = True

# Verifying the Records types in input files
for filePath in allFiles:
    filePath = os.path.abspath(filePath)
    with open(filePath, "r") as fp:
        fileLines = fp.readlines()
        lineCount = len(fileLines) - 2
        if (not fileLines[0].startswith("|H|")):
            logging.warning("No Header Record found in File: %s" % filePath)
            lineCount += 1
            ignoreWarning = False
            unverifiedFiles.append(filePath)
        if (not fileLines[-1].startswith("|T|")):
            logging.warning("No Trailer Record found in File: %s" % filePath)
            lineCount += 1
            ignoreWarning = False
            unverifiedFiles.append(filePath)
        if (fileLines[-1].startswith("|T|") and (lineCount != int(fileLines[-1].split("|")[-2]))):
            logging.warning("Number of Records Not Matching according to Trailer Record in File: %s" % filePath)
            ignoreWarning = False
            unverifiedFiles.append(filePath)
    if filePath not in unverifiedFiles:
        verifiedFiles.append(filePath)

# Condition to proceed with verified or unverified files        
if(not ignoreWarning):
    ignoreWarningInput = input("Continue with Warnings [Y/N] (default: Y): ")
    if (ignoreWarningInput != "N"):
        ignoreWarning = True

if (ignoreWarning):
    files = verifiedFiles + unverifiedFiles
else:
    files = verifiedFiles

# Creating an Empty Pandas DataFrame
df = pd.DataFrame()

# Appending Records to Empty Pandas DataFrame
for filePath in files:
    if (filePath in verifiedFiles):
        df = df.append(pd.read_csv(filePath,sep="|", header=None, skiprows=[0], skipfooter=1, usecols=lambda col: col > 1, engine='python', ), ignore_index=True)
    else:
        if (not fileLines[0].startswith("|H|")):
            df = df.append(pd.read_csv(filePath,sep="|", header=None, skipfooter=1, usecols=lambda col: col > 1, engine='python', ), ignore_index=True)
        if (not fileLines[-1].startswith("|T|")):
            df = df.append(pd.read_csv(filePath,sep="|", header=None, skiprows=[0], usecols=lambda col: col > 1, engine='python', ), ignore_index=True)

# Renaming DataFrame Columns with Schema
df.rename(columns = {2:'Customer Name'}, inplace = True)
df.rename(columns = {3:'Customer ID'}, inplace = True)
df.rename(columns = {4:'Customer Open Date'}, inplace = True)
df.rename(columns = {5:'Last Consulted Date'}, inplace = True)
df.rename(columns = {6:'Vaccination Type'}, inplace = True)
df.rename(columns = {7:'Doctor Consulted'}, inplace = True)
df.rename(columns = {8:'State'}, inplace = True)
df.rename(columns = {9:'Country'}, inplace = True)
df.rename(columns = {10:'Date of Birth'}, inplace = True)
df.rename(columns = {11:'Active Customer'}, inplace = True)

df.insert(8, 'Post Code', None)

df = df.drop_duplicates(subset=['Customer Name'])
df['Date of Birth']= pd.to_datetime(df['Date of Birth'], format='%d%m%Y')
df['Date of Birth'] = df['Date of Birth'].dt.strftime('%Y%m%d')

# Creating SQL Engine
engine = create_engine('mysql://%s:%s@%s:%d/%s' % (cfg["USERNAME"],cfg["PASSWORD"],cfg["HOST"],cfg["PORT"],cfg["DBNAME"]), echo=False)

# Create Table Query based on country
for country in df['Country'].unique():
    createQuery = '''CREATE TABLE IF NOT EXISTS `{}`.`{}`(
    `Customer Name` VARCHAR(255) NOT NULL,
    `Customer ID` VARCHAR(18) NOT NULL,
    `Customer Open Date` DATE NOT NULL,
    `Last Consulted Date` DATE NULL,
    `Vaccination Type` CHAR(5) NULL,
    `Doctor Consulted` CHAR(255) NULL,
    `State` CHAR(5) NULL,
    `Country` CHAR(5) NULL,
    `Post Code` INT(5) NULL,
    `Date of Birth` DATE NULL,
    `Active Customer` CHAR(1) NULL,
    PRIMARY KEY(`Customer Name`)
) ENGINE = INNODB;'''.format(cfg["DBNAME"], country)

    engine.execute(createQuery)
    logging.info("Successfully: {} Table Created".format(country))
    temp = df[df.Country.eq(country)]
    for i in range(len(temp)):
        try:
            temp.iloc[i:i+1].to_sql(country, con=engine, if_exists='append', index=False)            
        except:
            logging.error("Primary Key: {} Already Exists".format(temp.iloc[i]['Customer Name']))

logging.info("Task Completed!")    