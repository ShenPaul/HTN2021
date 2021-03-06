#https://cloud.ibm.com/docs/Cloudant?topic=Cloudant-creating-and-populating-a-simple-ibm-cloudant-database-on-ibm-cloud
from cloudant.client import Cloudant
from cloudant.error import CloudantException
from cloudant.result import Result, ResultByKey
import os
import json
from datetime import datetime #https://www.programiz.com/python-programming/datetime/strftime
import pandas as pd

client = None
covidInfoURL = 'https://raw.githubusercontent.com/ShenPaul/HTN2021/main/conposcovidloc.csv'

def main():
    with open('dbuploadcred.json') as f:
        creds = json.load(f)

    serviceUsername = creds['username']
    servicePassword = creds['password']
    serviceURL = creds['url']

    client = Cloudant(serviceUsername, servicePassword, url=serviceURL)
    client.connect()

    databaseName = "coviddb"

    covidDatabase = client.create_database(databaseName)

    if covidDatabase.exists():
        print("'{0}' successfully created.\n".format(databaseName))

    df = pd.read_csv(covidInfoURL)

    df['Accurate_Episode_Date'] = pd.to_datetime(df['Accurate_Episode_Date'])
    df['Case_Reported_Date'] = pd.to_datetime(df['Case_Reported_Date'])
    df['Test_Reported_Date'] = pd.to_datetime(df['Test_Reported_Date'])
    df['Specimen_Date'] = pd.to_datetime(df['Specimen_Date'])

    df.fillna('No Data', inplace=True)

    #df.drop(df.index[0:58], inplace=True)

    for row in df.itertuples(index=False, name=None):
        if row[2] != 'No Data':
            Accurate_Episode_DateField = row[2].strftime('%Y-%m-%d')
        else:
            Accurate_Episode_DateField = row[2]
        if row[3] != 'No Data':
            Case_Reported_DateField = row[3].strftime('%Y-%m-%d')
        else:
            Case_Reported_DateField = row[3]
        if row[4] != 'No Data':
            Test_Reported_DateField = row[4].strftime('%Y-%m-%d')
        else:
            Test_Reported_DateField = row[4]
        if row[5] != 'No Data':
            Specimen_DateField = row[5].strftime('%Y-%m-%d')
        else:
            Specimen_DateField = row[5]


        jsonDocument = {
            # "_idField": row[0],
            "Row_IDField": row[1],
            "Accurate_Episode_DateField": Accurate_Episode_DateField,
            "Case_Reported_DateField": Case_Reported_DateField,
            "Test_Reported_DateField": Test_Reported_DateField,
            "Specimen_DateField": Specimen_DateField,
            "Age_GroupField": row[6].replace('s', ''),
            "Client_GenderField": row[7],
            "Case_AcquisitionInfoField": row[8],
            "Outcome1Field": row[9],
            "Outbreak_RelatedField": row[10],
            "Reporting_PHU_IDField": row[11],
            "Reporting_PHUField": row[12],
            "Reporting_PHU_AddressField": row[13],
            "Reporting_PHU_CityField": row[14],
            "Reporting_PHU_Postal_CodeField": row[15],
            "Reporting_PHU_WebsiteField": row[16],
            "Reporting_PHU_LatitudeField": row[17],
            "Reporting_PHU_LongitudeField": row[18]
        }

        newDocument = covidDatabase.create_document(jsonDocument)

        # Check that the documents exist in the database.
        if newDocument.exists():
            print(row[0])

if __name__=='__main__':
    main()
