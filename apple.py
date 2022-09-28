import pandas as pd

'''
=====================================================
FILE IMPORT ROUND 1 -- CHANGE FILENAMES AS NECESSARY
=====================================================
'''
#link to encoura report - CHANGE THIS
encouraSFreport='~/Downloads/encoura923.csv'
#link to output file - CHOOSE NEW FILENAME
editfile='~/Downloads/encoura_09-23-2022.csv'
#link to source tracking 1st file - CHANGE THIS
sourceTracking1='~/Downloads/srcA923.csv'
#link to source tracking 2nd file - CHANGE THIS
sourceTracking2='~/Downloads/srcB923.csv'
#link to removal file in csv format
remove='~/Downloads/remove.csv'
#link to IUIE report - CHANGE THIS
dssfile='~/Downloads/apple923.csv'
#read files into dataframes
theContent=pd.read_csv(encouraSFreport, sep=",", encoding="latin_1",low_memory=False)
st=pd.read_csv(sourceTracking1, sep=",", encoding="latin_1",low_memory=False)
st2=pd.read_csv(sourceTracking2, sep=",", encoding="latin_1",low_memory=False)
#combine the source tracking files
tc=pd.concat([st,st2],sort=False)
#de-dup them
tc=tc.drop_duplicates(subset=['Full Name', 'Salesforce ID','Breadcrumb','Capture Source'])
#release the original separate frames from memory
del st
del st2
'''
=====================================================
CONVERT GENDER TO  DIMENSION Male/Female/Unknown only
=====================================================
'''
#Define all correct non-unknown leading charcters with all cases
allprefs=tuple(['M','F','m','f'])
#Define male and female lead chars
femprefs=tuple(['f','F'])
maleprefs=tuple(['m','M'])
#convert all falsy gneder rows to 'Unknown'
theContent.loc[theContent['Gender'].isna(),'Gender']='Unknown'
#Use lead char tuples to convert Male and Female rows
theContent.loc[theContent.loc[:,'Gender'].str.startswith(femprefs),'Gender']='Female'
theContent.loc[theContent.loc[:,'Gender'].str.startswith(maleprefs),'Gender']='Male'
#convert remainders to unknown
theContent.loc[~theContent.loc[:,'Gender'].str.startswith(allprefs),'Gender']='Unknown'
#State abbrev dict from web
us_state_to_abbrev = {
    "AlacontentMergea": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District of Columbia": "DC",
    "American Samoa": "AS",
    "Guam": "GU",
    "Northern Mariana Islands": "MP",
    "Puerto Rico": "PR",
    "United States Minor Outlying Islands": "UM",
    "U.S. Virgin Islands": "VI",
    "ILLINOIS\xa0":"IL",
}
#convert dictionary to upper case 
us_state_to_abbrev= {k.upper():v.upper() for k,v in us_state_to_abbrev.items()}
#convert state column to upper
theContent.loc[:, 'Mailing State/Province']=theContent.loc[:, 'Mailing State/Province'].str.upper()
#break salesforce IDs into 5 char chunks
theContent['SFID5']=theContent.loc[:,'Contact ID'].str[4::-1]
theContent['SFID10']=theContent['Contact ID'].str[9:4:-1]
theContent['SFID15']=theContent['Contact ID'].str[14:9:-1]
#convert uppercase letters into 1 and lower/numerals into 0
theContent['SFID5']=theContent['SFID5'].str.replace(r'[a-z0-9]', '0',regex=True)
theContent['SFID5']=theContent['SFID5'].str.replace(r'[A-Z]', '1',regex=True)
theContent['SFID10']=theContent['SFID10'].str.replace(r'[a-z0-9]', '0',regex=True)
theContent['SFID10']=theContent['SFID10'].str.replace(r'[A-Z]', '1',regex=True)
theContent['SFID15']=theContent['SFID15'].str.replace(r'[a-z0-9]', '0',regex=True)
theContent['SFID15']=theContent['SFID15'].str.replace(r'[A-Z]', '1',regex=True)
#Cypher for salesforce 15 to 18 conversion
cyph='ABCDEFGHIJKLMNOPQRSTUVWXYZ012345'
#convert chunks from binary to decimal and use cypher
def binToDec(binary):
    tt=0
    for i in range(len(binary)):
        tt+=(2**(4-i))*int(binary[i])
    return cyph[tt]
#apply fn to vectors
theContent['SFID5']=theContent['SFID5'].apply(binToDec)
theContent['SFID10']=theContent['SFID10'].apply(binToDec)
theContent['SFID15']=theContent['SFID15'].apply(binToDec)
#fix US states
mask = theContent['Mailing State/Province'].isin(us_state_to_abbrev.keys())
theContent.loc[mask, 'Mailing State/Province'] = theContent.loc[mask, 'Mailing State/Province'].map(us_state_to_abbrev)
#Concatenate contact ID and cypher chars to make 18 char SFID
theContent['SFID18']=theContent['Contact ID']+theContent['SFID5']+theContent['SFID10']+theContent['SFID15']
#remove "chunk" columns
theContent.drop(['SFID5', 'SFID10','SFID15'], axis=1, inplace=True)
#copy term code into Entry term and year for vector calculation
theContent['ENTRY TERM']=theContent['Term Code']
theContent['ENTRY YEAR']=theContent['Term Code']
#copy 'Last School Attended Graduation Date' into HS Graduate year for manipulation
theContent['HS GRADUATE YEAR']=theContent['Last School Attended Graduation Date']
#fn to convert term code into name
def termCalc(tcode):
    if(str(tcode)[3])=='8':
        return 'Fall'
    elif(str(tcode)[3])=='5':
        return 'Summer'
    elif(str(tcode)[3])=='2':
        return 'Spring'
#fn to convert term code into year
def yearCalc(tcode):
    return '20'+str(tcode)[1:3]
#fn to get year datepart from grad date
def gradYearCalc(gDate):
    if str(type(pd.to_datetime(gDate)))!="<class 'pandas._libs.tslibs.nattype.NaTType'>":
        return str(pd.to_datetime(gDate, infer_datetime_format=True).year)
#apply functions
theContent['ENTRY TERM']=theContent['ENTRY TERM'].apply(termCalc)
theContent['ENTRY YEAR']=theContent['ENTRY YEAR'].apply(yearCalc)
theContent['HS GRADUATE YEAR']=theContent['HS GRADUATE YEAR'].apply(gradYearCalc)
#release 
#import remove people
removeMe=pd.read_csv(remove, sep=",", encoding="latin_1",low_memory=False)
#merge with source tracking 
contentMerge=pd.merge(theContent,tc,how="left",left_on='SFID18', right_on='Salesforce ID')
#release source tracking df from memory
del tc
#remove the "remove" people
contentMerge = contentMerge[~contentMerge.SFID18.isin(removeMe['Related Record ID'])]
#release "removes" from memory
del removeMe
'''
==============================
CORRECT THE COLUMNS
==============================
'''
contentMerge.insert(0,"ADDRESS2","")
contentMerge=contentMerge.rename(columns={"Created Date_y": "SRCDATE", "Referral Source Code - Prospect":"SRCCODE","Mailing Street":"ADDRESS1",'Created Date_x':'Created Date'})
contentMerge['INQUIRED']=contentMerge['SRCDATE']
contentMerge.insert(0,'COMPLETED',"")
contentMerge.insert(0,"ADMITTED","") 
contentMerge.insert(0,"CONFIRMED","")
contentMerge.insert(0,"ENROLLED","") 
contentMerge.insert(0,"CANCELED","") 
contentMerge.insert(0,"DROPPED","") 
contentMerge.insert(0,"GRADUATED","") 
contentMerge.insert(0,"STUDENTATHLETE","")
contentMerge.insert(0,"ACADEMICPROGRAM","")
contentMerge=contentMerge[['Contact ID', 'Last Name', 'First Name', 'Gender', 'HS GPA - 4PT','ADDRESS1', 'ADDRESS2', 'Mailing City', 'Mailing State/Province', 'Mailing Zip/Postal Code', 'Birthdate', 'ENTRY TERM', 'ENTRY YEAR', 'Term Code', 'HS GRADUATE YEAR', 'Breadcrumb', 'SRCCODE', 'SRCDATE', 'Capture Source', 'Created Date', 'INQUIRED', 'Application Date - Applicant', 'COMPLETED', 'ADMITTED', 'CONFIRMED', 'ENROLLED', 'CANCELED', 'DROPPED', 'GRADUATED', 'ACADEMICPROGRAM', 'Admit Type', 'Institution', 'STUDENTATHLETE', 'Email', 'Current Action Date - Applicant','Stage', 'IU Admissions ID', 'IUID', 'Last School Attended Graduation Date', 'Admit Date - Applicant', 'Stage Date', 'Stage Date - Prospect', 'Stage Date - Withdrawn', 'Stage Date - Applicant', 'Stage Date - Committed', 'Stage Date - Admitted', 'Stage Date - Matriculated', 'Stage Date - Denied','Plan Description', 'Program Code', 'Program Description', 'Action - Applicant','Action Reason - Applicant', 'Admit Type - Applicant', 'Admit Type - Prospect', 'Recruiting Center', 'Recruiting Center - Prospect', 'SFID18','Full Name', 'Source Tracking Name', 'Source Tracking ID', 'Salesforce ID','Origin Indicator', 'Public Group']]
'''
===============================================
FILE IMPORT ROUND 2 -- CHANGE AS NECESSARY
===============================================
'''
dss=pd.read_csv(dssfile, sep=",", encoding="latin_1",low_memory=False)
de=dss.drop(['University ID','Academic Plan Description','Institution Code','Admit Type Code','Admit Term Code','Academic Year','Campus Code'], axis=1)
#create compound key
print(de.columns)
de['CompoundKey']=de['Primary Last Name']+de['Primary First Name']+de['Birthdate']
contentMerge['CompoundKey']=contentMerge['Last Name']+contentMerge['First Name']+contentMerge['Birthdate']
contentMerge=pd.merge(contentMerge,de,how="left",on='CompoundKey')
matrs=pd.unique(contentMerge[contentMerge['Appl Program Action Code'].isin(['MATR','DEIN'])].loc[:,'CompoundKey'])
wapps=pd.unique(contentMerge[contentMerge['Appl Program Action Code'].isin(['WADM','WAPP'])].loc[:,'CompoundKey'])
admts=pd.unique(contentMerge[contentMerge['Appl Program Action Code']=='ADMT'].loc[:,'CompoundKey'])
contentMerge['CONFIRMED']=contentMerge[contentMerge['CompoundKey'].isin(matrs)].loc[:,'Appl Program Action Date']
contentMerge['CANCELED']=contentMerge[contentMerge['CompoundKey'].isin(wapps)].loc[:,'Appl Program Action Date']
contentMerge['ADMITTED']=contentMerge[contentMerge['CompoundKey'].isin(admts)].loc[:,'Appl Program Action Date']
contentMerge['ENROLLED']=contentMerge.loc[:,'Derived Enrollment Status Code']
contentMerge['COMPLETED']=contentMerge.loc[:,'Appl Completed Date']
contentMerge=contentMerge.drop(['Stage','IU Admissions ID','IUID','Last School Attended Graduation Date','Admit Date - Applicant','Stage Date','Stage Date - Prospect','Stage Date - Withdrawn','Stage Date - Applicant','Stage Date - Committed','Stage Date - Admitted','Stage Date - Matriculated','Stage Date - Denied','Plan Description','Program Code','Program Description','Action - Applicant','Action Reason - Applicant','Admit Type - Applicant','Admit Type - Prospect','Recruiting Center','Recruiting Center - Prospect','SFID18','Source Tracking Name','Source Tracking ID','Origin Indicator','Public Group','CompoundKey','Student Last School Graduation Date','Appl Date','Appl Completed Date','Appl Program Action Code','Appl Program Action Date','Derived Enrollment Status Code','Birthdate_y','Primary First Name','Primary Last Name','Other Email Address','Campus Emailid','GDS Campus Email Address','Salesforce ID','Full Name'],axis=1)


#de-dup again

contentMerge=contentMerge.drop_duplicates(subset=['First Name', 'Last Name','Contact ID','Breadcrumb','Capture Source'])
print(pd.unique(contentMerge['CONFIRMED']))
print(len(pd.unique(contentMerge[contentMerge['ENROLLED']=='E']['Contact ID'])))
contentMerge.to_csv(editfile,index=False)
