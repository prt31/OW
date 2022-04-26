"""
Collection of functions to process OW-WW2 project data.

2. Process raw data

"""

import datetime
import ast
import re
import pandas as pd
import numpy as np
import json
from collections import Counter

def isNumeric(s):
    
    """
    To check if str is numeric (int/float)

    """
    s = s.strip()
    try:
       s = float(s)
       return True
    except:
       return False

def makeDecimal(d,m=0,s=0,ss=0):
    """
    To make d,m,s to decimal lat/lon

    """
    return d+(m/60)+(s/3600)


def processDates(raw_combined_df):

    """
    To process raw dates

    """
    fmt_list=('%d-%m-%y','%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y','%d%m%Y','%d-%B-%Y','%d -%m-%Y','%d.%m.%Y','%d,%m,%Y','%d- %m- %Y','%d %B %Y','%d %m %Y','%d-%m=%Y','%d -%m- %Y','%d, %B, %Y','%d, %B,%Y','%A %d %B %Y','%d,%B,%Y','%A %d %B %Y\n','%d/%m/%y','%d %B, %Y','%A %d-%B-%Y','%d/%b/%Y','%d, %B %Y','%d-%m','%m-%d-%Y')

    dates=raw_combined_df['date']
    #formatted date
    formatteddate=[]
    #concensus Confidence
    confi=[]
    #for each date dict
    for date in dates:
        qq=[]
        #Added for Dale 1942 on 19-02-2022
        try:
            datedict=ast.literal_eval(date)
        except:
            print("Date: {0}".format(date))
            formatteddate.append('empty')
            #Concensus Confidence
            confi.append(1)
            continue

        #if datedict is '' try next date
        if not len(datedict): 
            formatteddate.append('empty')
            #Concensus Confidence
            confi.append(1)
            continue

        #Pick the date with highest count
        qq=pd.DataFrame.from_dict(datedict,columns=['num'],orient='index').sort_values(by=['num'],ascending=False).reset_index()
        #Concensus Confidence
        confi.append(round(qq['num'].iloc[0]/qq['num'].sum(),2)) #added round on 26-11-2021
        #keep only first row
        qq=qq.iloc[[0]]

        #loop for first dict 
        for ind,row in qq.iterrows(): #each dictionary

            fmt_fnd=0

            for fmt in fmt_list:
                #if fmt_fnd is zero -- go in loop
                #if not exit loop
                if fmt_fnd: break #continue
                try:
                    fmt_fnd=1
                    dte=[]
                    dte=pd.to_datetime(row['index'],format=fmt)#added later yearfirst
                    if dte > datetime.datetime.now():
                        dte = dte.replace(year=dte.year-100)
                    formatteddate.append(dte.strftime('%Y-%b-%d'))#added later .dt.strftime('%Y-%m-%d')
                    pass
                except ValueError:
                    fmt_fnd=0
                    pass
            if fmt_fnd:
                #continue
                break
            if not fmt_fnd:
                #print('No format found, setting formatteddate to Empty ')
                formatteddate.append('empty')
                #raise ValueError('no valid date format found')

    #print(formatteddate)
    dates_df=pd.DataFrame(columns=['Dates','ConsConfidence'])

    dates_df['Dates']=pd.Series(formatteddate)     

    dates_df['ConsConfidence']=pd.Series(confi)

    return dates_df


def processZones(raw_combined_df):
    
    """
    To process raw zones

    """

    zones=raw_combined_df['zone']
    formattedzone=[]

    #concensus Confidence
    confi=[]
    #exclude from zone
    exclude_list=['',' ','  ','----','---','--','--.--',' --.--',' --',' ---','sea','-',
                  ' Place','Place ',' n/a',' At Sea',' At sea','Sea','Place',' Sea',' Sea ']
    #for each date dict
    for zone in zones:
        qq=[]
        #Added for Dale 1942 on 19-02-2022
        try:
            zonedict=ast.literal_eval(zone)
        except:
            print("zone: {0}".format(zone))
            formattedzone.append('empty')
            #Concensus Confidence
            confi.append(1)
            continue

        #zonedict=ast.literal_eval(zone)
        dict_new={}

        for kys,vals in zonedict.items():
            #if kys not in exclude_list:
            if not (kys in exclude_list):# | (any(word in kys for word in omit_words))):
                #make new dict with key and vals
                dict_new[kys]=vals

        #Pick the date with highest count
        qq=pd.DataFrame.from_dict(dict_new,columns=['num'],orient='index').sort_values(by=['num'],ascending=False).reset_index()

        if len(qq):
            #Concensus Confidence
            confi.append(round(qq['num'].iloc[0]/qq['num'].sum(),2)) #added round on 26-11-2021
            #append(qq['num'].iloc[0]/qq['num'].sum())
            #keep only first row zone
            formattedzone.append(qq['index'].iloc[0])
        else:
            #Concensus Confidence
            confi.append(1.0)
            #keep only first row zone
            formattedzone.append('empty')

    zones_df=pd.DataFrame(columns=['Zones','ConsConfidenceZ'])

    zones_df['Zones']=pd.Series(formattedzone)     

    zones_df['ConsConfidenceZ']=pd.Series(confi)

    return zones_df

def read_add_Positiondict(posDict=None):
    """
    Read position dict, pass it to the processing function
    If posDict is valid dict then add key to read dict and save as json file
    The dict should be like:
    {'test':{'lat':'empty','lon':'empty'}}
    
    """
    out_folder='/Users/praveenteleti/python_data/ww2_ow_05112021/'
    fn='OW_positions_dict'
    fp=out_folder+fn+'.json'
    
    with open(fp, 'rt', encoding="utf-8") as myfile:
        doc=myfile.read()
    pos=json.loads(doc)
    
    #If posDict is valid dict
    if type(posDict)==dict:
        
        for key,val in posDict.items():
            pos[key]=val
        #save json for future use
        with open(fp, "w", encoding="utf-8") as fl:
            json.dump(pos, fl)
        
    return pos

def processPositions(raw_combined_df):
    
    """
    To process raw positions

    """
    positions=raw_combined_df['position']
    formattedposition=[]

    #concensus Confidence
    confi=[]
    #exclude from zone
    exclude_list=['',' ','  ','-','----','---','--','--.--',' --.--',' --',' ---','---,---','- -','sea',
                  ' Place','Place ','n/a','At Sea','At sea','Sea','Place','Sea',' Sea ',
                 'left blank on form','N, E','left blank','Latitude (N/S), Longitude (E/W)',
                 'Area C-11 To Maui range and return','aa', 'EN, E', 'E', '---, ---', '229,041, 2,691',
                 'as above','TWYS','blank']
    omit_words=['Enroute','enroute','en route',' Fleet Tactics','not stated']

    #for each date dict
    for position in positions:

        if not pd.notna(position):#np.isnan(position):
            #Concensus Confidence
            confi.append(1.0)
            #keep only first row zone
            formattedposition.append('empty')
            continue

        qq=[]
        positiondict=ast.literal_eval(position)
        dict_new={}

        for kys,vals in positiondict.items():
            #if kys not in exclude_list:
            if not ((kys in exclude_list) | (any(word in kys for word in omit_words))):
                #make new dict with key and vals
                dict_new[kys]=vals

        #Pick the date with highest count
        qq=pd.DataFrame.from_dict(dict_new,columns=['num'],orient='index').sort_values(by=['num'],ascending=False).reset_index()

        if len(qq):
            #Concensus Confidence
            confi.append(round(qq['num'].iloc[0]/qq['num'].sum(),2)) #added round on 26-11-2021
            #append(qq['num'].iloc[0]/qq['num'].sum())
            #keep only first row zone
            formattedposition.append(qq['index'].iloc[0])
        else:
            #Concensus Confidence
            confi.append(1.0)
            #keep only first row zone
            formattedposition.append('empty')

    #print(formatteddate)
    positions_df=pd.DataFrame(columns=['Position','ConsConfidenceP'])

    positions_df['Position']=pd.Series(formattedposition)     

    positions_df['ConsConfidenceP']=pd.Series(confi)
    
    return positions_df


def positionToLatLon(positions_df,pos_dict=None):
    
    latList=[]
    lonList=[]
    georef_dict=read_add_Positiondict(pos_dict)
    
    
    for ind,row in positions_df.iterrows():#[1000:2000]
        latPT,latDMS,latVal,lonPT,lonDMS,lonVal=[],[],[],[],[],[]

        if (row['Position']=='empty') | (row['Position']=='as above'): 
            latList.append('empty')
            lonList.append('empty')
            continue

        #known place names
        if row['Position'] in georef_dict.keys():
            latList.append(georef_dict[row['Position']]['lat'])
            lonList.append(georef_dict[row['Position']]['lon'])
            continue

        #Added on ===03032022===
        if (',' in row['Position']) & ('n' not in row['Position']) & ('N' not in row['Position']) & ('s' not in row['Position']) & ('S' not in row['Position']):
            #',' in row['Position']:

            dms_lst=[]
            latDMS=re.split('(\W+)',row['Position'].split(',')[0])
            for dms in latDMS:
                if isNumeric(dms):
                    dms_lst.append(float(dms))

            latList.append(round(makeDecimal(*dms_lst),2))

            dms_lst=[]
            lonDMS=re.split('(\W+)',row['Position'].split(',')[1])
            for dms in lonDMS:
                if isNumeric(dms):
                    dms_lst.append(float(dms))

            lonList.append(round(makeDecimal(*dms_lst),2))
            continue

                #check N or S hemisphere
        if (pd.Series(row['Position']).str.contains('n',case=False)).bool():
            #lat part of position
            latPT=re.split("n", row['Position'], flags=re.IGNORECASE)[0]
            lonPT=re.split("n", row['Position'], flags=re.IGNORECASE)[1]

            latH='N'
        else:
            #print(row['Position'])

            #lat part of position
            latPT=re.split("s", row['Position'], flags=re.IGNORECASE)[0]
            lonPT=re.split("s", row['Position'], flags=re.IGNORECASE)[1]

            latH='S'

        #split further to get deg min sec
        latDMS=re.split('(\W+)',latPT)

        dms_lst=[]
        for dms in latDMS:
            if isNumeric(dms):
                dms_lst.append(float(dms))
                #print(dms_lst)

        #dms list exists
        #make decimal degrees
        if dms_lst:
            try:#added on 22042022
                latVal=round(makeDecimal(*dms_lst),2) #added [0:3] on 29-11-2021
            except:
                print(row,dms_lst)
                raise Exception("Invalid format")
                
            if latH=='S':#SH
                latVal=-latVal

            latList.append(latVal)
        else:
            latList.append('empty')

        #====check E or W hemisphere===
        if (pd.Series(row['Position']).str.contains('e',case=False)).bool():
            lonH='E'
        else:
            lonH='W'

        #split further to get deg min sec
        lonDMS=re.split('(\W+)',lonPT)

        dms_lst=[]
        for dms in lonDMS:
            if isNumeric(dms):
                dms_lst.append(float(dms))
                #print(dms_lst)

        #dms list exists
        #make decimal degrees
        if dms_lst:
            try:#added on 22042022
                lonVal=round(makeDecimal(*dms_lst),2)
            except:
                print(row,dms_lst)
                raise Exception("Invalid format")
                
            if lonH=='W':#WH
                lonVal=-lonVal

            lonList.append(lonVal)
        else:
            lonList.append('empty')


    positions_df['Lat']=pd.Series(latList)

    positions_df['Lon']=pd.Series(lonList)#pd.to_numeric(pd.Series(lonList),errors='coerce')
    
    return positions_df

def read_add_Placedict(posDict=None):
    """
    Read place dict, pass it to the processing function
    If posDict is valid dict then add key to read dict and save as json file
    The dict should be like:
    {'test':{'lat':'empty','lon':'empty'},'test2':{'lat':'empty','lon':'empty'},'test3':{'lat':'empty','lon':'empty'}}
    
    """
    out_folder='/Users/praveenteleti/python_data/ww2_ow_05112021/'
    fn='OW_places_dict'
    fp=out_folder+fn+'.json'
    
    with open(fp, 'rt', encoding="utf-8") as myfile:
        doc=myfile.read()
    pos=json.loads(doc)
    
    #If posDict is valid dict
    if type(posDict)==dict:
        for key,val in posDict.items():
            pos[key]=val
            #pos[list(posDict.keys())[0]]=posDict[list(posDict.keys())[0]]
        #save json for future use
        with open(fp, "w", encoding="utf-8") as fl:
            json.dump(pos, fl)
        
    return pos

def processPlaces(raw_combined_df,pos_dict=None):
    
    
    #========place data processing==============

    #seperate lat/lon in place
    #match position to Geo-ref dict

    places=raw_combined_df['place']
    formattedplace=[]

    #concensus Confidence
    confi=[]
    #exclude from zone
    exclude_list=['',' ','  ','----','---','--','--.--',' --.--',' --',' ---','sea',
                  ' Place','Place ','n/a','At Sea','At sea','Sea','Place','Sea',' Sea ',
                 'left blank on form','N, E','left blank','Latitude (N/S), Longitude (E/W)',
                 'Area C-11 To Maui range and return','aa', 'EN, E', 'E', '---, ---', '229,041, 2,691',
                 'as above']
    omit_words=['Enroute','enroute','en route',' Fleet Tactics','not stated','blank']

    #for each date dict
    for place in places:

        if not pd.notna(place):#np.isnan(position):
            #Concensus Confidence
            confi.append(1.0)
            #keep only first row zone
            formattedposition.append('empty')
            continue

        qq=[]
        placedict=ast.literal_eval(place)
        dict_new={}

        #to filter out irrrelavent info
        for kys,vals in placedict.items():
            #if kys not in exclude_list:
            if not ((kys in exclude_list) | (any(word in kys for word in omit_words))):
                #make new dict with key and vals
                dict_new[kys]=vals

        #Pick the date with highest count
        qq=pd.DataFrame.from_dict(dict_new,columns=['num'],orient='index').sort_values(by=['num'],ascending=False).reset_index()

        if len(qq):
            #Concensus Confidence
            confi.append(round(qq['num'].iloc[0]/qq['num'].sum(),2)) #added round on 26-11-2021
            #append(qq['num'].iloc[0]/qq['num'].sum())
            #keep only first row zone
            formattedplace.append(qq['index'].iloc[0])
        else:
            #Concensus Confidence
            confi.append(1.0)
            #keep only first row zone
            formattedplace.append('empty')


    placeLat=[]
    placeLon=[]
    georef_dict=read_add_Placedict(pos_dict)
    
    for place in formattedplace:

        if place in georef_dict.keys():
            placeLat.append(georef_dict[place]['lat'])#[row['Place']]['lat'])
            placeLon.append(georef_dict[place]['lon'])

        else:
            placeLat.append('empty')
            placeLon.append('empty')

    #place latlon df
    places_latlon_df=pd.DataFrame(columns=['Place','placeCon','placeLat','placeLon'])

    places_latlon_df['Place']=pd.Series(formattedplace)     

    places_latlon_df['placeCon']=pd.Series(confi)

    places_latlon_df['placeLat']=pd.Series(placeLat)     

    places_latlon_df['placeLon']=pd.Series(placeLon)

    return places_latlon_df


def findCommonPlaces(places_latlon_df,nrows=40):
    
    letter_counts = Counter(places_latlon_df.Place)#qq.workflow_name)
    img_count=pd.DataFrame.from_records(letter_counts.most_common(), columns=['image','count'])
    return img_count.head(nrows)

def combineNavProcessed(raw_df,dates_df,zones_df,positions_df,places_latlon_df):
    
    
    info_df=raw_df[['hours', 'image_name', 'image_url', 'ship_name','subject_ids','year_image']]
    #join all dfs to form nav df
    nav_combined_df=pd.concat([info_df,dates_df,zones_df,positions_df,places_latlon_df],axis=1,ignore_index=True)
    nav_combined_df.columns=['hours', 'image_name', 'image_url', 'ship_name','subject_ids','year_image', 'Date','dateCon','Zone','zoneCon', 'Position','positionCon','Lat','Lon','Place','placeCon','placeLat','placeLon']

    #combine lat/lon from position and place

    #priority to position lat/lon if empty then take place lat/lon

    comLat,comLon=[],[]
    for ind,row in nav_combined_df.iterrows():
        if row['Lat']=='empty':
            comLat.append(row['placeLat'])
            comLon.append(row['placeLon'])
        else:
            comLat.append(row['Lat'])
            comLon.append(row['Lon'])

    nav_combined_df['comLat']=pd.Series(comLat)
    nav_combined_df['comLon']=pd.Series(comLon)

    return nav_combined_df

def processBaroRaw(raw_combined_df):


    #=====baro processing=======

    baro_df=raw_combined_df[['baro','baro_ht']]

    formatted_baro=[]
    formatted_baro_ht=[]

    #concensus Confidence
    baro_confi=[]
    baro_ht_confi=[]

    #for each row of temp_df
    for ind,row in baro_df.iterrows():

        qq=[]
        baro_dict=ast.literal_eval(row['baro'])
        if not pd.isna(row['baro_ht']):
            baro_ht_dict=ast.literal_eval(row['baro_ht'])
        else:
            baro_ht_dict={}

        #Pick the baro with highest count
        qq=pd.DataFrame.from_dict(baro_dict,columns=['num'],orient='index').sort_values(by=['num'],ascending=False).reset_index()

        if len(qq):
            #Concensus Confidence
            baro_confi.append(round(qq['num'].iloc[0]/qq['num'].sum(),2)) #added round on 26-11-2021
            #append(qq['num'].iloc[0]/qq['num'].sum())
            #keep only first row tdry
            formatted_baro.append(qq['index'].iloc[0])
        else:
            #Concensus Confidence
            baro_confi.append(1.0)
            #keep only first row twet
            formatted_baro.append('empty')

        qq=[]
        #Pick with highest count == baro_ht
        qq=pd.DataFrame.from_dict(baro_ht_dict,columns=['num'],orient='index').sort_values(by=['num'],ascending=False).reset_index()

        if len(qq):
            #Concensus Confidence
            baro_ht_confi.append(round(qq['num'].iloc[0]/qq['num'].sum(),2)) #added round on 26-11-2021
            #append(qq['num'].iloc[0]/qq['num'].sum())
            #keep only first row twet
            formatted_baro_ht.append(qq['index'].iloc[0])
        else:
            #Concensus Confidence
            baro_ht_confi.append(1.0)
            #keep only first row twet
            formatted_baro_ht.append('empty')


    baro_pros_df=pd.DataFrame(columns=['baro','baroCons','baro_ht','barohtCons'])

    baro_pros_df['baro']=pd.Series(formatted_baro)     

    baro_pros_df['baroCons']=pd.Series(baro_confi)

    baro_pros_df['baro_ht']=pd.Series(formatted_baro_ht)     

    baro_pros_df['barohtCons']=pd.Series(baro_ht_confi)

    return baro_pros_df


def processTempRaw(raw_combined_df):
    
    
    
    #=====temp processing=======

    temp_df=raw_combined_df[['tdry','twet','twater']]

    formatted_tdry=[]
    formatted_twet=[]
    formatted_twater=[]

    #concensus Confidence
    tdry_confi=[]
    twet_confi=[]
    twater_confi=[]

    #for each row of temp_df
    for ind,row in temp_df.iterrows():

        qq=[]
        tdry_dict,twet_dict,twater_dict={},{},{}

        if not pd.isna(row['tdry']): tdry_dict=ast.literal_eval(row['tdry'])
        if not pd.isna(row['twet']): twet_dict=ast.literal_eval(row['twet'])
        if not pd.isna(row['twater']): twater_dict=ast.literal_eval(row['twater'])

        #Pick the tdry with highest count
        qq=pd.DataFrame.from_dict(tdry_dict,columns=['num'],orient='index').sort_values(by=['num'],ascending=False).reset_index()

        if len(qq):
            #Concensus Confidence
            tdry_confi.append(round(qq['num'].iloc[0]/qq['num'].sum(),2)) #added round on 26-11-2021
            #append(qq['num'].iloc[0]/qq['num'].sum())
            #keep only first row tdry
            formatted_tdry.append(qq['index'].iloc[0])
        else:
            #Concensus Confidence
            tdry_confi.append(1.0)
            #keep only first row twet
            formatted_tdry.append('empty')

        qq=[]
        #Pick with highest count == twet
        qq=pd.DataFrame.from_dict(twet_dict,columns=['num'],orient='index').sort_values(by=['num'],ascending=False).reset_index()

        if len(qq):
            #Concensus Confidence
            twet_confi.append(round(qq['num'].iloc[0]/qq['num'].sum(),2)) #added round on 26-11-2021
            #append(qq['num'].iloc[0]/qq['num'].sum())
            #keep only first row twet
            formatted_twet.append(qq['index'].iloc[0])
        else:
            #Concensus Confidence
            twet_confi.append(1.0)
            #keep only first row twet
            formatted_twet.append('empty')

        qq=[]
        #Pick with highest count == twater
        qq=pd.DataFrame.from_dict(twater_dict,columns=['num'],orient='index').sort_values(by=['num'],ascending=False).reset_index()

        if len(qq):
            #Concensus Confidence
            twater_confi.append(round(qq['num'].iloc[0]/qq['num'].sum(),2)) #added round on 26-11-2021
            #append(qq['num'].iloc[0]/qq['num'].sum())
            #keep only first row twater
            formatted_twater.append(qq['index'].iloc[0])
        else:
            #Concensus Confidence
            twater_confi.append(1.0)
            #keep only first row twater
            formatted_twater.append('empty')

    temp_pros_df=pd.DataFrame(columns=['tdry','tdryCons','twet','twetCons','twater','twaterCons'])

    temp_pros_df['tdry']=pd.Series(formatted_tdry)     

    temp_pros_df['tdryCons']=pd.Series(tdry_confi)

    temp_pros_df['twet']=pd.Series(formatted_twet)     

    temp_pros_df['twetCons']=pd.Series(twet_confi)

    temp_pros_df['twater']=pd.Series(formatted_twater)     

    temp_pros_df['twaterCons']=pd.Series(twater_confi)

    return temp_pros_df

def saveProcessedData(nav_combined_df,baro_df,temp_df):
    
    ship_pros=pd.concat([nav_combined_df,baro_df,temp_df],axis=1,ignore_index=True)
    ship_pros.columns=list(nav_combined_df.columns)+list(baro_df.columns)+list(temp_df.columns)
    
    ship_name=ship_pros['ship_name'].iloc[0]
    yr_name=ship_pros['year_image'].iloc[0]
    
    #save ship processed
    out_folder='/Users/praveenteleti/python_data/ww2_ow_05112021/'
    today=datetime.datetime.now().strftime('%d%m%Y')
    
    fn=out_folder+ship_name+'_'+str(yr_name)+'_all_processed_df_'+today+'.csv'
    ship_pros.to_csv(fn,index=False)
    
    return ship_pros

def inFillLatLon(processed_combined_df,printFile=True):
    
    """
    Fill-in comLat and comLon, by selecting all row for each subject_ids
    If comLat and comLon is present at 8,12 and 20
    """

    unique_subject_ids=list(processed_combined_df['subject_ids'].unique())
    unique_subject_ids.sort()
    #image name
    comLatList=[]
    comLonList=[]
    
    ship_name=processed_combined_df['ship_name'].iloc[0]
    yr_name=processed_combined_df['year_image'].iloc[0]
    

    for sid in unique_subject_ids:
        temp=[]
        temp=processed_combined_df[processed_combined_df['subject_ids']==sid].copy()#[row_indexer,column_indexer]
        temp.sort_values(by=['hours'],inplace=True)

        if not temp['comLat'].iloc[7]=='empty':
            comLatList.extend(np.repeat(temp['comLat'].iloc[7],8))
            comLonList.extend(np.repeat(temp['comLon'].iloc[7],8))
        else:
            comLatList.extend(np.repeat('empty',8))
            comLonList.extend(np.repeat('empty',8))

        if not temp['comLat'].iloc[11]=='empty':
            comLatList.extend(np.repeat(temp['comLat'].iloc[11],4))
            comLonList.extend(np.repeat(temp['comLon'].iloc[11],4))
        else:
            comLatList.extend(np.repeat('empty',4))
            comLonList.extend(np.repeat('empty',4))

        if not temp['comLat'].iloc[19]=='empty':
            comLatList.extend(np.repeat(temp['comLat'].iloc[19],12))
            comLonList.extend(np.repeat(temp['comLon'].iloc[19],12))
        else:
            comLatList.extend(np.repeat('empty',12))
            comLonList.extend(np.repeat('empty',12))

    inFilled=processed_combined_df.copy()

    inFilled['comLat']=pd.Series(comLatList)
    inFilled['comLon']=pd.Series(comLonList)
    
    out_folder='/Users/praveenteleti/python_data/ww2_ow_05112021/'
    today=datetime.datetime.now().strftime('%d%m%Y')
    
    fn=out_folder+ship_name+'_'+str(yr_name)+'_infilled_df_'+today+'.csv'

    if printFile:
        inFilled.to_csv(fn,index=False)
    
    return inFilled

def makedateHours(gdrive_df,printFile=True):
    
    #=============make dateHours========
    
    gdrive_df=gdrive_df.copy()
    
    
    ship_name=gdrive_df['ship_name'].iat[0]
    yr_name=gdrive_df['year_image'].iat[0]
    
    #Before generating kml file, combine hours + dates
    fil_df=[]
    fil_df=gdrive_df[['hours','Date','comLat','comLon']]

    dh_lst=[]
    #check for 24 hr
    for ind,row in fil_df.iterrows():
        tempdte=[]

        if row['hours']==24:
            #combine dates and hours
            tempdte=pd.to_datetime(row['Date']) +  pd.to_timedelta(1, unit='d')

        else:
            #combine dates and hours
            tempdte=pd.to_datetime(row['Date']) +  pd.to_timedelta(row['hours'], unit='h')

        tempdte=tempdte.strftime('%Y-%m-%d %H:%M')
        dh_lst.append(tempdte)

    gdrive_df.insert(6, 'dateHours', pd.Series(dh_lst))
    gdrive_df.sort_values(by=['dateHours'],inplace=True)

    gdrive_df.reset_index(drop=True, inplace=True)
    
    out_folder='/Users/praveenteleti/python_data/ww2_ow_05112021/'
    today=datetime.datetime.now().strftime('%d%m%Y')
    
    fn=out_folder+ship_name+'_'+str(yr_name)+'_processed_'+today+'.csv'

    if printFile:
        gdrive_df.to_csv(fn,index=False)
    
    return gdrive_df