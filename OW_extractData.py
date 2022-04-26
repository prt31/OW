"""
Collection of functions to process OW-WW2 project data.

1. Extract data

"""
import datetime
import ast
import re
import pandas as pd
import numpy as np
import json
from collections import Counter
from collections import defaultdict


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
    
def shipCountbyYear(filter_df,year):
    
    """
    Input: Filtered df and year
    
    Output: Dataframe of count of each ship in the given year
    
    """
    #find counts of each ship
    letter_counts = Counter(filter_df['ship_name'].loc[filter_df['year_image']==year])#qq.workflow_name)
    img_count=pd.DataFrame.from_records(letter_counts.most_common(), columns=['image','count'])
    
    return img_count

def saveRetiredShipbyYear(filter_df,ship,year):
    
    """
    What it does: Saves a csv file of dataframe of retired images and data of the given ship and year
    
    Input: Filtered df, ship and year
    
    Output: shape of the dataframe
    
    """
    
    #Now find year-ship data
    yr_name,ship_name=year,ship

    yr_rule=filter_df['year_image']==yr_name

    shp_rule=filter_df['ship_name']==ship_name

    ship_df=filter_df[yr_rule & shp_rule]

    ship_df.reset_index(inplace=True)

    #find counts of each image for given ship
    #five workflows x 3 retirement = 15 count
    letter_counts = Counter(ship_df.subject_ids)
    img_count=pd.DataFrame.from_records(letter_counts.most_common(), columns=['image','count'])
    img_count=img_count[img_count['count']>14]

    retired_images=[]

    #loop to see that image is retired from all workflows
    for ind,row in img_count.iterrows():

        temps=[]
        temps=dict(Counter(ship_df[ship_df['subject_ids']==row['image']].workflow_name))
        img_complete='Y'
        for num in temps.items():
            if not num[1]>2:
                img_complete='N'

        if img_complete=='Y':
            retired_images.append(row['image'])

    #get df for these images only
    #=== ship + retired images ===only
    df_cols=ship_df.columns
    #empty df
    ship_retired_df=pd.DataFrame(columns=df_cols)

    for ind,row in ship_df.iterrows():
        if row['subject_ids'] in retired_images:
            ship_retired_df=ship_retired_df.append(row)

    
    #==========
    #save csv
    
    ship_retired_df.drop(columns=['index'],inplace=True)
    
    out_folder='/Users/praveenteleti/python_data/ww2_ow_05112021/'
    printFile=1
    today=datetime.datetime.now().strftime('%d%m%Y')
    
    fn=out_folder+ship_name+'_'+str(yr_name)+'_retired_df_'+today+'.csv'

    if printFile: ship_retired_df.to_csv(fn,index=False)
    
    return ship_retired_df


def extractRawNavigation(ship_retired_df,printFile=False):
    
    
    """
    What it does: Extracts and saves a csv file of Navigation raw dataframe of retired ship
    
    Input: ship retired df
    
    Output: Navigation raw dataframe of retired ship
    
    """

    #process navigation workflow
    #load shp_retired_df
    #filter for navigation workflow
    nav_df=ship_retired_df[ship_retired_df['workflow_name']=='Navigation']

    ship_name=nav_df['ship_name'].iloc[0]
    yr_name=str(nav_df['year_image'].iloc[0])
    
    unique_subject_ids=list(nav_df['subject_ids'].unique())
    unique_subject_ids.sort()

    nav_day_df_all=pd.DataFrame(columns=['subject_ids','ship_name','year_image','hours','date','zone','position','place'])
    nav_day_df=pd.DataFrame(columns=['subject_ids','ship_name','year_image','hours','date','zone','position','place'])
    nav_day_df['hours']=list(range(1,25))#list(range(24))

    for sid in unique_subject_ids:#ind,row in nav_df.iterrows():
        temp=[]
        temp=nav_df[nav_df['subject_ids']==sid]
        #image name
        image_name=[]
        image_name=temp['image_name'].iloc[0]
        #image url 14032022
        image_url=temp['image_url'].iloc[0]

        date=[]
        zone=[]

        am8=[]
        noon=[]
        pm8=[]
        place=[]

        for ind,row in temp.iterrows():
            tt=json.loads(row['annotations'])
            sp_lines=[]

            for i in range(3):

                #===date, place and zone====

                if i==0:
                    date.append(tt[i]['value'])
                if i==1:
                    zone.append(tt[i]['value'])
                if i==2:
                    sp_lines=tt[i]['value'].splitlines()
                    if len(sp_lines)>1:
                        try:
                            am8.append(sp_lines[1].split(',',1)[1].strip())
                        except:
                            pass
                    if len(sp_lines)>2:
                        try:
                            noon.append(sp_lines[2].split(',',1)[1].strip())
                        except:
                            pass
                    if len(sp_lines)>3:
                        try:
                            pm8.append(sp_lines[3].split(',',1)[1].strip())
                        except:
                            pass
                    if len(sp_lines)>4:
                        try:
                            place.append(sp_lines[4].split(',',1)[1].strip())
                        except:
                            pass


        #make histogram dict
        dict_date=dict(Counter(date))
        xx=dict_date.pop('',None)
        dict_zone=dict(Counter(zone))
        xx=dict_zone.pop('',None)
        dict_am8=dict(Counter(am8))
        xx=dict_am8.pop('',None)
        dict_noon=dict(Counter(noon))
        xx=dict_noon.pop('',None)
        dict_pm8=dict(Counter(pm8))
        xx=dict_pm8.pop('',None)
        dict_place=dict(Counter(place))
        xx=dict_place.pop('',None)


        #fill-in the nav_day_df
        dict_date_list=((str(dict_date)+';')*24).split(';')[0:24]
        dict_zone_list=((str(dict_zone)+';')*24).split(';')[0:24]
        dict_place_list=((str(dict_place)+';')*24).split(';')[0:24]
        sh_list=((ship_name+';')*24).split(';')[0:24]
        year_list=((yr_name+';')*24).split(';')[0:24]
        
        nav_day_df['subject_ids']=pd.Series(np.repeat(sid,24))
        nav_day_df['ship_name']=pd.Series(sh_list)
        nav_day_df['year_image']=pd.Series(year_list)#added 22042022
        nav_day_df['date']=pd.Series(dict_date_list)
        nav_day_df['zone']=pd.Series(dict_zone_list)
        nav_day_df.loc[7,'position']=str(dict_am8)
        nav_day_df.loc[11,'position']=str(dict_noon)
        nav_day_df.loc[19,'position']=str(dict_pm8)
        nav_day_df['place']=pd.Series(dict_place_list)
        nav_day_df['image_name']=pd.Series(np.repeat(image_name,24))
        nav_day_df['image_url']=pd.Series(np.repeat(image_url,24))#added 14032022

        nav_day_df_all=nav_day_df_all.append(nav_day_df,sort=True,ignore_index=True)
        
   #save csv
    out_folder='/Users/praveenteleti/python_data/ww2_ow_05112021/'
    today=datetime.datetime.now().strftime('%d%m%Y')
    
    fn=out_folder+ship_name+'_'+yr_name+'_nav_raw_df_'+today+'.csv'
    
    if printFile:
        nav_day_df_all.to_csv(fn,index=False)
        
    return nav_day_df_all


def extractRawBarometer(ship_retired_df,printFile=False):
    
    
    """
    What it does: Extracts and saves a csv file of Barometer raw dataframe of retired ship
    
    Input: ship retired df, printFile
    
    Output: Barometer raw dataframe of retired ship
    
    """
    

    #process navigation workflow
    #load shp_retired_df
    #filter for AM Baro workflow
    rule1=ship_retired_df['workflow_name']=='AM Barometer'
    rule2=ship_retired_df['workflow_name']=='PM Barometer'

    baroAM_df=ship_retired_df[rule1 | rule2]
    
    
    #baroAM_df contains both AM and PM

    ship_name=baroAM_df['ship_name'].iloc[0]
    yr_name=baroAM_df['year_image'].iloc[0]
    
    unique_subject_ids=list(baroAM_df['subject_ids'].unique())
    unique_subject_ids.sort()

    #met df to store all rows
    baroAM_day_df_all=pd.DataFrame(columns=['subject_ids','image_name','ship_name','hours','baro','baro_ht'])
    #met df to store day
    baroAM_day_df=pd.DataFrame(columns=['subject_ids','image_name','ship_name','hours','baro','baro_ht'])
    #hours range
    #pre-populate hours 1-24
    baroAM_day_df['hours']=list(range(1,25))

    for sid in unique_subject_ids:
        temp=[]
        temp=baroAM_df[baroAM_df['subject_ids']==sid]

        #image name
        image_name=[]
        image_name=temp['image_name'].iloc[0]

        #baro_hours dict
        baro_hours_dict=[]
        baro_hours_dict = defaultdict(list)
        baro_ht_hours_dict=[]
        baro_ht_hours_dict = defaultdict(list)

        for ind,row in temp.iterrows():
            js=json.loads(row['annotations'])
            sp_lines=[]
            sp_lines=js[0]['value'].splitlines()

            #loop for number of lines
            for i in sp_lines:
                hr_val=[]
                bar_val=[]
                bar_ht_val=[]

                #empty string
                if not len(i):
                    continue

                #if first char of first line is non-numeric -- it's the label
                if(isNumeric(i[0].strip())):

                    str_chunks=i.split(',')

                    #first is hour
                    #second is baro
                    #third is baro_ht

                    #try-catch blocks to make room for non-uniform string lengths
                    try:
                        hr_val=int(str_chunks[0])
                    except:
                        continue
                    try:
                        bar_val=str_chunks[1].replace(' ','').replace('.','').replace(':','')
                        baro_hours_dict[hr_val].append(bar_val)
                    except:
                        pass
                    try:
                        bar_ht_val=str_chunks[2].replace(' ','').replace('.','').replace(':','')
                        baro_ht_hours_dict[hr_val].append(bar_ht_val)
                    except:
                        pass


        #print(baro_hours_dict)
        #print(sid)

        x_dict=[]
        #loop over the dict to make the histogram dict == baro
        for kys,vals in baro_hours_dict.items():

            #pop-out unwanted strings
            x_dict=dict(Counter(vals))
            xx=x_dict.pop('',None)
            xx=x_dict.pop('---',None)

            #match hours with keys of the dict
            baroAM_day_df.loc[baroAM_day_df['hours']==kys,'baro']=str(x_dict)

        x_dict=[]
        #loop over the dict to make the histogram dict == bar_ht
        for kys,vals in baro_ht_hours_dict.items():

            #pop-out unwanted strings
            x_dict=dict(Counter(vals))
            xx=x_dict.pop('',None)
            xx=x_dict.pop('---',None)

            #match hours with keys of the dict
            baroAM_day_df.loc[baroAM_day_df['hours']==kys,'baro_ht']=str(x_dict)#str(dict(Counter(vals)))

        sh_list=((ship_name+';')*24).split(';')[0:24]

        baroAM_day_df['subject_ids']=pd.Series(np.repeat(sid,24))
        baroAM_day_df['ship_name']=pd.Series(sh_list)
        baroAM_day_df['image_name']=pd.Series(np.repeat(image_name,24))

        baroAM_day_df_all=baroAM_day_df_all.append(baroAM_day_df,ignore_index=True)
        
   # #save csv
    # #ship_retired_df.drop(columns=['index'],inplace=True)
    out_folder='/Users/praveenteleti/python_data/ww2_ow_05112021/'
    today=datetime.datetime.now().strftime('%d%m%Y')
    
    fn=out_folder+ship_name+'_'+str(yr_name)+'_baro_raw_df_'+today+'.csv'

    if printFile:
        baroAM_day_df_all.to_csv(fn,index=False)
        
    return baroAM_day_df_all


def extractRawThermometer(ship_retired_df,printFile=False):
    
    
    """
    What it does: Extracts and saves a csv file of Thermometer raw dataframe of retired ship
    
    Input: ship retired df, printFile
    
    Output: Thermometer raw dataframe of retired ship
    
    """
    
    #process navigation workflow
    #load shp_retired_df
    #filter for AM Baro workflow
    rule1=ship_retired_df['workflow_name']=='AM Temperatures'
    rule2=ship_retired_df['workflow_name']=='PM Temperatures'

    tempAM_df=ship_retired_df[rule1 | rule2]
    
    
    #temp AM-PM

    ship_name=tempAM_df['ship_name'].iloc[0]
    yr_name=tempAM_df['year_image'].iloc[0]

    unique_subject_ids=list(tempAM_df['subject_ids'].unique())
    unique_subject_ids.sort()

    #met df to store all rows
    tempAM_day_df_all=pd.DataFrame(columns=['subject_ids','image_name','ship_name','hours','tdry','twet','twater'])
    #met df to store day
    tempAM_day_df=pd.DataFrame(columns=['subject_ids','image_name','ship_name','hours','tdry','twet','twater'])
    #hours range
    #pre-populate hours 1-24
    tempAM_day_df['hours']=list(range(1,25))

    for sid in unique_subject_ids:
        temp=[]
        temp=tempAM_df[tempAM_df['subject_ids']==sid]

        #image name
        image_name=[]
        image_name=temp['image_name'].iloc[0]

        tdry_hours_dict=[]
        tdry_hours_dict = defaultdict(list)

        twet_hours_dict=[]
        twet_hours_dict = defaultdict(list)

        twater_hours_dict=[]
        twater_hours_dict = defaultdict(list)

        for ind,row in temp.iterrows():
            js=json.loads(row['annotations'])
            sp_lines=[]
            sp_lines=js[0]['value'].splitlines()

            #loop for number of split lines
            for i in sp_lines:
                hr_val=[]
                tdry_val=[]
                twet_val=[]
                twater_val=[]

                #empty string
                if not len(i):
                    continue

                #if first char of first line is non-numeric -- it's the label
                if(isNumeric(i[0].strip())):

                    str_chunks=i.split(',')
                    #iter_chk= iter(str_chunks)

                    #first is hour
                    #second is tdry
                    #third is twet
                    #fourth is twater

                    #try-catch blocks to make arragement for non-uniform string lengths
                    try:
                        hr_val=int(str_chunks[0])
                    except:
                        continue
                    try:
                        tdry_val=str_chunks[1].replace(' ','').replace('.','')
                        tdry_hours_dict[hr_val].append(tdry_val)
                    except:
                        pass
                    try:
                        twet_val=str_chunks[2].replace(' ','').replace('.','')
                        twet_hours_dict[hr_val].append(twet_val)
                    except:
                        pass
                    try:
                        twater_val=str_chunks[3].replace(' ','').replace('.','')
                        twater_hours_dict[hr_val].append(twater_val)
                    except:
                        pass


    #     print(tdry_hours_dict)
    #     print(sid)

        x_dict=[]
        #loop over the dict to make the histogram dict == tdry
        for kys,vals in tdry_hours_dict.items():

            #pop-out unwanted strings
            x_dict=dict(Counter(vals))
            xx=x_dict.pop('',None)
            xx=x_dict.pop('---',None)

            #match hours with keys of the dict
            tempAM_day_df.loc[tempAM_day_df['hours']==kys,'tdry']=str(x_dict)

        x_dict=[]
        #loop over the dict to make the histogram dict == twet
        for kys,vals in twet_hours_dict.items():

            #pop-out unwanted strings
            x_dict=dict(Counter(vals))
            xx=x_dict.pop('',None)
            xx=x_dict.pop('---',None)

            #match hours with keys of the dict
            tempAM_day_df.loc[tempAM_day_df['hours']==kys,'twet']=str(x_dict)

        x_dict=[]
        #loop over the dict to make the histogram dict == twater
        for kys,vals in twater_hours_dict.items():

            #pop-out unwanted strings
            x_dict=dict(Counter(vals))
            xx=x_dict.pop('',None)
            xx=x_dict.pop('---',None)

            #match hours with keys of the dict
            tempAM_day_df.loc[tempAM_day_df['hours']==kys,'twater']=str(x_dict)


        #fill-in the temp_day_df
        sh_list=((ship_name+';')*24).split(';')[0:24]

        tempAM_day_df['subject_ids']=pd.Series(np.repeat(sid,24))
        tempAM_day_df['ship_name']=pd.Series(sh_list)
        tempAM_day_df['image_name']=pd.Series(np.repeat(image_name,24))

        tempAM_day_df_all=tempAM_day_df_all.append(tempAM_day_df,ignore_index=True)
        
    #save csv
    #ship_retired_df.drop(columns=['index'],inplace=True)
    out_folder='/Users/praveenteleti/python_data/ww2_ow_05112021/'
    today=datetime.datetime.now().strftime('%d%m%Y')
    
    fn=out_folder+ship_name+'_'+str(yr_name)+'_temp_raw_df_'+today+'.csv'

    if printFile:
        tempAM_day_df_all.to_csv(fn,index=False)

    return tempAM_day_df_all

def saveRawData(nav_df,baro_df,temp_df):
    
    raw_combined_df=pd.concat([nav_df,baro_df,temp_df],axis=1,ignore_index=True)
    raw_combined_df.columns=list(nav_df.columns)+list(baro_df.columns)+list(temp_df.columns)
    #raw_combined_df.reset_index(drop=True,inplace=True)
    #remove duplicate columns before consensus
    raw_combined_df = raw_combined_df.loc[:,~raw_combined_df.columns.duplicated()]
    
    ship_name=raw_combined_df['ship_name'].iloc[0]
    yr_name=raw_combined_df['year_image'].iloc[0]
    
    #save csv
    #ship_retired_df.drop(columns=['index'],inplace=True)
    out_folder='/Users/praveenteleti/python_data/ww2_ow_05112021/'
    today=datetime.datetime.now().strftime('%d%m%Y')
    
    fn=out_folder+ship_name+'_'+str(yr_name)+'_all_raw_df_'+today+'.csv'
    printFile=1
    
    if printFile:
        raw_combined_df.to_csv(fn,index=False)
        
    return raw_combined_df