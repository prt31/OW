"""
Visualise the ship data

"""
import pandas as pd
import cartopy.crs as ccrs
import calendar
import matplotlib.pyplot as plt
import numpy as np
import os
from datetime import datetime,timedelta
from simplekml import Kml, Style
import simplekml

def makeGlobeplot(gdrive_df,GlobeFace='pacific'):

    #Overview

    #custom background folder
    # os.environ["CARTOPY_USER_BACKGROUNDS"] = "/Users/praveenteleti/python_data/Background_img/"

    #using above
    kml_df=[]
    kml_df=gdrive_df


    #cast a type
    kml_df['baro']=pd.to_numeric(kml_df['baro'],errors='coerce')
    kml_df['tdry']=pd.to_numeric(kml_df['tdry'],errors='coerce')
    kml_df['twater']=pd.to_numeric(kml_df['twater'],errors='coerce')

    #to delete typos
    qq=[]
    qq=kml_df['baro']>3150

    kml_df.loc[qq,'baro']=np.nan

    qq=[]
    qq=kml_df['baro']<2800

    kml_df.loc[qq,'baro']=np.nan

    #to delete typos 
    qq=[]
    qq=kml_df['tdry']>120

    kml_df.loc[qq,'tdry']=np.nan

    qq=[]
    qq=kml_df['tdry']<40
    #iid=np.where(qq)[0].tolist()

    kml_df.loc[qq,'tdry']=np.nan

    #ship_name
    ship_name=kml_df['ship_name'].iat[0]
    yr_str=str(kml_df['year_image'].iat[0])
    
    #time in minutes since 1900-1-1
    min_list=[]
    t2=[]
    epoc_time='1900-01-01 00:00'
    t2=datetime.strptime(epoc_time,'%Y-%m-%d %H:%M')
    #minutes array
    for ind,row in kml_df.iterrows():
        t1=[]

        t1=datetime.strptime(row['dateHours'],'%Y-%m-%d %H:%M')

        t3=t1-t2

        min_list.append(t3.total_seconds() / 60)

    #chose face of globe
    cel,txtlon,txtlat=220,-170, 35
    if GlobeFace=='atlantic': 
        cel=300
        txtlon,txtlat=-60,35
        
    fig = plt.figure(figsize=[20,10])
    # ax.set_extent([130,270,-60,60],crs=ccrs.PlateCarree())#[lonmin,lonmax,latmin,latmax]
    ax = fig.add_subplot(1,1,1, projection=ccrs.Orthographic(central_latitude=0, central_longitude=cel))

    ax.stock_img()
    #ax.background_img(name='EO', resolution='low')
    ax.coastlines()

    #generate tick locations
    #srt_date=datetime(1941,1,1)

    #ytick color & title color
    yclr,tclr='black','black'

    dts=pd.date_range(start='1-1-'+yr_str, periods=5, freq=pd.offsets.MonthBegin(3))

    #Added 19-02-2022

    #xlt=(dts - datetime(1900,1,1,1,0,0)).total_seconds()/60
    xlt=(dts - t2).total_seconds()/60

    #xlabs=[calendar.month_abbr[idx] for idx in dts.month.values]
    xlabs=[datetime.strftime(id,'%b') for id in dts]

    cmap = plt.cm.get_cmap("tab20", 4)

    #scatter plot
    aa=ax.scatter(kml_df['comLon'],kml_df['comLat'],s=50,marker='+',c=min_list,cmap=cmap,transform=ccrs.PlateCarree())

    plt.xticks(size=16)
    plt.xlabel('Long.',size=16)
    plt.yticks(size=16)
    plt.ylabel('Lat.',size=16)

    cbar=fig.colorbar(aa, ax=ax,extend='both',ticks=xlt,shrink=0.8)#,orientation='horizontal') 

    cbar.set_label(label=yr_str,
                   size=15,weight='bold',rotation=0.0,ha='left')#position=(0.9,0.5))
    cbar.ax.tick_params(labelsize=16)

    cbar.ax.set_yticklabels(xlabs)

    #ship track
    plt.text(txtlon,txtlat, ship_name.capitalize()+'\nShip Track',size=16, transform=ccrs.Geodetic())

    #mslp inset
    ins = ax.inset_axes([0.4,0.35,0.6,0.2])#[left, bottom, width, height]

    #bb=ax2.plot_date(pd.to_datetime(kml_df['dateHours']),pd.to_numeric(kml_df['baro'],errors='coerce'))
    bb=ins.scatter(min_list,kml_df['baro']/100,c=min_list,cmap=cmap)
    xx=ins.set_title('MLSP [inHg]',size=16,color=tclr)
    xx=ins.set_xticks(ticks=[])#xlt)
    qq=ins.set_yticks(ticks=[30,29.50])
    yy=ins.set_yticklabels(labels=[30,29.50],size=16,color=yclr)

    #tdry inset
    ins = ax.inset_axes([0.4,0.1,0.6,0.2])#[left, bottom, width, height]

    #bb=ax2.plot_date(pd.to_datetime(kml_df['dateHours']),pd.to_numeric(kml_df['baro'],errors='coerce'))
    bb=ins.scatter(min_list,kml_df['tdry'],c=min_list,cmap=cmap)
    xx=ins.set_title('Tdry [\N{DEGREE SIGN}F]',size=16,color=tclr)
    xx=ins.set_xticks(ticks=[])#xlt)
    qq=ins.set_yticks(ticks=[80,60])
    yy=ins.set_yticklabels(labels=[80,60],size=16,color=yclr)
    
    return fig
    
    
def makeKMLfile(gdrive_df,printFile=True):
    
    #ship_name
    ship_name=gdrive_df['ship_name'].iat[0]
    yr_name=str(gdrive_df['year_image'].iat[0])
    
    #extract row with lat/lon and dates if lat/lon different from previous
    
    kml_df=pd.DataFrame()
    lat_lst=[]
    lon_lst=[]
    date_lst=[]
    baro_lst=[]
    tdry_lst=[]

    for ind,row in gdrive_df.iterrows():

        if ind==0:
            #tlat,tlon
            tlat,tlon,dte,baro,tdry=row['comLat'],row['comLon'],row['dateHours'],row['baro'],row['tdry']

            lat_lst.append(tlat)
            lon_lst.append(tlon)
            date_lst.append(dte)
            baro_lst.append(baro)
            tdry_lst.append(tdry)

        if not ((tlat==row['comLat']) & (tlon==row['comLon'])):

            #tlat,tlon
            tlat,tlon,dte,baro,tdry=row['comLat'],row['comLon'],row['dateHours'],row['baro'],row['tdry']

            lat_lst.append(tlat)
            lon_lst.append(tlon)
            date_lst.append(dte)
            baro_lst.append(baro)
            tdry_lst.append(tdry)

    kml_df['comLat']=pd.Series(lat_lst)
    kml_df['comLon']=pd.Series(lon_lst)
    kml_df['dateHours']=pd.Series(date_lst)
    kml_df['baro']=pd.Series(baro_lst)
    kml_df['tdry']=pd.Series(tdry_lst)
    
    #======df into kml file=====
    
    #get the lat/lon
    shipPos=kml_df
    #ship_name='pennsylvania'

    kml = Kml(name=ship_name+'_'+yr_name+'.kml')

    #
    fol = kml.newfolder(name=ship_name+" voyage")

    #First ballon style 
    firstStyle = Style()

    firstStyle.iconstyle.icon_href = 'http://maps.google.com/mapfiles/kml/pushpin/red-pushpin.png'

    firstStyle.balloonstyle.textcolor = simplekml.Color.rgb(0, 0, 255)
    firstStyle.balloonstyle.text = '<![CDATA[ <font color="#CC0000" size="6">$[name]<br/></font><font face="Arial" size="4">$[description]</font>]]>'
    firstStyle.balloonstyle.bgcolor = simplekml.Color.lightgreen

    #For all balloon style 
    sharedstyle = Style()

    sharedstyle.iconstyle.icon_href = 'http://maps.google.com/mapfiles/kml/pal4/icon35.png'

    sharedstyle.balloonstyle.textcolor = simplekml.Color.rgb(0, 0, 255)
    sharedstyle.balloonstyle.text = '<![CDATA[ <font color="#CC0000" size="6">$[name]<br/></font><font face="Arial" size="4">$[description]</font>]]>'
    sharedstyle.balloonstyle.bgcolor = simplekml.Color.lightblue

    #loop through the points
    for ind,row in shipPos.iterrows():

        lon,lat,dte,baro,tdry=row['comLon'],row['comLat'],row['dateHours'],row['baro'],row['tdry']

        #first point
        if not ind:
            pnt = fol.newpoint()
            pnt.name="First"#"{0},{1}".format(lon, lat)
            pnt.description=dte+' <br>'+str(lat)+', '+str(lon)+' <br>'+'Baro: '+str(baro)+' <br>'+'Tdry: '+str(tdry)+'</br></br></br>'
            pnt.coords=[(lon,lat)]
            pnt.style = firstStyle
        #all other points
        else:

            pnt = fol.newpoint()
            pnt.name="{0}".format(ind+1)#name="{0},{1}".format(lon, lat), 
            pnt.coords=[(lon,lat)]#[(row['comLon'],row['comLat'])]
            pnt.description=dte+' <br>'+str(lat)+', '+str(lon)+' <br>'+'Baro: '+str(baro)+' <br>'+'Tdry: '+str(tdry)+'</br></br></br>'
            pnt.style = sharedstyle

    #underlying ship track
    y = np.asarray(shipPos['comLat'])#gg['Plat'])#np.linspace(0, 3 * np.pi, 5)
    x = np.asarray(shipPos['comLon'])#list(map(wrapTo360,cln.tolist())))#

    #segments
    points = np.array([x, y]).T.reshape(-1, 1, 2)
    segments = np.concatenate([points[:-1], points[1:]], axis=1)

    hh=[]
    for i in range(len(segments)):
        hh.append(segments[i][0].tolist())

    ls=fol.newlinestring(name=ship_name, description=ship_name+" ship track")

    ls.coords=hh
    ls.extrude = 1
    ls.altitudemode = simplekml.AltitudeMode.relativetoground
    ls.linestyle.width = 3
    ls.linestyle.color = simplekml.Color.orangered#blue
    
    out_folder='/Users/praveenteleti/Documents/GloSAT/WW2 project data/'
#     today=datetime.datetime.now().strftime('%d%m%Y')
    
    fn=out_folder+ship_name+'_'+yr_name+'.kml'

    if printFile:
        kml.save(fn)

    return kml.kml()
