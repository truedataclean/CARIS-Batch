#Title: Export GeoTIFF from HPD data
#Edition: Third Edition
#Date: 2024-08-08
#Author: dpark@linz.govt.nz
#Description: This script is used to create GeoTIFFs from HPD data.
#Updates:
# User select chart style.
# Update jira document location for RNC Panel data fixing.  
#Dependencies: 
# Caris HPD PaperChartBuilder License 
# Oracle Client for HPD connection
# N(Mariners) Drive connection for Carisbatch running.
#Usage: The script is run from the command line. 
# The script requires a config file to be passed as an argument: c:\temp\config.yml

# Import the required libraries
import os
import os.path
import oracledb
from shapely import geometry
from osgeo import ogr, osr, gdal
from collections import Counter
import yaml
import time

os.unsetenv('PROJ_LIB')
gdal.UseExceptions()

Save = 'C:\\Temp\\chart\\'
Config = 'C:\\Temp\\config.yml'

isExist = os.path.exists(Save)
if not isExist:
   os.makedirs(Save)


def chartstyle(chtstyle):
    #User input Chart style
    print("=============================================================================")
    print("Current Chart style set: "+chtstyle)
    print("No[1] LINZ_BSB")
    print("No[2] LINZ_BSB-v2.0")
    print("No[3] LINZ_BSB-v3.0")

    while True:
        try:
            num = int(input("Please enter an Chart Style Number: "))
            if num < 1 or num > 3:
                print("Please input a valid style number as 3.")
            else:
                break
        except ValueError:
            print("Please input a valid style number as 3.")

    if num == 1:
        style = "LINZ_BSB"
    elif num == 2:
        style = "LINZ_BSB-v2.0"
    elif num == 3:
        style = "LINZ_BSB-v3.0"

    return style

def compchart(clippedRas,ldsRas):
    inRas = clippedRas
    outRas = ldsRas
    if os.path.exists(ldsRas):
            os.remove(ldsRas) 
    
    translateoptions = gdal.TranslateOptions(gdal.ParseCommandLine("-of Gtiff -co COMPRESS=LZW"))
    gdal.Translate(outRas, inRas, options=translateoptions)

    translateoptions = None

def clippedchart(polyshp,inRas,clippedRas,clayer):
    inshp = polyshp
    inRas = inRas
    outRas = clippedRas
    if os.path.exists(outRas):
            os.remove(outRas) 
    
    OutDS= gdal.Warp(outRas, inRas, cutlineDSName= inshp, cutlineLayer= clayer, cropToCutline=True, dstSRS='EPSG:4326')

    OutDS = None

def expgeotiff(sheet,ChartVN,inRas,user,password,Dbname):
    cariscon = 'hpd://'+user+':'+password+'@'+Dbname+'/db?ChartVersionId='+ChartVN
    filepath = inRas

    batch = "carisbatch -r ExportChartToTIFF -D 300 -e EXPORT_AREA -d 32 -C RGB(255,255,255,100)  -g -p {} {} {} 2> c:\\temp\\process-errors.txt".format(sheet,cariscon,filepath) 
    print('Export GeoTIFF as: ',filepath)
    if os.path.exists(filepath):
        os.remove(filepath)
        
    exportresult = os.system(batch)
    
    return exportresult

def rncpolytoshp(poly,polyshp, sheet):     

    # create the spatial reference system, WGS84, 4326
    srs =  osr.SpatialReference()
    srs.SetFromUserInput('WGS84')
 
    driver = ogr.GetDriverByName('Esri Shapefile')
    ds = driver.CreateDataSource(polyshp)
    layer = ds.CreateLayer('CropRegion', geom_type=ogr.wkbPolygon,srs=srs)
    layer.CreateField(ogr.FieldDefn('id', ogr.OFTInteger))
    defn = layer.GetLayerDefn()

    feat = ogr.Feature(defn)
    feat.SetField('id', sheet)

    geom = ogr.CreateGeometryFromWkb(poly.wkb)
    feat.SetGeometry(geom)

    layer.CreateFeature(feat)
    feat = geom = None 
    ds = layer = feat = geom = None

def cleanshp(shpdir):
    polyshp =shpdir +".shp"
    psf = shpdir +".dbf"
    psp = shpdir +".prj"
    psx = shpdir +".shx"
    try:
            os.remove(polyshp)
            os.remove(psf)
            os.remove(psp)
            os.remove(psx)
    except OSError as e:
            print ("Error code:", e.code)

def getrncpoly(pline):
    lstring = pline.replace('LINESTRING ', '')
    lstring = lstring.strip('(')
    lstring = lstring.strip(')')

   
    clist = []
    slist = lstring.split(",")
    for pt in slist:
        cpt = pt.strip()
        rncl = cpt.split(" ")
        if float(rncl[0]) <0:
            fixlong = float(rncl[0]) + 360
            rncl[0] = str(fixlong)

        clist += [(rncl[0],rncl[1]),]
        

    poly = geometry.Polygon([[p[0], p[1]] for p in clist]) 
    return poly

def putchartstyle(ChartVN, style, user, password,dsn):

        connection = oracledb.connect(
        user=user,
        password=password,
        dsn=dsn)

        cursor = connection.cursor()

        sql = """
        UPDATE chart_version_attribute SET CHARVAL = :c_style where chartver_chartver_id = :c_id and attributeclass_id = 1117
        """
        cursor.execute(sql, c_style = style, c_id = ChartVN)
        connection.commit()
        connection.close()    

def getchartstyle(ChartVN,user, password,dsn):

        connection = oracledb.connect(
        user=user,
        password=password,
        dsn=dsn)

        cursor = connection.cursor()

        sql = """
        select CHARVAL from chart_version_attribute where chartver_chartver_id = :c_id and attributeclass_id = 1117
        """
        cursor.execute(sql, c_id = ChartVN)
        out_data = cursor.fetchall()
        for i in out_data:
             cstyle = i[0]
        connection.close()    
        return cstyle

def rncfromhpd(chartname,user, password,dsn):

        connection = oracledb.connect(
        user=user,
        password=password,
        dsn=dsn)

        cursor = connection.cursor()

        print("Successfully connected to Oracle Database")

        sql = """
        select c.CHARTVER_ID, c.STRINGVAL, a.panelver_id, d.compositegeom_id,b.product_status,e.intval as panelnumber, TO_CHAR(SDO_UTIL.TO_WKTGEOMETRY(d.LLDG_geom)) as GEOM
        from panel_feature_vw a, CHART_SHEET_PANEL_VW b, CHART_ATTRIBUTES_VIEW c, hpd_spatial_representation d,panel_version_attribute e 
        where a.object_acronym = '$rncpanel' and a.rep_id=d.rep_id and a.panelver_id = b.panelver_id and b.chartver_id = c.chartver_id
        and e.panelvr_panelver_id=a.panelver_id and e.attributeclass_id=171 and c.acronym = 'CHTNUM'
        and c.STRINGVAL = :c_name"""

        cursor.execute(sql, c_name = chartname)
        out_data = cursor.fetchall()
        connection.close()    
        return out_data



def main():    
    
    with open(Config, 'r') as file:
        HPDConnection = yaml.safe_load(file)
        # user = HPDConnection['HPDConnection']['User']
        # password = HPDConnection['HPDConnection']['PW']
        # dsn = HPDConnection['HPDConnection']['dsn'] +':'+ HPDConnection['HPDConnection']['port'] +'/'+ HPDConnection['HPDConnection']['DBname']
        # Dbname = HPDConnection['HPDConnection']['DBname']     
        Charts = HPDConnection['Datasets']['Charts']
        user = 'lhsphpd'
        password = ']SNaEXNn9FF'
        dsn = 'CARIS-DHPD.ad.linz.govt.nz:1522/DHPD'
        Dbname = 'DHPD'

        for chart in Charts:
            chartname = chart
            print("Data export for Chart " + chartname)

            out_data = rncfromhpd(chartname, user, password,dsn)

            for ele, count in Counter(out_data).items():
                 ChartVN = str(ele[0])
                 ChartN = ele[1]
                 pline = ele[6]
                 sheet = str(ele[5])

                 sheetn = "%02d" %ele[5]
                 polyshp = Save + ChartVN +"_"+ ChartN + "_" + sheet +".shp"
                 shpdir = Save + ChartVN +"_"+ ChartN + "_" + sheet
                 inRas = Save + ChartVN + "_" + ChartN + "_" + sheet +".tif"
                 clippedRas = Save + ChartVN + "_" + ChartN + "_" + sheet +"_c.tif"
                 clayer = ChartVN +"_"+ ChartN + "_" + sheet
                 ldsRas = Save + ChartN + sheetn +".tif"
                

            
                # Uncertified(Duplicated) data check
                 if count >1:
                    print("Chart " + ChartN +" ID"+ChartVN+" and Panel Number "+ sheet +" has "+ str(count -1) +" duplicate Rnc Panel data," "\n" "Check HPD Paper Chart Editor for uncertified deletion.")
                    print("\n")

                # Rnc panel data check
                 elif count == 1:
                    if pline is None:
                        print("=============================================================================")
                        print("Chart " + ChartN +" ID"+ChartVN+" and Panel Number "+ sheet +" does not have Rncpanel coordinate data,")
                        print("Please check the instruction below to generate the Rnc panel data.")
                        print("https://toitutewhenua.atlassian.net/wiki/spaces/LI/pages/643760129/How+to+generate+Rnc+panel+data.")
                        print("\n")
                    
                    else:
                        # Current chart style check
                        updatechk = 0
                        chtstyle = getchartstyle(ChartVN,user,password,dsn)
                        bsbstyle = chartstyle(chtstyle)
                        
                        if chtstyle != bsbstyle:
                              style = bsbstyle
                              putchartstyle(ChartVN,style,user,password,dsn)
                              updatechk = 1
                              print("Chart Style updated as "+bsbstyle)

                        poly = getrncpoly(pline)
                        if os.path.exists(polyshp):  
                            cshp = cleanshp(shpdir)
                        wait = 3
                        time.sleep(wait)
                        rncshp = rncpolytoshp(poly,polyshp,sheet)

                        exportresult = expgeotiff(sheet,ChartVN,inRas,user,password,Dbname)
                        
                        if exportresult == 0:
                            print("GeoTIFF Exported")
                            wait = 3
                            time.sleep(wait)

                            clippedchart(polyshp,inRas,clippedRas,clayer)
                            time.sleep(wait)
                            compchart(clippedRas,ldsRas)
                
                        else:
                            print("GeoTIFF Export Error, Please check the error logs atprocess-errors.txt")

                        if updatechk == 1 :
                            style = chtstyle
                            putchartstyle(ChartVN,style,user,password,dsn)
                            print("Chart Style returned as " + style)

                    if os.path.exists(polyshp):
                        cshp = cleanshp(shpdir)
                    if os.path.exists(clippedRas):
                        os.remove(clippedRas) 
                        


        
    file.close()


if __name__ == "__main__":
    main()
    
