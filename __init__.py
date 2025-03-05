# from setuptools_scm import get_version
from collections import Counter
from shapely import geometry
from osgeo import ogr, osr
from osgeo import gdal
from tqdm import tqdm
import koordinates
import os
import cx_Oracle
import yaml
import os.path
import time
import logging
import shutil


def version():
    __version__ = get_version()
    print(f"Version: {__version__}")

def clean_up_files(polyshp, shpdir, clippedRas):
    try:
        if os.path.exists(polyshp):
            cleanshp(shpdir)
        if os.path.exists(clippedRas):
            os.remove(clippedRas)
    except OSError as e:
        logging.error(f"Error cleaning up files: {e}")    

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
    try:
        lstring = pline.replace('LINESTRING ', '')
        lstring = lstring.strip('(')
        lstring = lstring.strip(')')
    except AttributeError as e:
        print(f"Error processing input string: {e}")
        return None

    clist = []
    try:
        slist = lstring.split(",")
        for pt in slist:
            cpt = pt.strip()
            rncl = cpt.split(" ")
            if float(rncl[0]) < 0:
                fixlong = float(rncl[0]) + 360
                rncl[0] = str(fixlong)
            clist += [(rncl[0], rncl[1]),]
    except (ValueError, IndexError) as e:
        print(f"Error processing coordinates: {e}")
        return None

    try:
        poly = geometry.Polygon([[float(p[0]), float(p[1])] for p in clist])
    except ValueError as e:
        print(f"Error creating polygon: {e}")
        return None

    return poly

def getrncpoly(pline):
    try:
        lstring = pline.replace('LINESTRING ', '')
        lstring = lstring.strip('(')
        lstring = lstring.strip(')')

        clist = []
        slist = lstring.split(",")
        for pt in slist:
            cpt = pt.strip()
            rncl = cpt.split(" ")
            if float(rncl[0]) < 0:
                fixlong = float(rncl[0]) + 360
                rncl[0] = str(fixlong)

            clist += [(rncl[0], rncl[1]),]

        poly = geometry.Polygon([[p[0], p[1]] for p in clist])
        return poly
    
    except ValueError as e:
        print(f"Value error occurred: {e}")
        return None
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def rncpolytoshp(poly, polyshp, sheet):
    try:
        if not all([poly, polyshp, sheet]):
            raise ValueError("All input parameters must be provided and non-empty.")
        
        # create the spatial reference system, WGS84, 4326
        srs = osr.SpatialReference()
        srs.SetFromUserInput('WGS84')
        
        driver = ogr.GetDriverByName('Esri Shapefile')
        if driver is None:
            raise RuntimeError("Esri Shapefile driver is not available.")
        
        ds = driver.CreateDataSource(polyshp)
        if ds is None:
            raise RuntimeError("Failed to create data source.")
        
        layer = ds.CreateLayer('CropRegion', geom_type=ogr.wkbPolygon, srs=srs)
        if layer is None:
            raise RuntimeError("Failed to create layer.")
        
        layer.CreateField(ogr.FieldDefn('id', ogr.OFTInteger))
        defn = layer.GetLayerDefn()
        
        feat = ogr.Feature(defn)
        feat.SetField('id', sheet)
        
        geom = ogr.CreateGeometryFromWkb(poly.wkb)
        if geom is None:
            raise RuntimeError("Failed to create geometry from WKB.")
        
        feat.SetGeometry(geom)
        
        if layer.CreateFeature(feat) != 0:
            raise RuntimeError("Failed to create feature in layer.")
        
        # Clean up
        feat = geom = None
        ds = layer = None
    
    except ValueError as ve:
        print("ValueError:", ve)
    except RuntimeError as re:
        print("RuntimeError:", re)
    except Exception as e:
        print("An unexpected error occurred:", e)

def expgeotiff(sheet,ChartVN,inRas,username,password,service_name):
    try:
        if not all([sheet, ChartVN, inRas, username, password, service_name]):
            raise ValueError("All input parameters must be provided and non-empty.")
        
        cariscon = 'hpd://'+username+':'+password+'@'+service_name+'/db?ChartVersionId='+ChartVN
        filepath = inRas

        batch = "carisbatch -r ExportChartToTIFF -D 300 -e EXPORT_AREA -d 32 -C RGB(255,255,255,100)  -g -p {} {} {} 2> c:\\temp\\process-errors.txt".format(sheet, cariscon, filepath)
        print('Export GeoTIFF as: ', filepath)
        
        if os.path.exists(filepath):
            os.remove(filepath)
        
        exportresult = os.system(batch)
        
        if exportresult != 0:
            raise RuntimeError("Export process failed with exit code {}".format(exportresult))
        
        return exportresult
    
    except ValueError as ve:
        print("ValueError:", ve)
        return None
    except RuntimeError as re:
        print("RuntimeError:", re)
        return None
    except Exception as e:
        print("An unexpected error occurred:", e)
        return None
    
gdal.UseExceptions()

def clippedchart(polyshp, inRas, clippedRas, clayer):
    inshp = polyshp
    inRas = inRas
    outRas = clippedRas
    
    try:
        if os.path.exists(outRas):
            os.remove(outRas)
    except OSError as e:
        print(f"Error removing existing file {outRas}: {e}")
        return
    
    try:
        OutDS = gdal.Warp(outRas, inRas, cutlineDSName=inshp, cutlineLayer=clayer, cropToCutline=True, dstSRS='EPSG:4326')
        if OutDS is None:
            raise RuntimeError("gdal.Warp returned None")
    except RuntimeError as e:
        print(f"Error during gdal.Warp: {e}")
        return
    finally:
        if OutDS:
            OutDS = None

def compchart(clippedRas, ldsRas):
    inRas = clippedRas
    outRas = ldsRas
    
    try:
        if os.path.exists(ldsRas):
            os.remove(ldsRas)
    except OSError as e:
        print(f"Error removing existing file {ldsRas}: {e}")
        return
    
    try:
        translateoptions = gdal.TranslateOptions(gdal.ParseCommandLine("-of Gtiff -co COMPRESS=LZW"))
        gdal.Translate(outRas, inRas, options=translateoptions)
    except RuntimeError as e:
        print(f"Error during gdal.Translate: {e}")
        return
    finally:
        translateoptions = None

def hpd_exp(username, password,dsn,chartID, sheetNO):
    # Oracle DB connection details
    connection = None
    cursor = None
    connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)

    sql = """
        SELECT 
        a.panelver_id, c.CHARTVER_ID, c.STRINGVAL, a.panelver_id,
        e.intval as panelnumber,
        TO_CHAR(SDO_UTIL.TO_WKTGEOMETRY(d.LLDG_geom)) as GEOM
        FROM 
        panel_feature_vw a, 
        CHART_SHEET_PANEL_VW b, 
        CHART_ATTRIBUTES_VIEW c, 
        hpd_spatial_representation d,
        panel_version_attribute e 
        WHERE 
        a.object_acronym = '$rncpanel' 
        and a.rep_id=d.rep_id 
        and a.panelver_id = b.panelver_id 
        and b.chartver_id = c.chartver_id
        and e.panelvr_panelver_id=a.panelver_id 
        and e.attributeclass_id=171 
        and c.acronym = 'CHTNUM'
        and c.chartver_id = :chartID
        and e.intval = :sheetNO
        """
    try:
        cursor = connection.cursor()
        cursor.execute(sql, chartID=chartID, sheetNO=sheetNO)
        out_data = cursor.fetchall()

        for ele, count in Counter(out_data).items():
            sheetID = str(ele[0])
            ChartVN = str(ele[1])
            ChartN = ele[2]
            pline = ele[5]
            sheet = str(ele[4])
            sheetn = "%02d" %ele[4]

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
                    print("=============================================================================")
                    print("Chart " + ChartN +" ID"+ChartVN+" and Panel Number "+ sheet +" : Rncpanel data exported.")
        
    except cx_Oracle.DatabaseError as e:
        print(f"Database error occurred: {e}")
        return None
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    finally:
        cursor.close()
        connection.close()

    return ChartVN,ChartN,pline,sheet,sheetn,sheetID

def getchartstyle(ChartVN, username, password, dsn):

    connection = None
    cursor = None
    try:
        connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)
        cursor = connection.cursor()

        sql = """
        select CHARVAL 
        from chart_version_attribute 
        where chartver_chartver_id = :c_id and attributeclass_id = 1117
        """
        cursor.execute(sql, c_id=ChartVN)
        out_data = cursor.fetchall()
        cstyle = None
        for i in out_data:
            cstyle = i[0]
            return cstyle
    
    except cx_Oracle.DatabaseError as e:
        print(f"Database error occurred: {e}")
        return None
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def chartstyle(cstyle):
    #User input Chart style
    print("=============================================================================")
    print("Current Chart style set is: "+cstyle)
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

def updatechartstyle(ChartVN, new_style, username, password, dsn):
    connection = None
    cursor = None

    try:
        connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)
        cursor = connection.cursor()

        sql = """
        update chart_version_attribute
        set CHARVAL = :new_style
        where chartver_chartver_id = :c_id and attributeclass_id = 1117
        """
        cursor.execute(sql, new_style=new_style, c_id=ChartVN)
        connection.commit()
        print("Chart style updated successfully.")

    except cx_Oracle.DatabaseError as e:
        print(f"Database error occurred: {e}")
        if connection:
            connection.rollback()

    except Exception as e:
        print(f"An error occurred: {e}")
        if connection:
            connection.rollback()

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def get_ldsid(chart_number, lds_number, lds_host, lds_key):
    lds_val = None
    detail_id = ''
    try:
        client = koordinates.Client(host=lds_host, token=lds_key)
        for layer in client.layers.list():
            if f'Chart NZ {chart_number} ' in layer.title:
                layer_detail = client.layers.get(layer.id)
                detail_id = layer_detail.id
                logging.info(f'Layer ID: {layer_detail.id}')
                for key, val in layer_detail.data.source_summary.items():
                    if key == 'paths' and lds_number in val[0]:
                        logging.info(f'Path: {val[0]}, Layer ID: {layer.id}')
                        lds_val = layer.id
                        break  # Exit the loop once the value is found
            if lds_val:
                break  # Exit the loop once the value is found
    except Exception as e:
        logging.error(f'Error retrieving LDS ID: {e}')
    
    return lds_val if lds_val is not None else detail_id


def lds_data_source_scan(lds_host, lds_key, source_id=126622):
    try:
        client = koordinates.Client(host=lds_host, token=lds_key)

        # Start a new scan
        scan = client.sources.start_scan(source_id)

        # Wait for 10 minutes with a progress bar
        for _ in tqdm(range(600), desc="Waiting for scan to complete", unit="s"):
            time.sleep(1)

        # Retrieve the last scan
        last_scan = client.sources.list_scans(source_id=source_id)[0]

        # Get the scan details
        scans = client.sources.get_scan(source_id, last_scan)
        lds_status = scans.status
        if lds_status == 'completed':
            logging.info(f"Scan ID: {last_scan}, Status: {scans.status}, "
                         f"Started at: {scans.started_at.strftime('%x %X')}, "
                         f"Completed at: {scans.completed_at.strftime('%x %X')}")
        else:
            logging.error(f"Scan ID: {last_scan}, Status: {scans.status}, "
                          f"Started at: {scans.started_at.strftime('%x %X')}, "
                          f"Completed at: {scans.completed_at.strftime('%x %X')}")

    except koordinates.exceptions.KoordinatesException as e:
        logging.error(f'Error retrieving LDS data source: {e}')
    except Exception as e:
        logging.error(f'Unexpected error: {e}')

    return lds_status

def execute_lds_bulk_update(file_path):
   try:
      os.system(f'python {file_path}')
   except FileNotFoundError:
      print(f"Error: The file '{file_path}' does not exist.")  
   except Exception as e:
        print(f"An error occurred: {e}")

def writeldsconfig(ldsids, ldscfg):
    try:
        with open(ldscfg, 'r') as file:
            config = yaml.safe_load(file)
        
        config['Datasets']['Layers'] = ldsids
        
        with open(ldscfg, 'w') as file:
            yaml.dump(config, file)
        
        logging.info(f'Successfully updated LDS configuration file: {ldscfg}')
    except FileNotFoundError:
        logging.error(f'Configuration file not found: {ldscfg}')
    except yaml.YAMLError as e:
        logging.error(f'Error parsing YAML file: {e}')
    except Exception as e:
        logging.error(f'An unexpected error occurred: {e}')
            

def copy_to_share(source, lds_destination, lds_back_up):
    try:
        if os.path.exists(lds_destination):
            shutil.copy2(lds_destination, lds_back_up)
            os.remove(lds_destination)
        shutil.copy2(source, lds_destination)
    except Exception as e:
        print(f"An error occurred: {e}")

def readconfig(config_path="config.yaml"):                   ##########################################
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
            file.close()  
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Configuration file not found: {e}")
    
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file: {e}")

    try:
        hostname = config["oracle"]["hostname"]
        port = config["oracle"]["port"]
        service_name = config["oracle"]["service_name"]
        # username = config["oracle"]["username"]
        # password = config["oracle"]["password"]
        charts = config["Datasets"]["Charts"]
        savepath = config["SAVE_PATH"]
        backup = config["Backup_PATH"]
        ldsstag = config["LDS_STAG"]
        lds_host = config['LDSConnection']['host']
        lds_key = config['LDSConnection']['token']
        ldscfg = config['LDSConnection']['config']
        ## manual input for compiled running test file.
        username = 'lhsphpd' 
        password = ']SNaEXNn9FF'

    except KeyError as e:
        raise KeyError(f"Missing configuration key: {e}")

    dsn = cx_Oracle.makedsn(hostname, port, service_name=service_name)
    return charts, dsn, username, password, savepath, service_name, ldsstag, lds_host, lds_key, ldscfg, backup

  

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

WAIT_TIME = 3  # Define wait time as a constant

def main():
    try:
        # Read database credentials and user input from config.yaml
        charts, dsn, username, password, savepath, service_name, ldsstag, lds_host, lds_key, ldscfg, backup = readconfig()
        lds_ids =[]
        # Query Chart data from HPD
        # sheetID -- for checking LDS nzme
        for chartID, sheetNO in charts:     
            ChartVN, ChartN, pline, sheet, sheetn, sheetID = hpd_exp(username, password, dsn, chartID, sheetNO)

            # Current chart publishing style check from HPD
            updatechk = 0
            chtstyle = getchartstyle(ChartVN, username, password, dsn)
            bsbstyle = chartstyle(chtstyle)
            if chtstyle != bsbstyle:
                style = bsbstyle
                updatechartstyle(ChartVN, style, username, password, dsn)
                updatechk = 1
                logging.info(f"Chart Style updated as {bsbstyle}")

            # Convert Rncpanel data to polygon
            poly = getrncpoly(pline)

            # Set process file names
            polyshp = os.path.join(savepath, f"{ChartVN}_{ChartN}_{sheet}.shp")
            shpdir = os.path.join(savepath, f"{ChartVN}_{ChartN}_{sheet}")
            inRas = os.path.join(savepath, f"{ChartVN}_{ChartN}_{sheet}.tif")
            clippedRas = os.path.join(savepath, f"{ChartVN}_{ChartN}_{sheet}_c.tif")
            clayer = f"{ChartVN}_{ChartN}_{sheet}"
            ldsRas = os.path.join(savepath, f"{ChartN}{sheetn}.tif")
            
            clean_up_files(polyshp, shpdir, clippedRas)

            time.sleep(WAIT_TIME)
            # Convert Rncpanel data to shp
            rncpolytoshp(poly, polyshp, sheet)
            # Export GeoTiff Chart with Carisbatch 
            exportresult = expgeotiff(sheet, ChartVN, inRas, username, password, service_name)

            if exportresult == 0:
                logging.info("GeoTIFF Exported")
                time.sleep(WAIT_TIME)
                # Cutting Chart with Rncpanel boundary
                clippedchart(polyshp, inRas, clippedRas, clayer)
                time.sleep(WAIT_TIME)
                # Chart image Compression
                compchart(clippedRas, ldsRas)

            # Update LDS configuration file for the bulk update
                # Ensure all necessary variables are defined and initialized
                chart_prefix = ChartN[0:2]
                chart_number = ChartN[2:]
                lds_destination = ldsstag + ChartN + sheetn + ".tif"
                timestamp = time.strftime("_%Y-%m-%d_%H-%M-%S")
                lds_back_up = backup + ChartN + sheetn + timestamp + ".tif"
                lds_number = chart_number + sheetn
                
                # Logging chart details
                logging.info(f'Chart Number: {chart_number}, Chart Prefix: {chart_prefix}')
                
                # Get LDS ID and append to list
                try:
                    lds_id = get_ldsid(chart_number, lds_number, lds_host, lds_key)
                    logging.info(f'Returned LDS ID: {lds_id}, Type: {type(lds_id)}')
                    lds_ids.append(lds_id)
                except Exception as e:
                    logging.error(f'Error getting LDS ID: {e}')
                
                # Write LDS configuration
                try:
                    writeldsconfig(lds_ids, ldscfg)
                except Exception as e:
                    logging.error(f'Error writing LDS config: {e}')
                
                # Copy to the LDS staging source data set and backup LDS charts
                try:
                    copy_to_share(ldsRas, lds_destination, lds_back_up)
                except Exception as e:
                    logging.error(f'Error copying to share: {e}')

            else:
                logging.error("GeoTIFF Export Error, Please check the error logs at process-errors.txt")

            if updatechk == 1:
                style = chtstyle
                updatechartstyle(ChartVN, style, username, password, dsn)
                logging.info(f"Chart Style returned as {style}")

            clean_up_files(polyshp, shpdir, clippedRas)
        
        lds_scan = lds_data_source_scan(lds_host, lds_key)
        if lds_scan == 'completed':
            logging.info("LDS data source scan completed successfully.")
            execute_lds_bulk_update('LDS_bulk_updates.py') ##src\chart_exp\LDS_bulk_updates.py

        else:
            logging.error("LDS data source scan failed.")



    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
