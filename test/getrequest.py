# This file is to automatically save map from
# the extracted geometry information with postgresql
# and conver them into geojson to topojson to visualize
import os
import json
import geojson
import psycopg2
from psycopg2.extras import RealDictCursor
from PIL import Image
import time

#connet the database
file = open(os.path.dirname(os.path.abspath(__file__)) + "\pg.credentials")
connection_string = file.readline() + file.readline()
pg = psycopg2.connect(connection_string)

#start point for timing
Tstart = time.clock()

#loop for 25 maps
for i in range(1,26):
    #start point for timing
    start = time.clock()
    
    #specieNum is the name used in python replaced the spnr in database
    specieNum = i

    records_query = pg.cursor(cursor_factory=RealDictCursor)
    #request the name of the specie
    records_query.execute("""
    SELECT f.eng_name
    FROM s6035280.final as f
    WHERE f.id = '%d'
    """% (specieNum))
    bname = json.dumps(records_query.fetchall())
    bname = bname[15:-3]
    name = bname.replace(' ','_')
    
    #request the species distribution
    records_query.execute("""
        SELECT json_astext(json_field_astext(ST_AsGeoJSON(f.species_geom)))::json AS geometry
        FROM s6035280.final as f
        WHERE f.id = '%d'
    """ % (specieNum))
    distribution = json.dumps(records_query.fetchall())
    distribution = '{"name": "species","type": "FeatureCollection","features": [{ "type": "Feature", "properties": { }, "geometry": ' + distribution[14:-1] + ']}'

    #request the geometry of provinces
    records_query.execute("""
        SELECT json_astext(json_field_astext(ST_AsGeoJSON(f.state_geom)))::json AS state
        FROM s6035280.final as f
        WHERE f.id = '%d'
        """ % (specieNum))
    stateg = json.dumps(records_query.fetchall())
    stateg = '{"name": "prov","type": "FeatureCollection","features": [{ "type": "Feature", "properties": { }, "geometry": ' + stateg[11:-1] + ']}'

    #request the geometry of provinces
    records_query.execute("""
        SELECT json_astext(json_field_astext(ST_AsGeoJSON(f.stateboundary_geom)))::json AS state
        FROM s6035280.final as f
        WHERE f.id = '%d'
        """ % (specieNum))
    stateb = json.dumps(records_query.fetchall())
    stateb = '{"name": "stateb","type": "FeatureCollection","features": [{ "type": "Feature", "properties": { }, "geometry": ' + stateb[11:-1] + ']}'

    #request the geometry of bbox
    records_query.execute("""
        SELECT json_astext(json_field_astext(ST_AsGeoJSON(f.bbox_geom)))::json AS state
        FROM s6035280.final as f
        WHERE f.id = '%d'
    """ % (specieNum))
    bbox_g = json.dumps(records_query.fetchall())
    bbox_g = '{"name": "prov","type": "FeatureCollection","features": [{ "type": "Feature", "properties": { }, "geometry": ' + bbox_g[11:-1] + ']}'

    #request the geometry of country boundary
    records_query.execute("""
        SELECT json_astext(json_field_astext(ST_AsGeoJSON(f.cntryboundary_geom)))::json AS state
        FROM s6035280.final as f
        WHERE f.id = '%d'
    """ % (specieNum))
    cntryb = json.dumps(records_query.fetchall())
    cntryb = '{"name": "Country","type": "FeatureCollection","features": [{ "type": "Feature", "properties": { }, "geometry": ' + cntryb[11:-1] + ']}'

    #create a new folder
    #nowpath is where the python file is stored
    nowpath = os.getcwd()
    crepath = nowpath + '/cache'  # used for store geojson and topojson cache
    mapspath = nowpath + '/maps'  # used for store maps

    #check and create 2 requested folder
    def createFolder(folderpath):
        folder = os.path.exists(folderpath)
        if not folder:
            os.makedirs(folderpath)
    createFolder(crepath)
    createFolder(mapspath)    
    
    #save the geojsons into 4 different files
    fh = open(crepath+'/Species.geojson', 'w')
    fh.write(distribution)
    fh.close()

    fh = open(crepath+'/Prov.geojson', 'w')
    fh.write(stateg)
    fh.close()

    fh = open(crepath+'/Stateb.geojson', 'w')
    fh.write(stateb)
    fh.close()

    fh = open(crepath+'/Bbox.geojson', 'w')
    fh.write(bbox_g)
    fh.close()

    fh = open(crepath+'/Country.geojson', 'w')
    fh.write(cntryb)
    fh.close()

    #changes the current working directory to the path you save this python code
    os.chdir(nowpath)
    
    #convert geo2topo files
    command1 = 'geo2topo ' + crepath +'/Species.geojson>'+ crepath +'/Species.json'
    command2 = 'geo2topo ' + crepath +'/Prov.geojson>'+ crepath +'/Prov.json'
    command3 = 'geo2topo ' + crepath +'/Stateb.geojson>'+ crepath +'/Stateb.json'
    command4 = 'geo2topo ' + crepath +'/Bbox.geojson>'+ crepath +'/Bbox.json'
    command5 = 'geo2topo ' + crepath +'/Country.geojson>'+ crepath +'/Country.json'
    
    #Convert Vega-lite to Vega
    command6 = 'vl2vg '+ nowpath +'/bar.vl.json > '+ nowpath +'/vega.json'

    #Transform to PNG
    command7 = 'vg2png -b '+ nowpath + '/ vega.json '+ mapspath +'/orign_' + str(name) +'.png'

    #use command prompt to execute
    os.system(command1)
    os.system(command2)
    os.system(command3)
    os.system(command4)
    os.system(command5)    
    os.system(os.path.normpath(command6))
    os.system(command7)

    #resize the picture to 200 dpi
    imag = Image.open(mapspath +'/orign_' + str(name) +'.png').resize((200,200),Image.ANTIALIAS)
    imag.save(mapspath +'/dpi200_' + str(name) +'.png')

    #end point for timing
    elapsed = (time.clock() - start)

    #attention for finishing one specie
    print("Finish ", i,", specie: ",bname,". Time used:",elapsed, ' second(s).', sep = '')

#Close connection with database
pg.close ()

#end point for timing
Telapsed = (time.clock() - Tstart)

print("\nFinished output Atlas in ",Telapsed,' seconds.', sep = '')
