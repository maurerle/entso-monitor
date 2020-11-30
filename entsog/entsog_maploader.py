import requests
import os
import shutil
routes = ['countries_zones','pipelines_small_medium_large','pipelines_medium_large','pipelines_large','drilling_platforms','gasfields','projects','country_names']

url = "https://transparency.entsog.eu/assets/images/map_layers/"

def loadMap(folder,route,url=url):
    curUrl = url+route
    for z in range(2,6):
        numItems = 2**z
        for x in range(numItems):
            saveFolder = "{}/{}/{}/{}".format(folder,route,z,x)
            if not os.path.exists(saveFolder):
                os.makedirs(saveFolder)
                for y in range(numItems):
                    print(saveFolder)
                    r = requests.get("{}/{}/{}/{}.png".format(curUrl,z,x,y))

                    with open(saveFolder+"/{}.png".format(y), "wb") as f:
                        f.write(r.content)

def convertTmsXyz(source,target):    
    for z in range(2,6):
        numItems = 2**z
        for x in range(numItems):
            saveFolder = "{}/{}/{}".format(target,z,x)
            sourceFolder = "{}/{}/{}".format(source,z,x)
            if not os.path.exists(saveFolder):
                  os.makedirs(saveFolder)
            print(saveFolder)
            for y in range(numItems):
                shutil.copy(sourceFolder+"/{}.png".format(y), saveFolder+"/{}.png".format(numItems-y-1))
if __name__ == "__main__":  
    for route in routes:
        loadMap('data_tms',route)
        convertTmsXyz('data_tms/'+route,'data_xyz/'+route)
    