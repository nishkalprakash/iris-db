# This script is to create a class that will have the following capabilities
# - Read iris images from mongodb for a given db
# - Write processed images back to mongodb for each image

# import current file parent into path
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

# TODO: move common imports to vars
# from vars import *
from sys import platform
DB_NAME = "iris-db"
DB_IP = "10.171.18.104"
if platform == "linux":
    DB_IP = "localhost"

from pymongo import MongoClient, collection

class Datasets:
    """
    In: Name of Dataset
    Out: Obj
    param: 
        path - Absolute path of the ds in disk
        dbcoll - name of the db collection
        avail_ds - List of all ds avail
    """
    
    def __init__(self,ds_name=None) -> object:
        
        self.avail_ds=sorted(AVAIL_DS)
        # if ds_name is not None:
            # self.connect(ds_name=ds_name)
    
    def connect(self,ds_name,coll=None) -> collection:
        """Will connect to the database into a given collection"""
        if 1 or ds_name in self.avail_ds:
            self.ds_name=ds_name if coll is None else coll
            self.ds_prefix=Path(DS_PREFIX)
            self.ds_path=self.ds_prefix/self.ds_name
            # self.dbcol=self.ds_name if coll is None else coll
            # return self
        else:
            print(ds_name,"Collection Not found in available datasets.")
        
        
        self.con=MongoClient(DB_IP)
        return self.con[DB_NAME][self.ds_name]
        # return self.conn

    def __del__(self):
        try:
            self.con.close()
        except Exception as e:
            print(str(e))
            
def get_data_from_mongodb(dbName,query,proj):
    ds=Datasets()
    db=ds.connect(dbName)
    docs=list(db.find(query,proj))
    del ds
    return docs

def update_data_to_mongodb(dbName,docs):
    ds=Datasets()
    db=ds.connect(dbName)
    for doc in docs:
        db.update_one({'_id':doc['_id']},{'$set':doc})
    # db.update
    del ds