from pymongo import MongoClient
from pymongo.collection import Collection
from lib import LoggerManager as lm, FileManager as fm




# Setup logging
from functools import lru_cache
from difflib import get_close_matches

# from time import sleep
# logger = LoggerManager.get_logger(name=Path(__file__).stem)
lg = lm.get_logger(__name__,level="DEBUG")
# lg = lm.get_logger(__name__,level="INFO")
# set log level to info
# lg.setLevel(logging.DEBUG)

class IrisDB:
    """
    In: None
    Out: IrisDB Object that can be used to connect to a particular db
    param: 
        ds_id - name of the dataset collection
    """
    DB_IP = 'localhost'
    DB_NAME = 'iris_db'
    META_COLL_NAME = 'meta'

    def __init__(
        self, 
        ds_id=None,
        db_ip=None, 
        mongo_db_name=None, 
        meta_coll_name=None
        # db_coll=None, # this could be used to shortcircuit the process?
        ) -> object:
        self.db_ip = self.DB_IP if db_ip is None else db_ip
        self.mongo_db_name = self.DB_NAME if mongo_db_name is None else mongo_db_name
        self.meta_coll_name = self.META_COLL_NAME if meta_coll_name is None else meta_coll_name
        # self.db_coll = DB_COLL if db_coll is None else db_coll
        ## get user:passwd from mongo_creds.txt file
        user,passwd = fm.read_creds()
        self._mongo_admin_user = user
        self._mongo_admin_password = passwd
        self.closing = False
        if ds_id is not None:
            self.connect(ds_id)
        # fm.ensure_exists(self.DB_BASE_)
        # set it have the same properties as Collection class
        
    @property
    @lru_cache(maxsize=None) # Caches the result after the first call
    def mongo_client(self):
        """Lazily creates and returns the MongoClient instance."""
        if self.closing:
            delattr(self, 'mongo_client')
            return None
        lg.debug("MongoDB client is not initialized. Creating client...")
        mc = MongoClient(
            self.db_ip,
            username=self._mongo_admin_user,
            password=self._mongo_admin_password,
            authSource="admin"
        )
        lg.debug("MongoDB client created successfully.")
        return mc
    

    @property
    @lru_cache(maxsize=None) # Caches the result after the first call
    def mongo_db(self):
        """Lazily creates and returns the Database object using the client."""
        if self.closing:
            delattr(self, 'mongo_db')
            return None
        lg.debug("Establishing MongoDB database connection...")
        # This will automatically trigger the mongo_client property if needed
        conn = self.mongo_client[self.mongo_db_name]
        lg.debug("MongoDB connection established successfully.")
        return conn
    
    @property
    @lru_cache(maxsize=None) # Caches the result after the first call
    def meta_coll(self):
        """Lazily creates and returns the Meta collection using the client."""
        coll = self.mongo_db[self.meta_coll_name]
        lg.debug("Returned meta collection.")
        return coll

    @property
    @lru_cache(maxsize=None) # Caches the result after the first call
    def avail_ds(self) -> set:
        """Lazily creates and returns the set of available iris_db from meta coll using the mongo client."""
        lg.debug("Connecting to meta db to find avail ds")
        # This will automatically trigger the mongo_client property if needed
        try:
            avail_ds = {i['ds_id'] for i in self.meta_coll.find({}, {'_id': 0, 'ds_id': 1})}
        except Exception as e:
            lg.error(f"Error fetching available datasets: {e}")
            raise e
        avail_ds.add(self.meta_coll_name)
        # print(avail_ds)
        lg.debug(f"Fetched List of avail databases -> {avail_ds}")
        return avail_ds

    def get_avail_ds(self) -> set:
        """Get a set of available IRIS ds_id."""
        print(f"Avail Datasets: {self.avail_ds}")
        return self.avail_ds
    get_datasets = get_ds = list_ds = get_avail_ds = get_avail_ds
    
    # @property
    # @lru_cache(maxsize=None) # Caches the result after the first call
    # def coll(self):
    #     """Lazily creates and returns the collection object using the database."""
    #     if self.closing:
    #         delattr(self, 'coll')
    #         return None
    #     lg.debug("Establishing MongoDB collection connection...")
    #     # This will automatically trigger the mongo_db property if needed
    #     coll = self.mongo_db[self.ds_name]
    #     lg.debug("MongoDB collection connection established successfully.")
    #     return coll

    def find_ds(self, ds_id, avail_ds=None, acc=0.4, count=1) -> str|set:
        # 1. Create a mapping from lowercase name to original name.
        mapping = {db.lower(): db for db in (avail_ds or self.avail_ds)}
        # 2. Get the lowercase versions of all available DBs for matching.
        lower_avail_ds = list(mapping.keys())
        # 3. Perform the match on the lowercase versions.
        matches = get_close_matches(ds_id.lower(), lower_avail_ds, n=count, cutoff=acc)
        # 4. If a match is found, use the mapping to return the original name.
        if matches:
            if count > 1:
                res = {mapping[match] for match in matches}
            else:
                res = mapping[matches[0]]
            msg = f"Found matches for {ds_id}: {res}"
            print(msg)
            lg.debug(msg)
            return res
        return None

    # @lru_cache(maxsize=None) # Caches the result after the first call
    def set_meta_primary(self):
        """Get the meta collection"""
        lg.debug("Accessing meta collection...")
        self.ds_id = self.meta_coll_name
        self.coll = self.meta_coll
        return self.meta_coll
    meta_connect = set_meta_primary

    # def meta_connect(self):
    #     """Connect to the meta collection"""
    #     lg.info("Connecting to meta collection...")
    #     return self.meta_coll

    def connect(self, ds_id, acc=0.4) -> Collection:
        """Will connect to the database into a given collection
        It sets the self.ds_id attrib and self.coll
        """
        # if meta is tring to be connected then return the meta collection
        # if ds_id == self.meta_coll_name:
        #     self.ds_id = self.meta_coll_name
        #     # self.coll = self.get_meta_coll()
        #     lg.info(f"Connecting to {ds_id} Collection")
        #     return self.meta_coll
        
        avail_ds = self.avail_ds
        if (closest_match := self.find_ds(ds_id=ds_id, avail_ds=avail_ds, acc=acc)):
            self.ds_id = closest_match
            lg.info(f"Connecting to {self.ds_id} Collection")
            # self.ds_prefix=Path(DS_PREFIX)
            # self.ds_path=self.ds_prefix/self.ds_id
        else:
            lg.error(f"{ds_id} Collection Not found in available datasets. List: {avail_ds}")
            return None
        self.coll = self.mongo_db[self.ds_id]
        return self.coll
    get_coll = connect

    # def determine_coll(self,collection=None):
    #     if collection is None:
    #         if self.ds_id == self.meta_coll_name:
    #             collection = self.meta_coll
    #         else:
    #             collection = self.coll
    #     return collection
        
    def update(self, doc, key = None, coll=None):
        """Update a single document in the connected collection"""
        if '_id' in doc:
            key = '_id'
        if key is None:
            key = 'ds_id'
        if coll is None:
            coll = self.coll
        res=coll.update_one({key: doc[key]}, {'$set': doc}, upsert=False)
        lg.info(f"Updated document in {self.ds_id} collection.")
        return res
    
    def insert(self, docs, coll = None):
        if coll is None:
            coll = self.coll
        try:
            if isinstance(docs, dict):
                res = coll.insert_one(docs)
            elif isinstance(docs, list):
                # ignore if duplicate key error
                res = coll.insert_many(docs, ordered=False)
        except Exception as e:
            lg.error(f"Error inserting document into {self.ds_id} collection: {e}")
            res = None
        lg.info(f"Inserted document(s) into {self.ds_id} collection.")
        return res

    ## feature to get a Mongo Collection by getitem on iris_db object
    def __getitem__(self, coll_name):
        """Get a MongoDB collection by name"""
        # if not hasattr(self, 'mongo_conn'):
            # lg.error("No MongoDB connection established.")
            # return None
        return self.get_coll(coll_name)

    # def find(self, query, proj=None, collection=None):
    #     """Get data from the connected collection"""
    #     if collection is None:
    #         if not hasattr(self, 'coll'):
    #             lg.error("No collection connected. Please call connect() first.")
    #             return None
    #         collection = self.coll

    #     return collection.find(query, proj)

    # def update_one(self, doc, key=None, collection=None):
    #     """Update a single document in the connected collection"""
    #     if not hasattr(self, 'coll'):
    #         lg.error("No collection connected. Please call connect() first.")
    #         return None
    #     if collection is None:
    #         collection = self.coll
    #     if key is None:
    #         if '_id' in doc:
    #             key = '_id'
    #         elif self.ds_name=='meta' and 'ds_id' in doc:
    #             key = 'ds_id'
    #         else:
    #             raise ValueError("No valid key found for document.")
    #     if key in doc:
    #         collection.update_one({key: doc[key]}, {'$set': doc}, upsert=True)
    #     else:
    #         raise ValueError(f"Key '{key}' not found in document.")
    #     lg.info(f"Updated document in {self.ds_name} collection.")
    #     return True
    
    # def update_many(self, docs, key=None):
    #     """Update data in the connected collection"""
    #     if not hasattr(self, 'coll'):
    #         lg.error("No collection connected. Please call connect() first.")
    #         return None
    #     if isinstance(docs, dict):
    #         docs = [docs]
    #     for doc in docs:
    #         if key is None:
    #             if '_id' in doc:
    #                 key = '_id'
    #             elif self.ds_name=='meta' and 'ds_id' in doc:
    #                 key = 'ds_id'
    #             else:
    #                 raise ValueError("No valid key found for document.")
    #         if key in doc:
    #             self.coll.update_one({key: doc[key]}, {'$set': doc}, upsert=True)
    #         else:
    #             raise ValueError(f"Key '{key}' not found in document.")
    #     lg.info(f"Updated {len(docs)} documents in {self.ds_name} collection.")
    #     return True



    # def insert_one(self, doc):
    #     """Insert a single document into the connected collection"""
    #     if not hasattr(self, 'coll'):
    #         lg.error("No collection connected. Please call connect() first.")
    #         return None
    #     self.coll.insert_one(doc)
    #     lg.info(f"Inserted document into {self.ds_name} collection.")
    #     return True
    def __enter__(self):
        """Called when entering the 'with' statement."""
        lg.debug("Entering context...")
        return self # Return the instance to be used in the 'with' block
    
    def __exit__(self,*args):
        """Called when exiting the 'with' statement."""
        # This method is always called, ensuring the connection is closed.
        self.close()
        # lg.debug("MongoClient connection closed.")

    def close(self):
        """Explicitly close the mongo client connection"""  
        if self.closing:
            return
        try:
            self.closing = True
            if hasattr(self, 'mongo_client'):
                lg.debug("checking for mongoclient")
                self.mongo_client.close()
                lg.info("MongoDB client connection closed successfully.")
            else:
                lg.debug("MongoDB client was not initialized; no connection to close.")
        except Exception as e:
            lg.error(f"Error closing MongoDB client connection: {e}")
    
    def __del__(self):
        try:
            if self.closing:
                return
            self.close()
            lg.debug("MongoDB connection closed successfully.")
        except Exception as e:
            lg.error(str(e))
         
class Iris:
    """
    The obj of this class is an Iris image and its associated metadata.
    The IrisDB class will return a list of Iris objects.
    Iris objects will have a feature to lazily load the image using opencv from the path only when called via iris_obj.load_image().
    Iris objects also have metadata attributes that can be accessed directly.
    Iris objects are created by the IrisDB class when querying the database.
    Iris objects can also be displayed to view contents.
    """
    def __init__(self):
        self.image_path = None
        self.md = {}

