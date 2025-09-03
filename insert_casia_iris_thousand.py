#%% Imports
from biolab import *
from datetime import datetime
from pathlib import Path
from pymongo import ASCENDING

DB_BASE_ = Path("~/datasets/iris_db/").expanduser()
DS_ID = "CASIA_iris_thousand"
DS_NAME_ = Path(DS_ID)
DS_BASE_ = DB_BASE_ / DS_NAME_
# from lib import FileManager as FM
# fm = FM()

fm.ensure_exists(DS_BASE_)

#%%
ORIG_DB_BASE_ = Path("~/datasets/iris_datasets/CASIA/V4/CASIA-IrisV4(JPG)/CASIA-Iris-Thousand").expanduser()

#%% METADATA Dict

# insert into db
metadata_casia_iris_thousand = {
    "ds_id": DS_ID,
    "name": "CASIA-Iris-Thousand",
    "db_info": {
        "desc": """CASIA-Iris-Thousand is a large iris image database collected by the Chinese Academy of Sciences' Institute of Automation (CASIA) in 2010. It contains 20,000 images from 1,000 subjects, with each subject contributing 20 images. The images were captured using a high-quality iris camera under controlled conditions, ensuring good image quality for research purposes. The database is designed to support research in iris recognition and related fields, providing a diverse set of images that include variations in lighting, angle, and occlusions. CASIA-Iris-Thousand is widely used in the biometrics research community for developing and testing iris recognition algorithms.
        CASIA-Iris-Thousand contains 20,000 iris images from 1,000 subjects, which
        were collected using IKEMB-100 camera (Fig. 8) produced by IrisKing
        (Http://www.irisking.com). IKEMB-100 is a dual-eye iris camera with friendly visual
        feedback, realizing the effect of “What You See Is What You Get”. The bounding
        boxes shown in the frontal LCD help users adjust their pose for high-quality iris
        image acquisition. The main sources of intra-class variations in CASIA-Iris-Thousand
        are eyeglasses and specular reflections. Since CASIA-Iris-Thousand is the first
        publicly available iris dataset with one thousand subjects, it is well-suited for studying
        the uniqueness of iris features and develop novel iris classification and indexing methods.
        The images of CASIA-Iris-Thousand are stored as:
        $root path$/CASIA-Iris-Thousand/YYY/E/S5YYYENN.jpg
        YYY: the unique identifier of the subject in the subset
        E: 'L' denotes left eye and 'R' denotes right eye
        NN: the index of the image in the class
        """,
        "capture_device": "Irisking IKEMB-100 iris camera",
        "environment": "Indoor with lamp on/off",
        "type": "NIR",
        "notes": "",
        "periocular": False
    },
    # "db_specs":{
    "num_images": 20000,
    "num_people": 1000,
    "num_eyes": 2000,
    "num_eyes_per_person": 2,
    "num_samples_per_eye": 10,
    "num_sessions": 1,
    # },
    'img_specs':{
        "ext": ".jpg",
        "res": "640x480",
        "width": 640,
        "height": 480
    },
    'paths':{
        'orig_base_': str(ORIG_DB_BASE_),
        'base_': str(DS_BASE_),
    },
    'injested_at': datetime.now()
}

#%% Insert METADATA to mongodb

# from datetime import datetime
with IrisDB() as db:
    # Use the db object to interact with the database
    meta=db['meta']
    print(db.list_ds())
    # db.update(metadata_casia_v1)
    db.insert(metadata_casia_iris_thousand)
    # print(db.update({
    #     'ds_id': db.find_ds('casia-v1'),
    #     'injested_at': datetime.now()
    # }))
    # meta.update_one({'ds_id': db.find_ds('casia-v1')},
    #     {
    #         '$set':{
    #         'ds_id': db.find_ds('casia-v1'),
    #         'injested_at': datetime.now()
    #         }
    #     }
    # )
    # print(db.find_ds('cas'))
    meta.create_index([('ds_id', 1)], unique=True)
    # db.update_data(
    #     [{
    #         "ds_id": db.find_ds('casia-v1'),
    #         'orig_base': ORIG_DB_BASE_.as_posix(),
    #         'base': DS_BASE_.as_posix()
    #     }],
    #     key='ds_id'
    # )
    # print(db.mongo_conn[db.meta_coll].find({}, {'_id': 0, 'ds_id': 1}))
    
    pass
#%%
with IrisDB() as db:   
    print(db.list_ds())
#%% Insert individual data to CASIA-v1 coll

images = sorted(list(ORIG_DB_BASE_.rglob(f"*{metadata_casia_iris_thousand['img_specs']['ext']}")))
print(len(images))
#%%
docs=[]
# with IrisDB() as db:
ds_id = db.find_ds(DS_ID)
db = IrisDB(ds_id=ds_id)
meta_doc = db.meta_coll.find_one({'ds_id': ds_id})
#%%
try:
    db.coll.create_index([("image_id", ASCENDING)], unique=True)
    # create a 
    db.coll.create_index([("person_id",ASCENDING)])
    db.coll.create_index([("eye_id",ASCENDING)])
    # folder_tags can be one of 'orig','norm','segm'
    db.coll.create_index([("folder_tags",ASCENDING)])
    # db.coll.create_index([("status",ASCENDING)])
    # index path as well
    db.coll.create_index([("paths.rel_path_",ASCENDING)],unique=True)
    db.coll.create_index([("paths.path_",ASCENDING)],unique=True)
    # db.coll.create_index([("paths.common_path_",ASCENDING)],unique=True)
    # db.coll.create_index([("paths.full_path_",ASCENDING)],unique=True)
    # db.coll.create_index([("person_id",ASCENDING),("person_sample_id",ASCENDING)],unique=True)
    # db.coll.create_index([("eye_id",ASCENDING),("sample_id",ASCENDING)],unique=True)
except Exception as e:
    lg.error(f"Error creating indexes for {ds_id} collection: {e}")

base_ = DS_BASE_
folder_tags = ['orig']
session_id = 1
ext = metadata_casia_iris_thousand['img_specs']['ext']
db.close()
#%%
db = IrisDB(ds_id=ds_id)
for img_ in images:
    # name = YYY/E/S5YYYENN.jpg
    #              01234567
    img_stem = img_.stem  # S5YYYENN
    person_id,eye,sample_id = int(img_stem[2:5]),img_stem[5],int(img_stem[6:])
    if person_id == 0:
        person_id = 1000
    eye_id = f"{person_id}_{eye}"
    
    # renaming the 2nd session images into continuous id_s
    # if session_id == 2:
    #     sample_id = 3+sample_id
    if sample_id==0:
        sample_id = 10
    person_sample_id = sample_id
    if eye == 'R':
        person_sample_id += 10

    new_filename_ = Path(f"{eye_id}_{sample_id}{ext}")
    # status = 'orig'

    rel_path_ = eye_id / new_filename_
    # orig_path_ = 'orig'/rel_path_
    # full_orig_path = DS_BASE_ / orig_path_
    # norm_path_ = 'norm'/rel_path_
    # full_norm_path_ = DS_BASE_ / norm_path_
    # seg_path_ = 'seg'/rel_path_
    # full_seg_path_ = DS_BASE_ / seg_path_
    path_ = base_ / 'orig' / rel_path_

    paths = {
        'base_': str(base_),
        'rel_path_': str(rel_path_),
        'path_': str(path_),
        # 'orig_path_': str(orig_path_),
        # 'full_orig_path_': str(full_orig_path),
        # 'norm_path_': str(norm_path_),
        # 'full_norm_path_': str(full_norm_path_),
        # 'seg_path_': str(seg_path_),
        # 'full_seg_path_': str(full_seg_path_)
    }
    
    orig_paths = {
        'base_': str(ORIG_DB_BASE_),
        'rel_path_': str(img_.relative_to(ORIG_DB_BASE_)),
        'path_': str(img_),
    }
    
    doc = {
        'ds_id': ds_id,
        'person_id': str(person_id),
        'eye_id': eye_id, 
        'sample_id': str(sample_id),
        'person_sample_id': str(person_sample_id),
        'image_id': str(new_filename_.stem),
        'file_name': str(new_filename_),
        'session_id': str(session_id),
        # 'status': status,
        'eye': eye,
        'img_specs': meta_doc['img_specs'],
        'paths': paths,
        'orig_paths': orig_paths,
        'injested_at': datetime.now()
    }
    docs.append(doc)
    # db.insert(doc)
    fm.copy_file(img_, path_)

db.insert(docs)
db.close()


# %%
