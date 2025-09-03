#%% Imports
from biolab import *
from datetime import datetime
from pathlib import Path

DB_BASE_ = Path("~/datasets/iris_db/").expanduser()
DS_ID = "CASIA_v1"
DS_NAME_ = Path(DS_ID)
DS_BASE_ = DB_BASE_ / DS_NAME_
fm.ensure_exists(DS_BASE_)

ORIG_DB_BASE_ = Path("~/datasets/iris_datasets/CASIA/V1/CASIA-IrisV1/CASIA-IrisV1/CASIA Iris Image Database (version 1.0)").expanduser()

#%% METADATA Dict


# insert into db
metadata_casia_v1 = {
    "ds_id": DS_ID,
    "name": "CASIA-IrisV1",
    "db_info": {
        "desc": """CASIA Iris Image Database Version 1.0 (CASIA-IrisV1) includes 756 iris images
        from 108 eyes. For each eye, 7 images are captured in two sessions with our
        self-developed device CASIA close-up iris camera (Fig.1), where three samples are
        collected in the first session (Fig.2(a)) and four in the second session (Fig.2(b)). All
        images are stored as BMP format with resolution 320*280
        In order to protect our IPR in the design of our iris camera (especially the NIR
        illumination scheme), the pupil regions of all iris images in CASIA-IrisV1 were
        automatically detected and replaced with a circular region of constant intensity to
        mask out the specular reflections from the NIR illuminators. Such editing
        clearly makes iris boundary detection much easier but has minimal or no effects on
        other components of an iris recognition system, such as feature extraction and
        classifier design.
        It is suggested that you compare two samples from the same eye taken in different
        sessions when you want to compute the within-class variability. For example, the iris
        images in the first session can be employed as training dataset and those from the
        second session are used for testing.
        """,
        "capture_device": "CASIA close-up iris camera",
        "environment": "Indoor, controlled lighting",
        "type":"NIR",
        "notes":"",
        "periocular":False
    },
    # "db_specs":{
    "num_images": 756,
    "num_people": 108,
    "num_eyes": 108,
    "num_eyes_per_person": 1,
    "num_samples_per_eye": 7,
    "num_sessions": 2,
    # },
    'img_specs':{
        "ext": ".bmp",
        "res": "320x280",
        "width": 320,
        "height": 280
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
    db.insert(metadata_casia_v1)
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

#%% Insert individual data to CASIA-v1 coll

images = list(ORIG_DB_BASE_.rglob("*.bmp"))

docs=[]
# with IrisDB() as db:
ds_id = db.find_ds('casia-v1')
db = IrisDB(ds_id=ds_id)
meta_doc = db.meta_coll.find_one({'ds_id': ds_id})
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
eye = "L"

for img_ in images:
    person_id,session_id,sample_id = map(int, img_.stem.split("_"))
    eye_id = f"{person_id}_{eye}"
    ext = img_.suffix
    # renaming the 2nd session images into continuous id_s
    if session_id == 2:
        sample_id = 3+sample_id
    person_sample_id=sample_id # in this case
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
        'eye_id': eye_id,  # only one eye per person in this dataset
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
    # docs.append(doc)
    db.insert(doc)
    fm.copy_file(img_, path_)

# db.insert(docs)
db.close()

