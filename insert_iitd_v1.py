#%% Imports
from biolab import *
from datetime import datetime
from pathlib import Path
from pymongo import ASCENDING

DB = Path("~/datasets/").expanduser()
DB_BASE_ = DB / Path("iris_db/")
DS_ID = "IITD_v1"
DS_NAME_ = Path(DS_ID)
DS_BASE_ = DB_BASE_ / DS_NAME_
# from lib import FileManager as FM
# fm = FM()

fm.ensure_exists(DS_BASE_)

#%%
BASE_ = DB / "iris_datasets/"
ORIG_DB_BASE_ = BASE_ / Path("IITD/IITD V1/IITD Database")

#%% METADATA Dict

# insert into db
metadata_iitd_v1 = {
    "ds_id": DS_ID,
    "name": "IITD.v1",
    "db_info": {
        "desc": """

        Description of the IITD Iris Image Database version 1.0
=======================================================

This iris image database mainly consists of the iris images collected from the students and staff at IIT Delhi, India. This database has been acquired in
the Biometrics Research Laboratory during January - July 2007 using JIRIS, JPC1000, digital CMOS camera. The acquired images were saved in bitmap format.
The database of 2240 images is acquired from 224 different users and made available freely to the researchers. All the subjects in the database are in the
age group 14-55 years comprising of 176 males and 48 females. The resolution of these images is 320 x 240 pixels and all these images were acquired in the
indoor environment. All the images in the database were acquired from the volunteers who were not paid or provided any honorarium. The images were acquired
using an automated program that requires users to present their eyes in a sequence until ten images are registered.

Organization of Database
========================
The acquired database is saved in 224 folders, each corresponding to 224 subjects. Majority of images were acquired from the left eyes while the rest images
were acquired from right eye. Now the database has a label 'L' or 'R' which designates left or right eye. There are 1288 images from 224 subject that are from
left eyes while the rest images from 211 subjects are from right eyes.Except folders 1-13, 27, 55 and 65 all other folders have five left and 5 right eye 
images. (**appended on 20-04-2016**).

Usage of Database
========================
This database is only available for research and noncommercial purposes. Commercial distribution or any act related to commercial use of this database is strictly
prohibited. Kindly acknowledge all the publicly available publications/work employing this database with the following acknowledgment:

"Portions of the work tested on the IITD Iris Database version 1.0"
A citation to "IIT Delhi Iris Database version 1.0, http://web.iitd.ac.in/~biometrics/Database_Iris.htm

Related Publication:
====================
Ajay Kumar and Arun Passi, "Comparison and combination of iris matchers for reliable personal identification," Proc. CVPR 2008, Anchorage, Alaska, pp. 21-27 Jun. 2008

Contact Information:
====================
Ajay Kumar
Biometrics Research Laboratory
Indian Institute of Technology Delhi
New Delhi, India
E-mail: ajaykr@ieee.org

        IITD Iris Database Version 1.0
The IIT Delhi IrisDatabase (Version 1.0) is publicly available for academic use only. While every effort has been made to ensure accuracy of this database, we cannot accept responsibility for errors or omissions. The academic use of this database is free of charge. Any commercial distribution or act related to the commercial usage of this database is strictly prohibited. The distribution of this database to any parties that have not read and agreed to the terms and conditions of usage is strictly prohibited. Neither IIT Delhi, nor any third parties who may provide information to us for the dissemination purpose, shall have any responsibility for or be liable in respect of the content or the accuracy of the provided information, or for any errors or omissions therein. The IIT Delhi reserves the right to revise, amend, alter or delete the information provided herein at any time, but shall not be responsible for or liable in respect of any such revisions, amendments, alterations or deletions. Any publication using this database must reference to this website: http://web.iitd.ac.in/~biometrics/Database_Iris.htm, and this paper: A. Kumar and A. Passi, Comparison and combination of iris matchers for reliable personal identification, Proc. CVPR, Anchorage, pp. 21-27, Jun. 2008. The images available in this database can only be published or presented in research papers or at research conferences and cannot be used for any commercial purpose.
        
        """,
        "capture_device": "JIRIS, JPC1000, digital CMOS camera",
        "environment": "indoor",
        "type": "NIR",
        "notes": "",
        "periocular": False,
    },
    # "db_specs":{
    "num_images": 2240,
    "num_people": 224,
    "num_eyes": 435,
    "num_eyes_per_person": 2,
    "num_samples_per_eye": 10,
    "num_sessions": 1,
    # },
    'img_specs':{
        "ext": ".bmp",
        "res": "320x240",
        "width": 320,
        "height": 240
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
    db.update(metadata_iitd_v1)
    # db.insert(metadata_casia_iris_thousand)
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

images = sorted(list(ORIG_DB_BASE_.rglob(f"*{metadata_iitd_v1['img_specs']['ext']}")))
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
ext = metadata_iitd_v1['img_specs']['ext']
db.close()
#%%
db = IrisDB(ds_id=ds_id)
for img_ in images:
    # name = YYY/NN_E.bmp
    #             
    img_stem = img_.stem.split('_')  # 
    person_id, sample_id, eye = int(img_.parent.name), int(img_stem[0]), img_stem[1]
    # if person_id == 0:
        # person_id = 1000
    eye_id = f"{person_id}_{eye}"
    
    # renaming the 2nd session images into continuous id_s
    # if session_id == 2:
    #     sample_id = 3+sample_id
    # if sample_id==0:
        # sample_id = 10
    person_sample_id = sample_id
    if eye == 'R':
        sample_id -= 5

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
