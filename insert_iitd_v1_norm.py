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
ORIG_DB_BASE_ = BASE_ / Path("IITD/IITD V1/Normalized_Images")

#%% METADATA Dict


# with IrisDB(DS_ID) as db:
# # number of eyes calc
#     num_eyes = len(db.find({}).distinct('eye_id'))
#     print(f"Number of eyes in {DS_ID}: {num_eyes}")
#     num_samples_per_eye = {}
#     for eye in db.find({}).distinct('eye_id'):
#         n = len(list(db.find({'eye_id': eye})))
#         try:
#             num_samples_per_eye[n].append(eye)
#         except KeyError:
#             num_samples_per_eye[n] = [eye]
#     num_samples_per_eye_count = {k: len(v) for k, v in num_samples_per_eye.items()}
#     num_eyes_per_person = {}
#     for person in db.find({}).distinct('person_id'):
#         n = len(list(db.find({'person_id': person}).distinct('eye_id')))
#         try:
#             num_eyes_per_person[n].append(person)
#         except KeyError:
#             num_eyes_per_person[n] = [person]
#     num_eyes_per_person_count = {k: len(v) for k, v in num_eyes_per_person.items()}
#     print(f"Number of people in {DS_ID}: {len(db.find({}).distinct('person_id'))}")
#     print(f"Number of eyes per person in {DS_ID}: {num_eyes_per_person}")
#     print(f"Number of eyes per person count in {DS_ID}: {num_eyes_per_person_count}")

#     print(f"Number of samples per eye in {DS_ID}: {num_samples_per_eye}")
#     print(f"Number of samples per eye count in {DS_ID}: {num_samples_per_eye_count}")
    # eye_id_count = {}
    # for eye in db.find({}).distinct('eye_id'):
    #     eye_id_count[eye] = db.coll.count_documents({'eye_id': eye})
    # count_eye_id_list = {}
    # for k, v in eye_id_count.items():
    #     try:
    #         count_eye_id_list[v].append(k)
    #     except KeyError:
    #         count_eye_id_list[v] = [k]
    # # print(f"Number of unique eyes in {DS_ID}: {len(eye_id_count)}")
    # print(f"Number of eyes with count of samples in {DS_ID}: {count_eye_id_list}")
    # print(f"Number of eyes counted from samples in {DS_ID}: {eye_id_count}")
#%%
# insert into db
db = IrisDB(DS_ID)
meta = IrisMeta()
#%%
meta_tag = "norm_def"
metadata_iitd_v1 = {
    "ds_id": DS_ID,
    "db_info": {
        "name": "IITD.v1",
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
    "tags": ["orig", meta_tag],
    # "db_specs":{
    "orig": db.get_tag_data(),
    meta_tag: {
        "name": "Normalized images default using Daugman's rubber sheet model",
        'orig_base_': str(ORIG_DB_BASE_),
    },
    'injested_at': datetime.now()
}
meta.update(metadata_iitd_v1)  # delete if exists
# meta.insert()

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

images = sorted(list(ORIG_DB_BASE_.rglob(f"*.bmp")))
print(len(images))
#%%
docs=[]
# with IrisDB() as db:
ds_id = db.find_ds(DS_ID)
db = IrisDB(ds_id=ds_id)
meta = IrisMeta()
#%%
meta_doc = meta.get_metadata(ds_id)
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
folder_tags = metadata_iitd_v1['tags']
session_id = 1
db.close()
#%%
from PIL import Image
db = IrisDB(ds_id=ds_id)
for img_ in images:
    ext = img_.suffix
    # name = P_S.bmp
    #             
    person_id, sample_id = img_.stem.split('_')  # 
    eye = "L" # default left eye
    # if person_id == 0:
        # person_id = 1000
    eye_id = f"{person_id}_{eye}"
    
    # renaming the 2nd session images into continuous id_s
    # if session_id == 2:
    #     sample_id = 3+sample_id
    # if sample_id==0:
        # sample_id = 10
    person_sample_id = sample_id
    # if eye == 'R':
    #     sample_id -= 5

    new_filename_ = Path(f"{eye_id}_{sample_id}{ext}")
    # status = 'orig'

    rel_path_ = eye_id / new_filename_
    # orig_path_ = 'orig'/rel_path_
    # full_orig_path = DS_BASE_ / orig_path_
    # norm_path_ = 'norm'/rel_path_
    # full_norm_path_ = DS_BASE_ / norm_path_
    # seg_path_ = 'seg'/rel_path_
    # full_seg_path_ = DS_BASE_ / seg_path_
    path_ = base_ / meta_tag / rel_path_

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
    img=Image.open(img_)
    w,h = img.size
    img_specs = {
        'ext': ext,
        # get image width from image properties
        'width': w,
        'height': h,
        'res':f'{w}x{h}'
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
        'folder_tags': folder_tags,
        'img_specs': img_specs,
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
