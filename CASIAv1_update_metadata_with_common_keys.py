# %% Imports
from datetime import datetime
from biolab import *
from pathlib import Path

DB = Path("~/datasets/").expanduser()
DB_BASE_ = DB / Path("iris_db/")
DS_ID = "CASIA_v1"
DS_NAME_ = Path(DS_ID)
DS_BASE_ = DB_BASE_ / DS_NAME_

lg = LM.get_logger(__name__, level="INFO")
# %% db init
# insert into db
db = IrisDB(DS_ID)
meta = IrisMeta()
# %% meta doc
meta_doc = meta.get_metadata(DS_ID)
# %%
# db.list_ds()
# %%
# img_doc = db.find_one({})
# img_doc['img_specs'].pop('res', None)
# for tag in meta_doc['img_tags']:
#     try:
#         img_doc[tag] = {}
#         img_doc[tag]['img_specs'] = img_doc.pop('img_specs')
#     except KeyError as e:
#         print(f"KeyError for tag {tag}: {e}")
# img_doc['orig']['img_tags'] = meta_doc['img_tags']
# img_doc = next(images)
# old_base_ = Path(meta_doc["orig"]["old_base_"]).expanduser()
# old_images_list = sorted(list(old_base_.rglob("*.bmp")))
# %%
# unset all injested at fields in meta.coll
# meta.coll.update_many({}, {'$unset': {'mask_irisseg.injested_at': ""}})
# %% unset injested_at fields in db.coll
# print(db.coll.update_many({}, {'$unset': {f'injested_at': ""}}))
# %% set meta image specs from img_doc
# img_doc['img_tags'] = meta_doc['img_tags']
# for tag in meta_doc['img_tags']:
#     try:
#         print(meta.coll.update_one({'ds_id': DS_ID}, {
#             '$set': {f'{tag}.img_specs': img_doc[tag]['img_specs']}}))
#     except KeyError as e:
#         print(f"KeyError for tag {tag}: {e}")
# %% # unset unneeded keys from meta docs
# for tag in ['norm_def', 'mask_irisseg']:
#     print(meta.coll.update_one({'ds_id': DS_ID}, {'$unset': {f'{tag}': ""}}))
# %% unset few fields in each doc in db.coll
# drop index
# db.coll.drop_index('paths.rel_path__1')
# db.coll.drop_index('paths.path__1')
# %%
# db.coll.drop_index('folder_tags_1')
# %% create index on img_tags
# db.coll.create_index('img_tags')
# %%
# db.coll.update_many({}, {'$set': {'img_tags': ['orig']}})
# for doc in list(db.coll.find({})):
#     # rename dic key
#     doc['sample_id_person'] = doc.pop('person_sample_id')  # rename person_sample_id to sample_id_person
#     # remove key file_name from doc
#     doc.pop('file_name')
#     doc.pop('img_specs')
#     doc.pop('paths')
#     orig_=doc.pop('orig_paths')
#     doc['orig'] = {
#         'old_rel_': orig_['rel_path_'],
#     }
#     # delete the doc first
#     db.coll.delete_one({'_id': doc['_id']})
#     # insert the modified doc
#     print(db.coll.insert_one(doc))
#     lg.info(f"Updated {doc['image_id']}")


# %% unset {tag}.injested_at fields in db.coll
# for tag in img_doc['img_tags']:
# print(db.coll.update_many({}, {'$unset': {f'{tag}.injested_at': ""}}))

# %% set {tag}.img_specs fields in meta.coll
# img_doc['orig'] = {"img_specs": {
# "ext": ".bmp",
# "width": 320,
# "height": 240
# }}
# for tag in meta_doc['img_tags']:
# try:
# print(meta.coll.update_one({'ds_id': DS_ID}, {
#   '$set': {f'{tag}.img_specs': img_doc[tag]['img_specs']}}))
# except KeyError as e:
# print(f"KeyError for tag {tag}: {e}")
# %% unset all {tag}.img_specs fields in db.coll
# for tag in img_doc['img_tags']:
# print(db.coll.update_many({}, {'$unset': {f'{tag}.img_specs': ""}}))
# %% set orig field in db.coll
# for img_ in old_images_list[1:]:
#     # name = YYY/NN_E.bmp
#     #
#     img_stem = img_.stem.split('_')  #
#     person_id, sample_id, eye = int(img_.parent.name), int(img_stem[0]), img_stem[1]
#     # if person_id == 0:
#         # person_id = 1000
#     eye_id = f"{person_id}_{eye}"
#     image_id = f"{eye_id}_{sample_id}"
#     doc = {
#         'image_id': image_id,
#         'orig':
#             {
#                 'old_rel_': str(img_.relative_to(old_base_)),
#                 'injested_at': datetime.now()
#             },
#     }
#     db.update(doc,key='image_id')
#     lg.info(f"Updated {image_id}")

# db.close()
# %% meta stats per tag
db.get_tag_data("orig")
# %%
# db.get_tag_data('orig')
for tag in meta_doc["img_tags"]:
    stats = db.get_tag_data(tag)
    # append to {tag}.stats
    print(meta.coll.update_one({"ds_id": DS_ID}, {"$set": {f"{tag}.stats": stats}}))
# %%
# export collection to jsonl
# db.export_jsonl(DB / 

