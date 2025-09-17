// Command to view all metadata excluding large fields
db.getCollection("meta").find(
  {},
  {
    "orig": 0,
    "mask_irisseg": 0,
    "norm_def.stats.num_eyes_per_person": 0,
    "norm_def.stats.num_samples_per_eye": 0,
  }
);
