{
  "_id": "CASIA-V4-Thousand", # A unique, human-readable identifier
  "name": "CASIA Iris Image Database (Version 4.0) - Thousand",
  "description": "Contains 20,000 iris images from 1,000 subjects, captured with a self-developed iris camera. Known for variations in gaze and lighting.",
  "source_path": "/mnt/data/iris_raw/CASIA-Iris-Thousand/",
  "statistics": {
    "subjects": 1000,
    "images": 20000,
    "sessions_per_subject": 2,
    "image_format": "jpg",
    "resolution": {
      "width": 640,
      "height": 480
    }
  },
  "properties": {
    "capture_device": "CASIA close-up iris camera",
    "environment": "Indoor, controlled lighting"
  },
  "ingested_at": ISODate("2025-08-27T12:30:00Z") # Timestamp of when this entry was created
}

{
  "_id": ObjectId("64e8b3f1a9d8c7b4e6f1a2b3"), # Default unique MongoDB ObjectId
  "db_ref": "CASIA-V4-Thousand", # Foreign key linking to the 'databases' collection
  "subject_id": "1024", # The identifier for the person
  "eye": "L", # 'L' for Left, 'R' for Right
  "instance_id": "S51024L03", # Unique identifier for this specific capture/image
  "file_info": {
    "absolute_path": "/mnt/data/iris_raw/CASIA-Iris-Thousand/1024/L/S51024L03.jpg",
    "original_filename": "S51024L03.jpg"
  },
  "processing": {
    "status": "features_extracted", # e.g., "ingested", "features_extracted", "failed_quality_check"
    "extracted_at": ISODate("2025-08-28T09:15:00Z"),
    "feature_extractor_version": "osiris_v4.1"
  },
  "features": {
    "template": BinData(0, "...binary data of the iris code..."), # Use MongoDB's Binary type
    "mask": BinData(0, "...binary data of the noise mask...")
  },
  "quality_metrics": {
    "focus_score": 0.89,
    "occlusion_percent": 5.2,
    "pupil_to_iris_ratio": 0.45
  }
}