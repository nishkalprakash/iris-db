meta = {
    "_id" : "68c9059edd89bc27175b6098",
    "ds_id" : "IITD_v1",
    "name" : "IITD.v1",
    "doc" : "https://docs.google.com/document/d/1N_ninklrJYcEyG0EUuSx_KmBeHvnOYrtBTEw1yMZXNM/edit?tab=t.0",
    "img_tags" : [
        "orig",
        "norm_def",
        "mask_irisseg"
    ],
    "orig" : {
        "info" : "Original images",
        "old_base_" : "~/datasets/iris_datasets/IITD/IITD V1/IITD Database",
        "img_specs" : {
            "ext" : ".bmp",
            "width" : 320,
            "height" : 240
        }
    },
    "norm_def" : {
        "info" : "Normalized images default using Daugman's rubber sheet model",
        "old_base_" : "~/datasets/iris_datasets/IITD/IITD V1/Normalized_Images",
        "img_specs" : {
            "ext" : ".bmp",
            "width" : 432,
            "height" : 48
        }
    },
    "mask_irisseg" : {
        "info" : "Iris segmentation masks from IRISSEG-EP dataset",
        "old_base_" : "~/datasets/iris_datasets/SEGMENTATION_GROUND_TRUTHS/IRISSEG-EP/IRISSEG-EP-Masks-r1/IRISSEG-EP-Masks/masks/iitd",
        "img_specs" : {
            "ext" : ".tiff",
            "width" : 320,
            "height" : 240
        }
    }
}

meta_long={
    "_id" : "68c9059edd89bc27175b6098",
    "ds_id" : "IITD_v1",
    "name" : "IITD.v1",
    "doc" : "https://docs.google.com/document/d/1N_ninklrJYcEyG0EUuSx_KmBeHvnOYrtBTEw1yMZXNM/edit?tab=t.0",
    "img_tags" : [
        "orig",
        "norm_def",
        "mask_irisseg"
    ],
    "orig" : {
        "info" : "Original images",
        "old_base_" : "~/datasets/iris_datasets/IITD/IITD V1/IITD Database",
        "img_specs" : {
            "ext" : ".bmp",
            "width" : 320,
            "height" : 240
        },
        "stats" : {
            "num_images" : 2240,
            "num_people" : 224,
            "num_eyes" : 435,
            "num_eyes_per_person_count" : {
                "1" : 13,
                "2" : 211
            },
            "num_samples_per_eye_count" : {
                "5" : 416,
                "10" : 13,
                "6" : 3,
                "4" : 3
            },
            "num_sessions" : [
                "1"
            ]
        }
    },
    "norm_def" : {
        "info" : "Normalized images default using Daugman's rubber sheet model",
        "old_base_" : "~/datasets/iris_datasets/IITD/IITD V1/Normalized_Images",
        "img_specs" : {
            "ext" : ".bmp",
            "width" : 432,
            "height" : 48
        },
        "stats" : {
            "num_images" : 1120,
            "num_people" : 224,
            "num_eyes" : 224,
            "num_eyes_per_person_count" : {
                "1" : 224
            },
            "num_samples_per_eye_count" : {
                "5" : 224
            },
            "num_sessions" : [
                "1"
            ]
        }
    },
    "mask_irisseg" : {
        "info" : "Iris segmentation masks from IRISSEG-EP dataset",
        "old_base_" : "~/datasets/iris_datasets/SEGMENTATION_GROUND_TRUTHS/IRISSEG-EP/IRISSEG-EP-Masks-r1/IRISSEG-EP-Masks/masks/iitd",
        "img_specs" : {
            "ext" : ".tiff",
            "width" : 320,
            "height" : 240
        },
        "stats" : {
            "num_images" : 2240,
            "num_people" : 224,
            "num_eyes" : 435,
            "num_eyes_per_person_count" : {
                "1" : 13,
                "2" : 211
            },
            "num_samples_per_eye_count" : {
                "5" : 416,
                "10" : 13,
                "6" : 3,
                "4" : 3
            },
            "num_sessions" : [
                "1"
            ]
        }
    }
}