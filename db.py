#%%
from pathlib import Path

base = Path(r"iris_datasets\MMUIrisDatabase\MMU2 Iris Database")

#%%
l=list(Path(base).rglob("*.bmp"))
len(l)
#%%
import random

random.sample(l, 5)


#%%
from collections import Counter
Counter([i.stem[:-4] for i in l])

## Person 50 has only 5 images (left eye) instead of 10
#%%
p={int(i.stem[:-4]) for i in l}
set(range(1,101)) - p