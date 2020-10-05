import os

FOLDERPATH = "/home/hadoop/sgds/data/raw"

for filename in os.listdir(FOLDERPATH):
    filepath = os.path.join(FOLDERPATH, filename)
    if filename.startswith("202006"):
        print("Rename", filepath)
        os.rename(filepath, filepath.lower())
