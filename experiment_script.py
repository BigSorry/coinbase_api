import util

def getIds():
    ids_path = "data/ids.pkl"
    ids_list = util.readPickle(ids_path)
    return ids_list

ids_saved = getIds()
for id in ids_saved:
    print(id)
    print()