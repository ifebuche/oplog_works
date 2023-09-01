import pandas as pd

# def convert_bson_to_str():
#     for col in table.columns:
#         if type(table[col][0]) == ObjectId:
#             table[col] = [str(line) for line in table[col]]


def date_convert(x):
    try:
        if "Coordinated" in x:
            x = pd.to_datetime(x.replace("GMT+0000 (Coordinated Universal Time)", ""))
        else:
            x = pd.Timestamp(int(x) / 1000, unit="s")
    except Exception as e:
        print(e)

    return x
