import json
import pandas as pd

def create_json(infn: str = "glosses_from_sheets2.tsv", ofn: str = "gloss_dict1.json"):
    with open(infn, "r") as f:
        sheet = pd.read_csv(f, sep="\t", header = 0, na_values=None)
        
        print("n glosses: ", len(sheet))
        print()
        na_to_none = lambda x: None if pd.isna(x) or x == 'None' else x
        dict_entries = [{"gloss":sheet.gloss[rn], "type":na_to_none(sheet.type[rn]), "class":na_to_none(sheet.class_[rn]), 
                          "status":na_to_none(sheet.ok[rn]), "change_to":na_to_none(sheet.change_to[rn]), 
                          "explanation": na_to_none(sheet.explanation[rn])} for rn in range(len(sheet))]
        dict_entries = {"glosses": dict_entries}
        
        with open(ofn, 'w') as f:
            json.dump(dict_entries, f, indent=2)

def create_csv(infn: str = "gloss_dict1.json", ofn: str = "glosses_in_use.csv"):
    with open(infn, "r") as f:
        d = json.load(f)
    with open(ofn, 'w') as f:
        f.writelines([";".join([gl["gloss"], gl["type"], str(gl["class"])]) + "\n" for gl in d["glosses"]])

create_json()