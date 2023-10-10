from sqlalchemy import create_engine, text
from sqlalchemy import select, insert
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
import gspread
# from oauth2client.client import GoogleCredentials
import pandas as pd
import re
from oauth2client.service_account import ServiceAccountCredentials
from conf import DBNAME, USER, PASSWORD, HOST, DATAURL
import urllib
import pickle
# import psycopg2
import json
import time

engine = create_engine(f"postgresql://{USER}:{urllib.parse.quote_plus(PASSWORD)}@{HOST}/{DBNAME}")
con = engine.connect()

def get_table() -> pd.DataFrame:
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('pragmaticon.json', scope)
    gc = gspread.authorize(credentials)
    sheet_url = DATAURL
    wb = gc.open_by_url(sheet_url)
    sheet = wb.worksheet('main')
    df = pd.DataFrame(sheet.get_all_values())
    return df



def create_tables(): #creates the db structure and returns its classes
    def camelize_classname(base, tablename, table):
        no_spaces = str(tablename[0].upper() + re.sub(r'_([a-z])', lambda m: m.group(1).upper(), tablename[1:]))
        capitalize_after_digit = re.sub(r'([0-9])([a-z])', lambda m: m.group(1) + m.group(2).upper(), no_spaces)
        return capitalize_after_digit
    
    with open("create_new_structure.sql") as file:
            query = text(file.read())
            con.execute(query)
    con.commit()
    
    Base = automap_base()
    Base.prepare(autoload_with=engine, classname_for_table=camelize_classname)
    con.commit()
    return Base

base = create_tables()

Syntax, CxSyntax, CxSemantics, Examples, GlossTypes, InnerStructureSubtypes, InnerStructureTypes, Intonations, Languages, Lemmas, Pragmatics, Literature, Semantics, Speech_acts, Frames, GlossClass, Glosses, InnerStructure, SourceConstructions, Formulas, Frame2SpeechActs, Variations, Formula2InnerStructure, Frame2Var, Variation2Constituents, Glossing, Constituents = base.classes.Syntax, base.classes.CxSyntax, base.classes.CxSemantics, base.classes.Examples, base.classes.GlossTypes, base.classes.InnerStructureSubtypes, base.classes.InnerStructureTypes, base.classes.Intonations, base.classes.Languages, base.classes.Lemmas, base.classes.Pragmatics, base.classes.Literature, base.classes.Semantics, base.classes.SpeechActs, base.classes.Frames, base.classes.GlossClass, base.classes.Glosses, base.classes.InnerStructure, base.classes.SourceConstructions, base.classes.Formulas, base.classes.Frame2SpeechActs, base.classes.Variations, base.classes.Formula2InnerStructure, base.classes.Frame2Var, base.classes.Variation2Constituents, base.classes.Glossing, base.classes.Constituents

# # Pickled the dataframe not to send API requests all the time
# dat = get_table()
# with open('pickled_df', 'wb') as f:
#      pickle.dump(dat, f)

with open('pickled_df', 'rb') as f:
     dat: pd.DataFrame = pickle.load(f)

dat.columns = dat.iloc[0]
dat = dat.iloc[1:] #setting numeric indexing
# print(dat.columns.to_list())

row_count = len(dat.values)

def validate_row(row_n: int, dat: pd.DataFrame) -> None:
    if dat.df[row_n].strip() == '' or dat.variation[row_n].strip() == '' or dat.languages[row_n].strip() == '':
         raise Exception(f"row {str(row_n)} is invalid")

"""parsing a dataframe row
Three types of parcing: single value (stripped), multiple values (split by |), single value hierarchical (split by the first : - maxsplit parameter)
Special cases: glosses, examples
"""



def create_gloss_dict (dat: pd.DataFrame, old_dict: str = 'gloss_dict.json', new_dict: str = 'gloss_dict.json'):
    glosses = dat.glosses.unique()
    glosses = [re.split("=| |\.|-", gloss) for gloss in glosses]
    glosses_flat = set([item.strip().lower() for sublist in glosses for item in sublist if item.strip() != ""])
    
    # getting annotated glosses:
    with open(old_dict, 'r') as f:
         existing_dict = json.load(f)
    existing_glosses = [gloss["gloss"] for gloss in existing_dict["glosses"]]
    new_glosses_count = 0
    for gl in glosses_flat:
         if gl not in existing_glosses:
              new_glosses_count += 1
              existing_dict["glosses"].append({"gloss":gl, "type": "lexical", "class": None})
    if new_glosses_count != 0:
        with open(new_dict, 'w') as f:
            json.dump(existing_dict, f, indent=2)
    print(f"n_new_glosses: {str(new_glosses_count)}")

create_gloss_dict(dat)

def import_gloss_dict(fn: str = 'gloss_dict.json') -> dict:
    with open(fn, 'r') as f:
        gloss_dict = json.load(f)
    gloss_dict = gloss_dict["glosses"]
    # gloss_types = { gl["gloss"]:gl["type"] for gl in gloss_dict if gl["type"] is not None }
    gloss_types = { gl["gloss"]:gl["type"] for gl in gloss_dict }
    # gloss_classes = { gl["gloss"]:gl["class"] for gl in gloss_dict if gl["class"] is not None }
    gloss_classes = { gl["gloss"]:gl["class"] for gl in gloss_dict }
    return gloss_types, gloss_classes

def process_gloss(row_n: int, dat: pd.DataFrame):

    variation = dat.variation[row_n].strip()
    constituents = variation.split(" ")

    glossed = dat.glosses[row_n].strip()
    if glossed == '':
        glossed_markers_aligned = False
    else:
        markers = [re.split("-", constituent) for constituent in constituents]
        markers = [item for sublist in markers for item in sublist] #flattened
        glossed_markers = [glossed_constituent.split("-") for glossed_constituent in glossed.split(" ")]
        glossed_markers = [item.strip() for sublist in glossed_markers for item in sublist] #flattened
        
        if glossed_markers != [] and len(glossed_markers) == len(markers):
            glossed_markers_aligned = { markers[i]:[gloss for gloss in re.split("\.|=", glossed_markers[i]) if gloss != ''] for i, value in enumerate(markers)}
            #created a marker alignement dictionary (for table Glossing)
        else:
            glossed_markers_aligned = False
        
    lang_cand = dat.languages[row_n].strip()
    lang = session.query(Languages).filter(Languages.language==lang_cand).first()
    if lang is None:
        lang = Languages(language=lang_cand)
    
    constituents = [re.sub('\.|-', '', constituent).replace("=","\'") for constituent in constituents]
    lemmas = dat.lemmas[row_n].strip().split(' ')
    if lemmas != [] and len(constituents) == len(lemmas):
        lemmas_aligned = { constituent.strip():lemmas[i].strip() for i, constituent in enumerate(constituents) }
        #created a lemmas_alignement dictionary (for table Constituents)
    else:
        lemmas_aligned = { constituent:None for constituent in constituents }
    
    return glossed_markers_aligned, lemmas_aligned, lang

def create_glossing(mark_cand: str, glosses: list[str], lang, gloss_types: dict, gloss_classes: dict):
    for gl_cand in glosses:
        
        # checking if the type already in GlossTypes
        gl_type_cand = gloss_types[gl_cand]
        gl_type = session.query(GlossTypes).filter(GlossTypes.gloss_type==gl_type_cand).first()
        
        if gl_type is not None: #type exists
            # checking if the entry in Glosses needs to be created
            gl = session.query(Glosses).filter(Glosses.gloss==gl_cand, Glosses.glosstypes == gl_type).first()
            if gl is not None: #gloss exists
                #checking if the glossing needs to be created:
                    in_db =  session.query(Glossing).filter(Glossing.marker==mark_cand, Glossing.languages==lang, Glossing.glosses==gl).first()
                    if in_db is not None: #move on to the next mapping
                        # print (f"This combination marker, gloss, and language ({mark_cand}, {gl_cand}, lang) already in Glossing")
                        continue
                        
            else: #gloss does not exist (and hence neither the glossing)
                #retrieving/creating the gloss class needed for creation of a new Gloss
                gl_class_cand = gloss_classes[gl_cand]
                gl_class = session.query(GlossClass).filter(GlossClass.gloss_class==gl_class_cand).first()
                if gl_class is None:
                    gl_class = GlossClass(gloss_class=gl_class_cand)
                # creating the new Gloss with an existing type
                gl = Glosses(gloss=gl_cand, glossclass=gl_class, glosstypes = gl_type)
        else: #type does not exist (and hence neither the gloss, or the glossing)
            gl_type = GlossTypes(gloss_type=gl_type_cand)
            gl_class_cand = gloss_classes[gl_cand]
            gl_class = session.query(GlossClass).filter(GlossClass.gloss_class==gl_class_cand).first()
            if gl_class is None:
                gl_class = GlossClass(gloss_class=gl_class_cand)
            gl = Glosses(gloss=gl_cand, glossclass=gl_class, glosstypes = gl_type)
        
        glossing = Glossing(glosses=gl,
                            languages=lang, 
                            marker=mark_cand)
        session.add(glossing)
        session.commit()

def create_glossings(glossed_markers_aligned, lang, gloss_types: dict, gloss_classes: dict):
    #creating Glossings:
    if glossed_markers_aligned:
        # print("starting ")
        for mark_cand, glosses in glossed_markers_aligned.items():
            create_glossing(mark_cand, glosses, lang, gloss_types, gloss_classes)
    

session = Session(engine)

gloss_types, gloss_classes = import_gloss_dict()
# process_gloss(1, dat, gloss_types, gloss_classes)
glossed_markers_aligned, lemmas_aligned, lang = process_gloss(2, dat)

def create_constituents(lemmas_aligned: dict, lang):

    for constituent, lemma in lemmas_aligned.items():
        l = session.query(Lemmas).filter(Lemmas.lemma == lemma).first()
        if l is None:
            l = Lemmas(lemma=lemma)
        else:
            #checking if this constituent already exists
            const = session.query(Constituents).filter(Constituents.constituent == constituent, 
                                                       Constituents.languages == lang, Constituents.lemmas == l
                                                       ).first()
            if const is not None: # a constituent already exists
                continue
            
        constituents = Constituents(constituent=constituent, languages=lang, lemmas=l)
        session.add(constituents)
        session.commit()



start = time.time()
for rc in range(row_count):    
    try:
        validate_row(rc, dat)
    except Exception as ex:
        # print(ex)
        continue
    #Create Glossing, Glosses for every row
    glossed_markers_aligned, lemmas_aligned, lang = process_gloss(rc, dat)
    # print(str(rc))
    create_glossings(glossed_markers_aligned, lang, gloss_types, gloss_classes)
    create_constituents(lemmas_aligned, lang)
    # except Exception as ex:
    #     if type(ex) == KeyError:
    #         print(ex)
    #     else:
    #         print(ex)
    #     continue
end = time.time()
print("The time of execution of above program is :", (end-start), "s")

def add_formula(cand_form, cand_lang, cand_constr, cand_inton):
    
    lang = session.query(Languages).filter(Languages.language == cand_lang).first()
    constr = session.query(SourceConstructions).filter(SourceConstructions.construction == cand_constr).first()

    if lang is not None and constr is not None:
        form = session.query(Formulas).filter(
            Formulas.formula == cand_form, 
            Formulas.languages == lang, 
            Formulas.sourceconstructions == constr).first()
        
        if form is not None:
            raise ValueError("This combination of formula, language, and construction already in Formulas")
    else:
        if lang is None:
            lang = Languages(language=cand_lang)
        if constr is None:
            constr = SourceConstructions(construction=cand_constr)

    inton = session.query(Intonations).filter(Intonations.intonation == cand_inton).first()
    if inton is None:
        inton = Intonations(intonation=cand_inton)

    new_formula2 = Formulas(languages = lang, sourceconstructions=constr, intonations=inton, formula=cand_form)
    session.add(new_formula2)
    session.commit()
try:
    add_formula("ci mancherebbe", "it", "ci mancherebbe VP-Inf", "exclamative")
except Exception as ex:
    print(ex)







# new_formula = Formulas(languages = Languages(language=language), source_constructions=SourceConstructions(construction=construction), intonations=Intonations(intonation=intonation), formula=formula)
# # new_frame = 
# session.add(new_formula)
# session.commit()



# def split_data(dataframe):
#     new_Formulas: pd.DataFrame = dataframe[dataframe['status'].isin(['to_db', 'change'])]
#     delete_Formulas = dataframe[dataframe['status'].isin(['delete', 'change'])]
#     return new_Formulas, delete_Formulas


con.close()