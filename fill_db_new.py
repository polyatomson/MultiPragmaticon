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
import json
import time

def my_timer(func): #timer wrapper

    def wrapper(*args, **kwargs):
        func_name = func.__name__
        print(f"Starting {func_name}")
        
        t1 = time.perf_counter()
        output = func(*args, **kwargs)
        t2 = time.perf_counter()
        
        print(f"Total time for {func_name}: {t2 - t1:.3f} s\n")
        return output
    
    return wrapper

engine = create_engine(f"postgresql://{USER}:{urllib.parse.quote_plus(PASSWORD)}@{HOST}/{DBNAME}")
con = engine.connect()
session = Session(engine)

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
Syntax, CxSyntax, CxSemantics, Examples, GlossTypes, InnerStructureSubtypes, InnerStructureTypes, Intonations, Languages, Lemmas, Pragmatics, Literature, Semantics, Speech_acts, Frames, GlossClass, Glosses, InnerStructure, SourceConstructions, Formulas, Frame2SpeechActs, Variations, Formula2InnerStructure, Frame2Var, Variation2Constituents, Glossing, Constituents, Constituents2Glossing = base.classes.Syntax, base.classes.CxSyntax, base.classes.CxSemantics, base.classes.Examples, base.classes.GlossTypes, base.classes.InnerStructureSubtypes, base.classes.InnerStructureTypes, base.classes.Intonations, base.classes.Languages, base.classes.Lemmas, base.classes.Pragmatics, base.classes.Literature, base.classes.Semantics, base.classes.SpeechActs, base.classes.Frames, base.classes.GlossClass, base.classes.Glosses, base.classes.InnerStructure, base.classes.SourceConstructions, base.classes.Formulas, base.classes.Frame2SpeechActs, base.classes.Variations, base.classes.Formula2InnerStructure, base.classes.Frame2Var, base.classes.Variation2Constituents, base.classes.Glossing, base.classes.Constituents, base.classes.Constituents2Glossing

def get_table() -> pd.DataFrame:
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('pragmaticon.json', scope)
    gc = gspread.authorize(credentials)
    sheet_url = DATAURL
    wb = gc.open_by_url(sheet_url)
    sheet = wb.worksheet('main')
    df = pd.DataFrame(sheet.get_all_values())
    return df

#Get the dataframe and save it as a pickle not to send API requests all the time
# dat = get_table()
# with open('pickled_df', 'wb') as f:
#      pickle.dump(dat, f)

def unpickle_df(pickled_df_fn: str = 'pickled_df'):
    with open(pickled_df_fn, 'rb') as f:
        dat: pd.DataFrame = pickle.load(f)

    dat.columns = dat.iloc[0]
    dat = dat.iloc[1:] #setting numeric indexing
    print(dat.columns.to_list())
    return dat

def validate_row(row_n: int, dat: pd.DataFrame) -> None:
    if dat.df[row_n].strip() == '' or dat.variation[row_n].strip() == '' or dat.languages[row_n].strip() == '':
         raise Exception(f"row {str(row_n-1)} is invalid: no formula, language, or variation")


def update_gloss_dict (dat: pd.DataFrame, old_dict: str = 'gloss_dict1.json', new_dict: str = 'gloss_dict_revise.json'):
    glosses = dat.glosses.unique()
    glosses = [re.split("=| |\.|-", gloss) for gloss in glosses]
    glosses_flat = set([item.strip() for sublist in glosses for item in sublist if item.strip() != ""])
    
    # getting annotated glosses:
    with open(old_dict, 'r') as f:
         existing_dict = json.load(f)
    existing_glosses = [gloss["gloss"] for gloss in existing_dict["glosses"]]
    new_glosses_count = 0
    for gl in glosses_flat:
         if gl not in existing_glosses:
              new_glosses_count += 1
              existing_dict["glosses"].append({"gloss":gl, "type": "lexical", "class": None, "change_to": None})
    if new_glosses_count != 0:
        with open(new_dict, 'w') as f:
            json.dump(existing_dict, f, indent=2)
    print(f"n_new_glosses: {str(new_glosses_count)}")

def import_gloss_dict(fn: str = 'gloss_dict1.json') -> dict:
    with open(fn, 'r') as f:
        gloss_dict = json.load(f)
    gloss_dict = gloss_dict["glosses"]
    gloss_types = { gl["gloss"]:gl["type"] for gl in gloss_dict }
    gloss_classes = { gl["gloss"]:gl["class"] for gl in gloss_dict }
    change = { gl["gloss"]:gl["change_to"] for gl in gloss_dict 
              if gl["status"] == "0" and gl["change_to"] != "specific_lexical_gloss"}
    return gloss_types, gloss_classes, change

def change_gloss(glosses: list[str], change: dict): #used in process_variation to replace incorrect glosses with the correct ones
    changed_list = list()
    for gloss in glosses:
        if gloss not in change:
            changed_list.append(gloss)
        else:
            for new_gl in change[gloss].split('.'):
                changed_list.append(new_gl)
    return changed_list


def create_lang(lang_cand: str):
    lang_cand = lang_cand.strip()
    lang = session.query(Languages).filter(Languages.language==lang_cand).first()
    if lang is None:
        lang = Languages(language=lang_cand)
        session.add(lang)
        
    return lang

def create_construction(cand_constr: str, cand_cx_sem: str, cand_cx_syntax: str, cand_cx_intonation: str, lang):
    
    if cand_constr.strip() == '':
        return None

    cand_constr, cand_cx_sem, cand_cx_syntax = cand_constr.strip(), cand_cx_sem.strip(), cand_cx_syntax.strip()
    constr = session.query(SourceConstructions).filter(SourceConstructions.construction == cand_constr, SourceConstructions.languages == lang).first()
    
    if constr is None:
        # create/retrieve cxsemantics:
        if cand_cx_sem != '':
            cxsem = session.query(CxSemantics).filter(CxSemantics.cx_semantics == cand_cx_sem).first()
            if cxsem is None:
                cxsem = CxSemantics(cx_semantics = cand_cx_sem)
        else:
            cxsem = None
        # create/retrieve cxsyntax:
        if cand_cx_syntax != '':
            cxsynt = session.query(CxSyntax).filter(CxSyntax.cx_syntax == cand_cx_syntax).first()
            if cxsynt is None:
                cxsynt = CxSyntax(cx_syntax = cand_cx_syntax)
        else:
            cxsynt = None

        # creating a new construction
        constr = SourceConstructions(construction=cand_constr, cxsemantics = cxsem, cxsyntax = cxsynt, cx_intonation = cand_cx_intonation, languages = lang)
        session.add(constr)
        session.commit()
    
    return constr

def create_formula(cand_form: str, lang, constr):
    
    cand_form = cand_form.strip()

    # checking if this formula is in Formulas
    form = session.query(Formulas).filter(
            Formulas.formula == cand_form, 
            Formulas.languages == lang, 
            Formulas.sourceconstructions == constr
            ).first()
        
    if form is None:
        form = Formulas(languages = lang, sourceconstructions=constr, formula=cand_form)
        session.add(form)
        session.commit()
    
    return form

def process_variation(row_n: int, dat: pd.DataFrame, change: dict) -> (str, str, list[dict]):

    variation = dat.variation[row_n].strip()
    constituents = [const.strip() for const in re.split(" | |=", variation)]
    constituents_pretty = [re.sub('\.|-|∅', '', constituent) for constituent in constituents]
    
    lemmas = dat.lemmas[row_n].strip().split(' ')
    if lemmas != [''] and len(constituents_pretty) == len(lemmas):
        #create a lemmas_alignement dictionary (for table Constituents)
        lemmas_aligned = lemmas
    else:
        lemmas_aligned = [None for cp in constituents_pretty]

    glossed = dat.glosses[row_n].strip()
    if glossed != '':
        glossed_constituents = [glossed_cost.strip() for glossed_cost in re.split(" | |=", glossed)]
        try:
            if len(glossed_constituents) != len(constituents_pretty):
                raise Exception("len of constituents in glosses vs. variation does not match")
            glosses_aligned = list()
            for i, cp in enumerate(constituents_pretty):
                constituent_markers = constituents[i].strip('.').split('-')
                glosses = [gc for gc in glossed_constituents[i].strip('.').split('-') if gc != '']
                if len(constituent_markers) != len(glosses):
                    raise Exception(("len of tokens in markers vs. glosses does not match"))
                #created a marker ~ list of glosses alignement dictionary for the constituent (for table Glossing)
                glossed_markers_aligned = [
                        {"marker":marker, "glosses":change_gloss(glosses[k].split('.'), change)}
                        for k, marker in enumerate(constituent_markers)]
                glosses_aligned.append(glossed_markers_aligned)
                # update full gloss to account for the changes in glosses
                glossed_constituents[i] = ".".join(glossed_markers_aligned[0]["glosses"]) + ''.join([
                   punc + ".".join(glossed_markers_aligned[i+1]["glosses"]) 
                   for i, punc in enumerate(re.findall('-|=', glossed_constituents[i]))
                ])
        except Exception as ex:
            print(str(row_n+1), ex)
            if str(ex) == "len of constituents in glosses vs. variation does not match":
                glossed_constituents = [None for cp in constituents_pretty]
            glosses_aligned = [None for cp in constituents_pretty]
    else:
        print(str(row_n+1), "no glosses")
        glossed_constituents = [None for cp in constituents_pretty]
        glosses_aligned = glossed_constituents
    

    constituents = [{"constituent": cp, 
                     "lemma": lemmas_aligned[i], 
                     "full_gloss": glossed_constituents[i],
                     "markers": glosses_aligned[i]
                     } for i, cp in enumerate(constituents_pretty)]
    return variation, constituents

def create_glossing(mark_cand: str, gl_cand: str, lang, gloss_types: dict, gloss_classes: dict):
    
    # checking if the type already in GlossTypes
    gl_type_cand = gloss_types[gl_cand]
    gl_type = session.query(GlossTypes).filter(GlossTypes.gloss_type==gl_type_cand).first()
    
    if gl_type is not None: #type exists
        # checking if the entry in Glosses needs to be created
        gl = session.query(Glosses).filter(Glosses.gloss==gl_cand, Glosses.glosstypes == gl_type).first()
        if gl is not None: #gloss exists
            #checking if the glossing needs to be created:
                in_db =  session.query(Glossing).filter(Glossing.marker==mark_cand, Glossing.languages==lang, Glossing.glosses==gl).first()
                if in_db is not None: #return glossing
                    # print (f"This combination marker, gloss, and language ({mark_cand}, {gl_cand}, lang) already in Glossing")
                    return in_db
                    
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
    
    return glossing

def create_glossings(glossed_markers_aligned: list[dict], lang, gloss_types: dict, gloss_classes: dict):
    #creating Glossings from a dict of markers
    if glossed_markers_aligned != None:
        # print("starting ")
        glossings = list()
        for glossed_marker in glossed_markers_aligned:
            glossings_new = [create_glossing(glossed_marker["marker"], gloss_cand, lang, gloss_types, gloss_classes)
                         for gloss_cand in glossed_marker["glosses"]]
            glossings.extend(glossings_new)
        return glossings

def create_constituent(constituent_dict: dict, lang, gloss_types:dict, gloss_classes: dict):
    const_cand = constituent_dict["constituent"]
    lemma_cand = constituent_dict["lemma"]
    markers = constituent_dict["markers"]
    glossings = create_glossings(markers, lang, gloss_types, gloss_classes)

    if lemma_cand is not None:
        l = session.query(Lemmas).filter(Lemmas.lemma == lemma_cand).first()
        if l is None:
            l = Lemmas(lemma=lemma_cand)
            session.add(l)
            session.commit()
    else:
        l = None
    #checking if this constituent already exists
    const = session.query(Constituents).filter(Constituents.constituent == const_cand, 
                                                    Constituents.languages == lang,
                                                    Constituents.lemmas == l
                                                    ).first()
    if const is None: # creating a new constituent
        full_gloss = constituent_dict["full_gloss"]
        const = Constituents(constituent=const_cand, languages=lang, lemmas=l, glossed=full_gloss)
        # , constituents2glossing_collection = glossings
        session.add(const)
        session.commit()

    if glossings is not None:
        for gl in glossings:
            c2g = session.query(Constituents2Glossing).filter(Constituents2Glossing.constituents == const, Constituents2Glossing.glossing == gl).first()
            if c2g is None:
                c2g = Constituents2Glossing(constituents = const, glossing = gl)
                session.add(c2g)
                session.commit()

    return const

def create_variation(var_cand: str, form, main, synt_cand: str, inton_cand: str):
    var_cand = var_cand.strip()
    var_cand = re.sub('\.|-', '', var_cand)
    #creating/retrieving intonation
    inton_cand = inton_cand.strip()
    if inton_cand != '':
        inton = session.query(Intonations).filter(Intonations.intonation == inton_cand).first()
        if inton == None:
            inton = Intonations(intonation = inton_cand)
            session.add(inton)
    else:
        inton = None
    # Checking if the unique variation already exists
    var = session.query(Variations).filter(Variations.variation == var_cand, 
                                           Variations.formulas == form, 
                                           Variations.intonations == inton
                                           ).first()
    if var != None:
        return var
    else:
        #creating/retrieving syntax
        synt_cand = synt_cand.strip()
        if synt_cand != '':
            synt = session.query(Syntax).filter(Syntax.syntax == synt_cand).first()
            if synt == None:
                synt = Syntax(syntax = synt_cand)
        else:
            synt = None
        
        if main != '':
            main = True
        else:
            # try to infer whether this is the main variation
            if var_cand == form.formula:
                main = True
            else:
                main = False

        # Creating a new var
        var = Variations(variation = var_cand, main = main, formulas = form, intonations = inton, syntax = synt)
        session.add(var)
        session.commit()
    
    return var

def create_var2const(var, consts: list):
    for const in consts:
        var2const = session.query(Variation2Constituents).filter(
            Variation2Constituents.variations == var,
            Variation2Constituents.constituents == const).first()
        if var2const is None:
            var2const = Variation2Constituents(variations = var, constituents = const)
            session.add(var2const)
            session.commit()

def create_inner_structure(dat_inner_structure: str, form):
    dat_inner_structure = dat_inner_structure.strip()
    if dat_inner_structure == "":
        return None
    
    # get rid of inconsitencies in spaces
    inner_structure_clean = ":".join([instr.strip() for instr in dat_inner_structure.split(':')])
    inner_structures = [inn_str.strip().split(':', maxsplit=1) for inn_str in inner_structure_clean.split('|') if inn_str != '']
    

    for inn_str in inner_structures:
        innstr1_cand = inn_str[0]
        if len(inn_str) > 1:
            innstr2_cand = inn_str[1]
        else:
            innstr2_cand = None
        
        #creating / retrieving the main inner_structure_subtype
        innstr1 = session.query(InnerStructureTypes).filter(
                InnerStructureTypes.inner_structure_type == innstr1_cand).first()
        if innstr1 is None:
            innstr1 = InnerStructureTypes(inner_structure_type = innstr1_cand)
            session.add(innstr1)
            session.commit()

        # creating / retrieving inner_structure_subtype
        if innstr2_cand is None:
            innstr2 = None
        else: 
            innstr2 = session.query(InnerStructureSubtypes).filter(
                InnerStructureSubtypes.inner_structure_subtype == innstr2_cand).first()
            if innstr2 is None:
                innstr2 = InnerStructureSubtypes(inner_structure_subtype = innstr2_cand)
                session.add(innstr2)
                session.commit()
        
        innstr = session.query(InnerStructure).filter(InnerStructure.innerstructuretypes == innstr1, 
                                                        InnerStructure.innerstructuresubtypes == innstr2).first()
        if innstr is None:
            innstr = InnerStructure(innerstructuretypes = innstr1, innerstructuresubtypes = innstr2)
            session.add(innstr)
            session.commit()

        form2innstr = session.query(Formula2InnerStructure).filter(
            Formula2InnerStructure.innerstructure == innstr,
            Formula2InnerStructure.formulas == form).first()
        if form2innstr is None:
            form2innstr = Formula2InnerStructure(innerstructure = innstr, formulas = form)
            session.add(form2innstr)
            session.commit()
    
    return True


@my_timer
def main(dat: pd.DataFrame):
    session = Session(engine)
    row_count = len(dat.values)
    # start = time.time()
    gloss_types, gloss_classes, change = import_gloss_dict() # import the glosses annotations
    for rc in range(row_count+1):    
        try:
            validate_row(rc, dat)
        except Exception as ex:
            print(ex)
            continue
        #Create tables with formal characteristics for each row:
        lang = create_lang(dat.languages[rc]) # create/retrieve the language
        constr = create_construction(dat.source_construction[rc],
                                    dat.cx_semantics[rc],
                                    dat.cx_syntax[rc],
                                    dat.cx_intonation[rc],
                                    lang) # create/retrieve the construction (and related tables)
        form = create_formula(dat.df[rc], lang, constr) # create/retrieve the formula
        # innstr_list = create_inner_structure(dat.inner_structure[rc]) #create/retrieve inner structures (and their types)
        create_inner_structure(dat.inner_structure[rc], form)
        var_cand, constituents_dict = process_variation(rc, dat, change) # parse constituents, glosses and lemmas
        consts = [create_constituent(const, lang, gloss_types, gloss_classes) for const in constituents_dict] # create Constituents and the dependent tables, and establish connections between them
        var = create_variation(var_cand, form, dat.main[rc], dat.syntax[rc], dat.intonation[rc]) #create Variations
        create_var2const(var, consts) # establish many-to-many relations between variations and constituents
        session.flush()
    
    session.close()
    # end = time.time()
    # print("The entire table data inserted in :", (end-start), "s")

# main(unpickle_df())

@my_timer
def get_all_constituents() -> list:
    # constituents_ids = con.execute(text("SELECT constituents.constitent_id"))
    constituents = con.execute(text("""
                                SELECT constituent, full_gloss, lang, lemma, json_agg(json_build_object('marker', markers, 'glosses', glosses)) FROM
                                (SELECT constituents.constituent as constituent, 
                                    constituents.glossed as full_gloss,
                                    languages.language as lang, 
                                    lemmas.lemma as lemma,
                                    glossing.marker as markers,
                                    min(constituents2glossing.constituents2glossing_id) as min_constituents2glossing_id,
                                    json_agg(glosses.gloss ORDER BY constituents2glossing.constituents2glossing_id) as glosses
                                    
                                FROM public.constituents constituents JOIN public.languages languages ON (constituents.language_id = languages.language_id)
                                    LEFT JOIN public.lemmas lemmas ON (constituents.lemma_id = lemmas.lemma_id)
                                    LEFT JOIN public.constituents2glossing constituents2glossing ON (constituents2glossing.constituent_id = constituents.constituent_id)
                                    JOIN public.glossing glossing ON (constituents2glossing.glossing_id = glossing.glossing_id)
                                    LEFT JOIN public.glosses glosses ON (glossing.gloss_id = glosses.gloss_id)
                                GROUP BY constituents.constituent, constituents.glossed, languages.language, lemmas.lemma, glossing.marker
                                ORDER BY constituents.constituent, min_constituents2glossing_id
                                    )
                                GROUP BY constituent, full_gloss, lang, lemma
                                    
                                
                                    """
                                    )).all()
    constituents = [tuple(row) for row in constituents]
    # glosses = con.
    # glossings = 
    return constituents

def get_all_constituents_strings() -> list:
    # constituents_ids = con.execute(text("SELECT constituents.constitent_id"))
    constituents = con.execute(text("""
                                SELECT constituent, full_gloss, lang, lemma, STRING_AGG(markers, '-'), STRING_AGG(glosses, '-') FROM
                                (SELECT constituents.constituent as constituent, 
                                    constituents.glossed as full_gloss,
                                    languages.language as lang, 
                                    lemmas.lemma as lemma,
                                    glossing.marker as markers,
                                    min(constituents2glossing.constituents2glossing_id) as min_constituents2glossing_id,
                                    STRING_AGG(glosses.gloss, '.' ORDER BY constituents2glossing.constituents2glossing_id) as glosses
                                    
                                FROM public.constituents constituents JOIN public.languages languages ON (constituents.language_id = languages.language_id)
                                    LEFT JOIN public.lemmas lemmas ON (constituents.lemma_id = lemmas.lemma_id)
                                    LEFT JOIN public.constituents2glossing constituents2glossing ON (constituents2glossing.constituent_id = constituents.constituent_id)
                                    LEFT JOIN public.glossing glossing ON (constituents2glossing.glossing_id = glossing.glossing_id)
                                    LEFT JOIN public.glosses glosses ON (glossing.gloss_id = glosses.gloss_id)
                                GROUP BY constituents.constituent, constituents.glossed, languages.language, lemmas.lemma, glossing.marker
                                ORDER BY constituents.constituent, min_constituents2glossing_id
                                    )
                                GROUP BY constituent, full_gloss, lang, lemma
                                    
                                
                                    """
                                    )).all()
    constituents = [tuple(row) for row in constituents]
    # glosses = con.
    # glossings = 
    return constituents



# all_const = get_all_constituents()
all_const = {"constituents":get_all_constituents_strings()}
with open('all_constituents.json', 'w', encoding='utf-8') as f:
    json.dump(all_const, f, indent = 2, ensure_ascii=False)

con.close()