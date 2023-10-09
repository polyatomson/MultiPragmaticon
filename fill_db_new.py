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
    with open("create_new_structure.sql") as file:
            query = text(file.read())
            con.execute(query)
    Base = automap_base()
    Base.prepare(autoload_with=engine)
    con.commit()
    return Base

base = create_tables()

Syntax, CxSyntax, CxSemantics, Examples, GlossTypes, InnerStructureSubtypes, InnerStructureTypes, Intonations, Languages, Lemmas, Pragmatics, Literature, Semantics, Speech_acts, Frames, GlossClass, Glosses, InnerStructure, SourceConstructions, Formulas, Frame2SpeechActs, Variations, Formula2InnerStructure, Frame2Var, Variation2Gloss, Variation2Lemma = base.classes.syntax, base.classes.cx_syntax, base.classes.cx_semantics, base.classes.examples, base.classes.gloss_types, base.classes.inner_structure_subtypes, base.classes.inner_structure_types, base.classes.intonations, base.classes.languages, base.classes.lemmas, base.classes.pragmatics, base.classes.literature, base.classes.semantics, base.classes.speech_acts, base.classes.frames, base.classes.gloss_class, base.classes.glosses, base.classes.inner_structure, base.classes.source_constructions, base.classes.formulas, base.classes.frame2speech_acts, base.classes.variations, base.classes.formula2inner_structure, base.classes.frame2var, base.classes.variation2gloss, base.classes.variation2lemma

# # Pickled the dataframe not to send API requests all the time
# dat = get_table()
# with open('pickled_df', 'wb') as f:
#      pickle.dump(dat, f)

with open('pickled_df', 'rb') as f:
     dat: pd.DataFrame = pickle.load(f)

dat.columns = dat.iloc[0]
dat = dat.iloc[1:] #setting numeric indexing
print(dat.columns.to_list())

def get_unique_values_single(colname_dat, dat) -> list:
    unique_values = [text.strip() for text in dat[colname_dat].unique() if text != '']
    return unique_values

def get_unique_values_multiple(colname_dat, dat) -> list:
    combs = [x.split('|') for x in dat[colname_dat].unique() if x != '']
    unique_values = set([item.strip() for sublist in combs for item in sublist if item != ''])
    unique_values = set([': '.join([part.strip() for part in v.split(':')]) for v in unique_values if ':' in v]) #to account for inconsistencies in the annotation
    return list(unique_values)

# print(get_unique_values_multiple('inner_structure', dat)[0:20])

def get_new_values(unq_v_fnc, colname_dat: str, column_n, dat: pd.DataFrame) -> list:
    unique_values = unq_v_fnc(colname_dat, dat)
    stmt = select(column_n)
    in_db = session.scalars(stmt).fetchall()
    unique_values_new = [uv for uv in unique_values if uv not in in_db]
    return unique_values_new

def update_unique_values(unique_values_new: list, table_n, column_n_str: str) -> None:
    to_db = [{column_n_str:uvn} for uvn in unique_values_new]
    if to_db != []:
         session.execute(insert(table_n), to_db)
    session.commit()


session = Session(engine)
new_formula = Formulas(languages = Languages(language='it'), source_constructions=SourceConstructions(construction='come Cl'), intonations=Intonations(intonation="question"), formula="come mai")
new_formula = Formulas(languages = Languages(language='it'), source_constructions=SourceConstructions(construction='come Cl'), intonations=Intonations(intonation="question"), formula="come mai")
session.add(new_formula)
session.commit()
update_unique_values(get_new_values(get_unique_values_single, 'syntax', Syntax.syntax, dat), Syntax, 'syntax')
update_unique_values(get_new_values(get_unique_values_single, 'cx_syntax', CxSyntax.cx_syntax, dat), CxSyntax, 'cx_syntax')
update_unique_values(get_new_values(get_unique_values_single,'cx_semantics', CxSemantics.cx_semantics, dat), CxSemantics, 'cx_semantics')
update_unique_values(get_new_values(get_unique_values_single,'intonation', Intonations.intonation, dat), Intonations, 'intonation')
update_unique_values(get_new_values(get_unique_values_single,'languages', Languages.language, dat), Languages, 'language')
update_unique_values(get_new_values(get_unique_values_single,'pragmatics', Pragmatics.pragmatics, dat), Pragmatics, 'pragmatics')
update_unique_values(get_new_values(get_unique_values_multiple, 'semantics', Semantics.semantics, dat), Semantics, 'semantics')


     


# con.fetchall()

# def get_glosses(text, gloss_dict):
#     gloss = re.split("\.|-|=| ", text)
#     return [gloss_dict[i] for i in gloss if i] 


# def dict_to_list(dictionary):
#     return tuple((i,) for i, v in dictionary.items())


# def split_data(dataframe):
#     new_Formulas: pd.DataFrame = dataframe[dataframe['status'].isin(['to_db', 'change'])]
#     delete_Formulas = dataframe[dataframe['status'].isin(['delete', 'change'])]
#     return new_Formulas, delete_Formulas


# def main():

    
    # wb = gc.open_by_url(sheet_url)
    # sheet = wb.worksheet('main')

    # df = pd.DataFrame(sheet.get_all_values())
#     df.columns = df.iloc[0]
#     df = df.iloc[1:]
#     to_db, to_delete = split_data(df)

#     Formulas_to_del = to_delete['DF'].to_list()

#     create_tables()

#     glosses = [x.lower() for x in to_db['glosses'].to_list()]

#     clear_none = lambda x_list: set([x.strip() for x in x_list if x])
#     gloss = ' '.join(glosses)
#     gloss_dict = {text.strip():id for id, text in enumerate(clear_none(sorted(re.split("\.|-|=| ", gloss))), 1)}

#     lemmas = []
#     for row in to_db['lemmas'].apply(str.split):
#         lemmas.extend(row)
#     lemmas = {text.strip():id for id, text in enumerate(clear_none(lemmas), 1) if text}

#     to_db['DF_lang'] = to_db.apply(lambda x:'%s_%s' % (x['DF'].strip(), x['language']), axis=1).str.strip()
    
#     inner_type = {text.strip():id for id, text in enumerate(clear_none(sorted(to_db['inner structure type'].unique())), 1)}
#     inner_subtype = {text.strip():id for id, text in enumerate(clear_none(sorted(to_db['inner structure subtype'].unique())), 1)}
#     intonation = {text.strip():id for id, text in enumerate(clear_none(sorted(to_db['intonation'].unique())), 1)}
#     source_constr = {text.strip():id for id, text in enumerate(clear_none(sorted(to_db['source construction'].unique())), 1)}
#     Languages = {text.strip():id for id, text in enumerate(clear_none(sorted(to_db['language'].unique())), 1)}
    
#     speechact1 = to_db['speech act 1'].apply(lambda x: x.split('|')).to_list()
#     speechact1 = set([i.strip() for a in speechact1 for i in a])

#     speechact = to_db['speech act'].apply(lambda x: x.split('|')).to_list()
#     speechact = set([i.strip() for a in speechact for i in a])

#     speechacts  = sorted(speechact | speechact1)
#     speechacts = {text.strip():id for id, text in enumerate(clear_none(speechacts), 1)}
    
#     add_semantics = to_db['additional semantics'].apply(lambda x: x.split('|')).to_list()
#     add_semantics = sorted(set([i for a in add_semantics for i in a]))
#     add_sem = {text:id for id, text in enumerate(clear_none(add_semantics), 1)}
    
#     primary_sem = {text.strip():id for id, text in enumerate(clear_none(sorted(to_db['primary semantics'].unique())), 1)}

#     structures = {text.strip():id  for id, text in enumerate(clear_none(sorted(to_db['structure'].unique())), 1)}

#     clear_formulae = lambda x_list: [x for x in x_list if not x.startswith('_')]

#     Formulas = clear_formulae(sorted(to_db['DF_lang'].unique()))
#     Formulas = {text.strip():id for id, text in enumerate(clear_none(Formulas), 1)}


#     c.execute('SELECT gloss, gloss_id FROM glosses')
#     glosses_from_db = dict(c.fetchall())
#     gloss_dict.update(glosses_from_db)

#     c.execute('SELECT lemma, lemma_id FROM lemmas')
#     lemmas_from_db = dict(c.fetchall())
#     lemmas.update(lemmas_from_db)

#     c.execute('SELECT inner_structure_type, inner_structure_type_id FROM inner_structure_types')
#     inner_type_from_db = dict(c.fetchall())
#     inner_type.update(inner_type_from_db)

#     c.execute('SELECT inner_structure_subtype, inner_structure_subtype_id FROM inner_structure_subtypes')
#     inner_subtype_from_db = dict(c.fetchall())
#     inner_subtype.update(inner_subtype_from_db)

#     c.execute('SELECT intonation, intonation_id FROM intonations')
#     intonations_from_db = dict(c.fetchall())
#     intonation.update(intonations_from_db)

#     c.execute('SELECT construction, сonstruction_id FROM source_constructions')
#     sc_from_db = dict(c.fetchall())
#     source_constr.update(sc_from_db)

#     c.execute('SELECT language, language_id FROM Languages')
#     languages_from_db = dict(c.fetchall())
#     Languages.update(languages_from_db)

#     c.execute('SELECT speech_act, speech_act_id FROM speech_acts')
#     sa_from_db = dict(c.fetchall())
#     speechacts.update(sa_from_db)

#     c.execute('SELECT additional_sem, additional_sem_id from additional_semantics')
#     add_sem_from_db = dict(c.fetchall())
#     add_sem.update(add_sem_from_db)

#     c.execute('SELECT primary_sem, primary_sem_id FROM primary_semantics')
#     prim_sem_from_db = dict(c.fetchall())
#     primary_sem.update(prim_sem_from_db)

#     c.execute('SELECT structure, structure_id FROM structures')
#     structures_from_db = dict(c.fetchall())
#     structures.update(structures_from_db)

#     c.execute('SELECT formula, language, formula_id FROM Formulas LEFT JOIN Languages using(language_id)')
#     Formulas_from_db = {x + '_' + y : z for x,y,z in c.fetchall()}
#     Formulas.update(Formulas_from_db)


#     dataframe = to_db[['source construction', 'SC syntax', 'SC intonation']].drop_duplicates()
#     dataframe['index'] = dataframe.index
#     dataframe = dataframe[['index', 'source construction', 'SC syntax', 'SC intonation']]
#     source_constr_list = dataframe.values[1:]

#     clear_source = lambda x_list: set([x[1] for x in x_list if x[1]])

#     source_constr = {text.strip():id for id, text in enumerate(clear_source(source_constr_list), 1)}

#     get_intontaion = lambda x: (x[1], None, (intonation[x[3]] if x[3] else None))
#     source_constr_list = [get_intontaion(i) for i in source_constr_list.tolist()]

#     realisations = []
#     realisation2gloss = []
#     realisation2inner_type = []
#     semantics = []
#     realisation2inner_structure = []
#     realisation2speech_acts = []
#     realisation2lemma = []

#     number = 1
#     for _, row in to_db.iterrows():
#         if row['realisation'] == '' or row['DF'] == '':
#             continue
#         realisation = row['realisation'].strip()
#         formula_id = (Formulas[row['DF_lang'].strip()] if row['DF_lang'] else None)
#         structure_id = (structures[row['structure'].strip()] if row['structure'] else None)
#         intonation_id = (intonation[row['intonation'].strip()] if row['intonation'] else None)
#         syntax = row['syntax'].strip()
#         examples = row['examples'].strip()
#         source_constr_id = (source_constr[row['source construction'].strip()] if row['source construction'] else None)
#         comments = row['comments'].strip()
#         gloss_row = row['glosses'].strip().lower()
#         realisations.append([realisation, formula_id, structure_id, intonation_id, 
#                             gloss_row, syntax, examples, source_constr_id, comments])
        
#         gloss_row = get_glosses(gloss_row, gloss_dict)
#         if gloss_row:
#             for _, g in enumerate(gloss_row):
#                 realisation2gloss.append([number, g])

#         inner_type_id = (inner_type[row['inner structure type'].strip()] if row['inner structure type'] else None)
#         inner_subtype_id = (inner_subtype[row['inner structure subtype'].strip()] if row['inner structure subtype'] else None)
#         realisation2inner_type.append([number, inner_type_id, inner_subtype_id])

#         add_sem_list = row['additional semantics'].split('|')
#         primary_sem_id = (primary_sem[row['primary semantics'].strip()] if row['primary semantics'] else None)
#         for sem in add_sem_list:
#             add_sem_id = (add_sem[sem.strip()] if sem else None)
#             semantics.append([number, primary_sem_id, add_sem_id])

#             speech_act = row['speech act'].split('|')
#             speech_act1 = row['speech act 1'].split('|')
#         for act in speech_act:
#             for act1 in speech_act1:
#                 realisation2speech_acts.append([number, (speechacts[act1.strip()] if act1 else None), (speechacts[act.strip()] if act else None)])

#         lemm = row['lemmas']
#         for l in lemm.split():
#             realisation2lemma.append([number, (lemmas[l.strip()] if l else None)])
#         number += 1
    
#     df_to_lang_list = []
#     for k, _ in Formulas.items():
#         formula, language = k.split('_')
#         if formula != '':
#             df_to_lang_list.append([formula.strip(), (Languages[language.strip()] if language else None)])
    

#     c.executemany('INSERT INTO Languages (language) VALUES (%s) ON CONFLICT DO NOTHING', dict_to_list(Languages))
#     conn.commit()
#     # print(df_to_lang_list)
#     c.executemany('INSERT INTO Formulas (formula, language_id) VALUES (%s, %s) ON CONFLICT DO NOTHING', df_to_lang_list)
#     conn.commit()

#     c.executemany('INSERT INTO intonations (intonation) VALUES (%s) ON CONFLICT DO NOTHING', dict_to_list(intonation))
#     conn.commit()

#     c.executemany('INSERT INTO structures (structure) VALUES (%s) ON CONFLICT DO NOTHING', dict_to_list(structures))
#     conn.commit()

#     c.executemany('INSERT INTO speech_acts (speech_act) VALUES (%s) ON CONFLICT DO NOTHING', dict_to_list(speechacts))
#     conn.commit()

#     c.executemany('INSERT INTO primary_semantics (primary_sem) VALUES (%s) ON CONFLICT DO NOTHING', dict_to_list(primary_sem))
#     conn.commit()

#     c.executemany('INSERT INTO additional_semantics (additional_sem) VALUES (%s) ON CONFLICT DO NOTHING', dict_to_list(add_sem))
#     conn.commit()

#     c.executemany('INSERT INTO inner_structure_types (inner_structure_type) VALUES (%s) ON CONFLICT DO NOTHING', dict_to_list(inner_type))
#     conn.commit()

#     c.executemany('INSERT INTO inner_structure_subtypes (inner_structure_subtype) VALUES (%s) ON CONFLICT DO NOTHING', dict_to_list(inner_subtype))
#     conn.commit()

#     c.executemany('INSERT INTO glosses (gloss) VALUES (%s) ON CONFLICT DO NOTHING', dict_to_list(gloss_dict))
#     conn.commit()

#     c.executemany('INSERT INTO lemmas (lemma) VALUES (%s) ON CONFLICT DO NOTHING', dict_to_list(lemmas))
#     conn.commit()

#     c.executemany('INSERT INTO source_constructions (construction, construction_syntax, intonation_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING', source_constr_list)
#     conn.commit()

#     c.executemany('INSERT INTO realisations (realisation, formula_id, structure_id, intonation_id, full_gloss, syntax, examples, source_constr_id, comments) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', realisations)
#     conn.commit()

#     c.executemany('INSERT INTO realisation2gloss (realisation_id, gloss_id) VALUES (%s, %s)', realisation2gloss)
#     conn.commit()

#     c.executemany('INSERT INTO semantics (realisation_id, primary_sem_id, additional_sem_id) VALUES (%s, %s, %s)', semantics)
#     conn.commit()
    
#     c.executemany('INSERT INTO realisation2inner_structure (realisation_id, inner_structure_type_id, inner_structure_subtype_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING', realisation2inner_type)
#     conn.commit()

#     c.executemany('INSERT INTO realisation2speech_acts (realisation_id, speech_act_1_id, speech_act_id) VALUES (%s, %s, %s)', realisation2speech_acts)
#     conn.commit()

#     c.executemany('INSERT INTO realisation2lemma (realisation_id, lemma_id) VALUES (%s, %s)', realisation2lemma)
#     conn.commit()

#     conn.close()
# if __name__ == '__main__':
#     main()


con.close()