import gspread
from oauth2client.client import GoogleCredentials
import pandas as pd
import re
from oauth2client.service_account import ServiceAccountCredentials
from conf import DBNAME, USER, PASSWORD, HOST

import psycopg2

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    'pragmaticon.json', scope
)
gc = gspread.authorize(credentials)

conn = psycopg2.connect(dbname=DBNAME,
                        user=USER,
                        password=PASSWORD,
                        host=HOST)

c = conn.cursor()

def create_tables():
    c.execute("""CREATE TABLE IF NOT EXISTS languages (
             language_id integer GENERATED ALWAYS AS IDENTITY,
             language char(2) NOT NULL UNIQUE,
             PRIMARY KEY(language_id)
          )""")
    conn.commit()

    c.execute("""CREATE TABLE IF NOT EXISTS formulas (
                formula_id integer GENERATED ALWAYS AS IDENTITY,
                formula varchar(50) NOT NULL,
                language_id integer,
                PRIMARY KEY(formula_id),
                CONSTRAINT fk_language
                    FOREIGN KEY(language_id) 
                        REFERENCES languages(language_id)
                        ON DELETE SET NULL
            )""")
    conn.commit()

    c.execute("""CREATE TABLE IF NOT EXISTS structures (
                structure_id integer GENERATED ALWAYS AS IDENTITY,
                structure varchar(50) UNIQUE,
                PRIMARY KEY(structure_id)
            )""")
    conn.commit()

    c.execute("""CREATE TABLE IF NOT EXISTS speech_acts (
                speech_act_id integer GENERATED ALWAYS AS IDENTITY,
                speech_act varchar(50) NOT NULL UNIQUE,
                PRIMARY KEY(speech_act_id)
            )""")
    conn.commit()

    c.execute("""CREATE TABLE IF NOT EXISTS intonations (
                intonation_id integer GENERATED ALWAYS AS IDENTITY,
                intonation varchar(50) NOT NULL UNIQUE,
                PRIMARY KEY(intonation_id)
            )""")
    conn.commit()

    c.execute("""CREATE TABLE IF NOT EXISTS primary_semantics (
                primary_sem_id integer GENERATED ALWAYS AS IDENTITY,
                primary_sem varchar(100) NOT NULL UNIQUE,
                PRIMARY KEY(primary_sem_id)
            )""")
    conn.commit()

    c.execute("""CREATE TABLE IF NOT EXISTS additional_semantics (
                additional_sem_id integer GENERATED ALWAYS AS IDENTITY,
                additional_sem varchar(100) NOT NULL UNIQUE,
                PRIMARY KEY(additional_sem_id)
            )""")
    conn.commit()

    c.execute("""CREATE TABLE IF NOT EXISTS inner_structure_types (
                inner_structure_type_id integer GENERATED ALWAYS AS IDENTITY,
                inner_structure_type varchar(100) NOT NULL UNIQUE,
                PRIMARY KEY(inner_structure_type_id)
            )""")
    conn.commit()

    c.execute("""CREATE TABLE IF NOT EXISTS inner_structure_subtypes (
                inner_structure_subtype_id integer GENERATED ALWAYS AS IDENTITY,
                inner_structure_subtype varchar(100) NOT NULL UNIQUE,
                PRIMARY KEY(inner_structure_subtype_id)
            )""")
    conn.commit()

    c.execute("""CREATE TABLE IF NOT EXISTS glosses (
                gloss_id integer GENERATED ALWAYS AS IDENTITY,
                gloss varchar(50) NOT NULL UNIQUE,
                PRIMARY KEY(gloss_id)
            )""")
    conn.commit()

    c.execute("""CREATE TABLE IF NOT EXISTS lemmas (
                lemma_id integer GENERATED ALWAYS AS IDENTITY,
                lemma varchar(50) NOT NULL UNIQUE,
                PRIMARY KEY(lemma_id)
            )""")
    conn.commit()

    c.execute("""CREATE TABLE IF NOT EXISTS source_constructions (
                сonstruction_id integer GENERATED ALWAYS AS IDENTITY,
                construction varchar(200) NOT NULL UNIQUE,
                construction_syntax varchar(200),
                intonation_id integer,
                PRIMARY KEY(сonstruction_id),
                CONSTRAINT fk_intonation
                    FOREIGN KEY(intonation_id) 
                        REFERENCES intonations(intonation_id)
                        ON DELETE SET NULL
            )""")
    conn.commit()

    c.execute("""CREATE TABLE IF NOT EXISTS realisations (
                realisation_id integer GENERATED ALWAYS AS IDENTITY,
                realisation varchar(100) NOT NULL,
                formula_id integer NOT NULL,
                structure_id integer,
                intonation_id integer,
                full_gloss varchar(200),
                syntax varchar(200),
                examples varchar(1024),
                source_constr_id integer,
                comments varchar(512),
                PRIMARY KEY(realisation_id),
                CONSTRAINT fk_formula
                    FOREIGN KEY(formula_id) 
                        REFERENCES formulas(formula_id)
                        ON DELETE CASCADE,
                CONSTRAINT fk_structure
                    FOREIGN KEY(structure_id) 
                        REFERENCES structures(structure_id)
                        ON DELETE SET NULL,
                CONSTRAINT fk_intonation
                    FOREIGN KEY(intonation_id) 
                        REFERENCES intonations(intonation_id)
                        ON DELETE SET NULL,
                CONSTRAINT fk_sc
                    FOREIGN KEY(source_constr_id) 
                        REFERENCES source_constructions(сonstruction_id)
                        ON DELETE SET NULL
            )""")
    conn.commit()

    c.execute("""CREATE TABLE IF NOT EXISTS semantics (
                semantics_id integer GENERATED ALWAYS AS IDENTITY,
                realisation_id integer NOT NULL,
                primary_sem_id integer,
                additional_sem_id integer,
                PRIMARY KEY(semantics_id),
                CONSTRAINT fk_realisation
                    FOREIGN KEY(realisation_id) 
                        REFERENCES realisations(realisation_id)
                        ON DELETE CASCADE,
                CONSTRAINT fk_primary_sem
                    FOREIGN KEY(primary_sem_id) 
                        REFERENCES primary_semantics(primary_sem_id)
                        ON DELETE SET NULL,
                CONSTRAINT fk_additional_sem
                    FOREIGN KEY(additional_sem_id) 
                        REFERENCES additional_semantics(additional_sem_id)
                        ON DELETE SET NULL
            )""")
    conn.commit()

    c.execute("""CREATE TABLE IF NOT EXISTS realisation2lemma (
                realisation2lemma_id integer GENERATED ALWAYS AS IDENTITY,
                realisation_id integer NOT NULL,
                lemma_id integer,
                PRIMARY KEY(realisation2lemma_id),
                CONSTRAINT fk_realisation
                    FOREIGN KEY(realisation_id) 
                        REFERENCES realisations(realisation_id)
                        ON DELETE CASCADE,
                CONSTRAINT fk_lemma
                    FOREIGN KEY(lemma_id) 
                        REFERENCES lemmas(lemma_id)
                        ON DELETE SET NULL
            )""")
    conn.commit()

    c.execute("""CREATE TABLE IF NOT EXISTS realisation2speech_acts (
                realisation2speech_act_id integer GENERATED ALWAYS AS IDENTITY,
                realisation_id integer NOT NULL,
                speech_act_1_id integer,
                speech_act_id integer,
                PRIMARY KEY(realisation2speech_act_id),
                CONSTRAINT fk_realisation
                    FOREIGN KEY(realisation_id) 
                        REFERENCES realisations(realisation_id)
                        ON DELETE CASCADE,
                CONSTRAINT fk_speech_act_1
                    FOREIGN KEY(speech_act_1_id) 
                        REFERENCES speech_acts(speech_act_id)
                        ON DELETE SET NULL,
                CONSTRAINT fk_speech_act
                    FOREIGN KEY(speech_act_id) 
                        REFERENCES speech_acts(speech_act_id)
                        ON DELETE SET NULL
            )""")
    conn.commit()

    c.execute("""CREATE TABLE IF NOT EXISTS realisation2inner_structure (
                realisation2inner_structure_id integer GENERATED ALWAYS AS IDENTITY,
                realisation_id integer NOT NULL,
                inner_structure_type_id integer,
                inner_structure_subtype_id integer,
                PRIMARY KEY(realisation_id),
                CONSTRAINT fk_realisation
                    FOREIGN KEY(realisation_id) 
                        REFERENCES realisations(realisation_id)
                        ON DELETE CASCADE,
                CONSTRAINT fk_inner_structure_type
                    FOREIGN KEY(inner_structure_type_id) 
                        REFERENCES inner_structure_types(inner_structure_type_id)
                        ON DELETE SET NULL,
                CONSTRAINT fk_inner_structure_subtype
                    FOREIGN KEY(inner_structure_subtype_id) 
                        REFERENCES inner_structure_subtypes(inner_structure_subtype_id)
                        ON DELETE SET NULL
            )""")
    conn.commit()
    c.execute("""CREATE TABLE IF NOT EXISTS realisation2gloss (
                realisation2gloss_id integer GENERATED ALWAYS AS IDENTITY,
                realisation_id integer NOT NULL,
                gloss_id integer,
                PRIMARY KEY(realisation2gloss_id),
                CONSTRAINT fk_realisation
                    FOREIGN KEY(realisation_id) 
                        REFERENCES realisations(realisation_id)
                        ON DELETE CASCADE,
                CONSTRAINT fk_gloss
                    FOREIGN KEY(gloss_id) 
                        REFERENCES glosses(gloss_id)
                        ON DELETE SET NULL
            )""")
    conn.commit()


def get_glosses(text, gloss_dict):
    gloss = re.split("\.|-|=| ", text)
    return [gloss_dict[i] for i in gloss if i] 


def dict_to_list(dictionary):
    return tuple((i,) for i, v in dictionary.items())


def split_data(dataframe):
    new_formulas: pd.DataFrame = dataframe[dataframe['status'].isin(['to_db', 'change'])]
    delete_formulas = dataframe[dataframe['status'].isin(['delete', 'change'])]
    return new_formulas, delete_formulas


def main():

    sheet_url = 'https://docs.google.com/spreadsheets/d/' \
                '1kyesqJ3k2WFmygRq7R1iL2ZC7yM-XNQ5Shye2rdEAL0/' \
                'edit#gid=67313858'
    wb = gc.open_by_url(sheet_url)
    sheet = wb.worksheet('Лист1')

    df = pd.DataFrame(sheet.get_all_values())
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    to_db, to_delete = split_data(df)

    formulas_to_del = to_delete['DF'].to_list()

    create_tables()

    glosses = [x.lower() for x in to_db['glosses'].to_list()]

    clear_none = lambda x_list: set([x.strip() for x in x_list if x])
    gloss = ' '.join(glosses)
    gloss_dict = {text.strip():id for id, text in enumerate(clear_none(sorted(re.split("\.|-|=| ", gloss))), 1)}

    lemmas = []
    for row in to_db['lemmas'].apply(str.split):
        lemmas.extend(row)
    lemmas = {text.strip():id for id, text in enumerate(clear_none(lemmas), 1) if text}

    to_db['DF_lang'] = to_db.apply(lambda x:'%s_%s' % (x['DF'].strip(), x['language']), axis=1).str.strip()
    
    inner_type = {text.strip():id for id, text in enumerate(clear_none(sorted(to_db['inner structure type'].unique())), 1)}
    inner_subtype = {text.strip():id for id, text in enumerate(clear_none(sorted(to_db['inner structure subtype'].unique())), 1)}
    intonation = {text.strip():id for id, text in enumerate(clear_none(sorted(to_db['intonation'].unique())), 1)}
    source_constr = {text.strip():id for id, text in enumerate(clear_none(sorted(to_db['source construction'].unique())), 1)}
    languages = {text.strip():id for id, text in enumerate(clear_none(sorted(to_db['language'].unique())), 1)}
    
    speechact1 = to_db['speech act 1'].apply(lambda x: x.split('|')).to_list()
    speechact1 = set([i.strip() for a in speechact1 for i in a])

    speechact = to_db['speech act'].apply(lambda x: x.split('|')).to_list()
    speechact = set([i.strip() for a in speechact for i in a])

    speechacts  = sorted(speechact | speechact1)
    speechacts = {text.strip():id for id, text in enumerate(clear_none(speechacts), 1)}
    
    add_semantics = to_db['additional semantics'].apply(lambda x: x.split('|')).to_list()
    add_semantics = sorted(set([i for a in add_semantics for i in a]))
    add_sem = {text:id for id, text in enumerate(clear_none(add_semantics), 1)}
    
    primary_sem = {text.strip():id for id, text in enumerate(clear_none(sorted(to_db['primary semantics'].unique())), 1)}

    structures = {text.strip():id  for id, text in enumerate(clear_none(sorted(to_db['structure'].unique())), 1)}

    clear_formulae = lambda x_list: [x for x in x_list if not x.startswith('_')]

    formulas = clear_formulae(sorted(to_db['DF_lang'].unique()))
    formulas = {text.strip():id for id, text in enumerate(clear_none(formulas), 1)}


    c.execute('SELECT gloss, gloss_id FROM glosses')
    glosses_from_db = dict(c.fetchall())
    gloss_dict.update(glosses_from_db)

    c.execute('SELECT lemma, lemma_id FROM lemmas')
    lemmas_from_db = dict(c.fetchall())
    lemmas.update(lemmas_from_db)

    c.execute('SELECT inner_structure_type, inner_structure_type_id FROM inner_structure_types')
    inner_type_from_db = dict(c.fetchall())
    inner_type.update(inner_type_from_db)

    c.execute('SELECT inner_structure_subtype, inner_structure_subtype_id FROM inner_structure_subtypes')
    inner_subtype_from_db = dict(c.fetchall())
    inner_subtype.update(inner_subtype_from_db)

    c.execute('SELECT intonation, intonation_id FROM intonations')
    intonations_from_db = dict(c.fetchall())
    intonation.update(intonations_from_db)

    c.execute('SELECT construction, сonstruction_id FROM source_constructions')
    sc_from_db = dict(c.fetchall())
    source_constr.update(sc_from_db)

    c.execute('SELECT language, language_id FROM languages')
    languages_from_db = dict(c.fetchall())
    languages.update(languages_from_db)

    c.execute('SELECT speech_act, speech_act_id FROM speech_acts')
    sa_from_db = dict(c.fetchall())
    speechacts.update(sa_from_db)

    c.execute('SELECT additional_sem, additional_sem_id from additional_semantics')
    add_sem_from_db = dict(c.fetchall())
    add_sem.update(add_sem_from_db)

    c.execute('SELECT primary_sem, primary_sem_id FROM primary_semantics')
    prim_sem_from_db = dict(c.fetchall())
    primary_sem.update(prim_sem_from_db)

    c.execute('SELECT structure, structure_id FROM structures')
    structures_from_db = dict(c.fetchall())
    structures.update(structures_from_db)

    c.execute('SELECT formula, language, formula_id FROM formulas LEFT JOIN languages using(language_id)')
    formulas_from_db = {x + '_' + y : z for x,y,z in c.fetchall()}
    formulas.update(formulas_from_db)


    dataframe = to_db[['source construction', 'SC syntax', 'SC intonation']].drop_duplicates()
    dataframe['index'] = dataframe.index
    dataframe = dataframe[['index', 'source construction', 'SC syntax', 'SC intonation']]
    source_constr_list = dataframe.values[1:]

    clear_source = lambda x_list: set([x[1] for x in x_list if x[1]])

    source_constr = {text.strip():id for id, text in enumerate(clear_source(source_constr_list), 1)}

    get_intontaion = lambda x: (x[1], None, (intonation[x[3]] if x[3] else None))
    source_constr_list = [get_intontaion(i) for i in source_constr_list.tolist()]

    realisations = []
    realisation2gloss = []
    realisation2inner_type = []
    semantics = []
    realisation2inner_structure = []
    realisation2speech_acts = []
    realisation2lemma = []

    number = 1
    for _, row in to_db.iterrows():
        if row['realisation'] == '' or row['DF'] == '':
            continue
        realisation = row['realisation'].strip()
        formula_id = (formulas[row['DF_lang'].strip()] if row['DF_lang'] else None)
        structure_id = (structures[row['structure'].strip()] if row['structure'] else None)
        intonation_id = (intonation[row['intonation'].strip()] if row['intonation'] else None)
        syntax = row['syntax'].strip()
        examples = row['examples'].strip()
        source_constr_id = (source_constr[row['source construction'].strip()] if row['source construction'] else None)
        comments = row['comments'].strip()
        gloss_row = row['glosses'].strip().lower()
        realisations.append([realisation, formula_id, structure_id, intonation_id, 
                            gloss_row, syntax, examples, source_constr_id, comments])
        
        gloss_row = get_glosses(gloss_row, gloss_dict)
        if gloss_row:
            for _, g in enumerate(gloss_row):
                realisation2gloss.append([number, g])

        inner_type_id = (inner_type[row['inner structure type'].strip()] if row['inner structure type'] else None)
        inner_subtype_id = (inner_subtype[row['inner structure subtype'].strip()] if row['inner structure subtype'] else None)
        realisation2inner_type.append([number, inner_type_id, inner_subtype_id])

        add_sem_list = row['additional semantics'].split('|')
        primary_sem_id = (primary_sem[row['primary semantics'].strip()] if row['primary semantics'] else None)
        for sem in add_sem_list:
            add_sem_id = (add_sem[sem.strip()] if sem else None)
            semantics.append([number, primary_sem_id, add_sem_id])

            speech_act = row['speech act'].split('|')
            speech_act1 = row['speech act 1'].split('|')
        for act in speech_act:
            for act1 in speech_act1:
                realisation2speech_acts.append([number, (speechacts[act1.strip()] if act1 else None), (speechacts[act.strip()] if act else None)])

        lemm = row['lemmas']
        for l in lemm.split():
            realisation2lemma.append([number, (lemmas[l.strip()] if l else None)])
        number += 1
    
    df_to_lang_list = []
    for k, _ in formulas.items():
        formula, language = k.split('_')
        if formula != '':
            df_to_lang_list.append([formula.strip(), (languages[language.strip()] if language else None)])
    

    c.executemany('INSERT INTO languages (language) VALUES (%s) ON CONFLICT DO NOTHING', dict_to_list(languages))
    conn.commit()
    print(df_to_lang_list)
    c.executemany('INSERT INTO formulas (formula, language_id) VALUES (%s, %s) ON CONFLICT DO NOTHING', df_to_lang_list)
    conn.commit()

    c.executemany('INSERT INTO intonations (intonation) VALUES (%s) ON CONFLICT DO NOTHING', dict_to_list(intonation))
    conn.commit()

    c.executemany('INSERT INTO structures (structure) VALUES (%s) ON CONFLICT DO NOTHING', dict_to_list(structures))
    conn.commit()

    c.executemany('INSERT INTO speech_acts (speech_act) VALUES (%s) ON CONFLICT DO NOTHING', dict_to_list(speechacts))
    conn.commit()

    c.executemany('INSERT INTO primary_semantics (primary_sem) VALUES (%s) ON CONFLICT DO NOTHING', dict_to_list(primary_sem))
    conn.commit()

    c.executemany('INSERT INTO additional_semantics (additional_sem) VALUES (%s) ON CONFLICT DO NOTHING', dict_to_list(add_sem))
    conn.commit()

    c.executemany('INSERT INTO inner_structure_types (inner_structure_type) VALUES (%s) ON CONFLICT DO NOTHING', dict_to_list(inner_type))
    conn.commit()

    c.executemany('INSERT INTO inner_structure_subtypes (inner_structure_subtype) VALUES (%s) ON CONFLICT DO NOTHING', dict_to_list(inner_subtype))
    conn.commit()

    c.executemany('INSERT INTO glosses (gloss) VALUES (%s) ON CONFLICT DO NOTHING', dict_to_list(gloss_dict))
    conn.commit()

    c.executemany('INSERT INTO lemmas (lemma) VALUES (%s) ON CONFLICT DO NOTHING', dict_to_list(lemmas))
    conn.commit()

    c.executemany('INSERT INTO source_constructions (construction, construction_syntax, intonation_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING', source_constr_list)
    conn.commit()

    c.executemany('INSERT INTO realisations (realisation, formula_id, structure_id, intonation_id, full_gloss, syntax, examples, source_constr_id, comments) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', realisations)
    conn.commit()

    c.executemany('INSERT INTO realisation2gloss (realisation_id, gloss_id) VALUES (%s, %s)', realisation2gloss)
    conn.commit()

    c.executemany('INSERT INTO semantics (realisation_id, primary_sem_id, additional_sem_id) VALUES (%s, %s, %s)', semantics)
    conn.commit()
    
    c.executemany('INSERT INTO realisation2inner_structure (realisation_id, inner_structure_type_id, inner_structure_subtype_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING', realisation2inner_type)
    conn.commit()

    c.executemany('INSERT INTO realisation2speech_acts (realisation_id, speech_act_1_id, speech_act_id) VALUES (%s, %s, %s)', realisation2speech_acts)
    conn.commit()

    c.executemany('INSERT INTO realisation2lemma (realisation_id, lemma_id) VALUES (%s, %s)', realisation2lemma)
    conn.commit()

    conn.close()

if __name__ == '__main__':
    main()

