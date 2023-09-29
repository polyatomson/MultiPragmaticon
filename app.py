from sqlalchemy import create_engine, func, and_, or_
from sqlalchemy.sql.expression import any_, all_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from fill_db import main as fill_db
from flask import Flask, render_template, request, redirect, url_for, send_file
import pandas as pd
import re
import time
import os
from conf import DBNAME, USER, PASSWORD, HOST

import psycopg2


class rlist(list):
    def remove_all(self, element):
        return [el for el in self if el != element]

app = Flask(__name__)
#test

def search_by_parameters(cursor, substring, inner_structure_type, inner_structure_subtype, languages, syntax,
                         primary_sem, add_sem, speech_act, structure, intonations, lemmas, glosses, sc_syntax):
    if substring:
        aux_dict = {
            'glosses': ('1',),
            'len_glosses': 0,
            'list_glosses': [],
            'lemmas': ('1',),
            'len_lemmas': 0,
            'list_lemmas': [],
            'inner_structure_type': ('1',),
            'len_inner_structure_type': 0,
            'list_inner_structure_type': [],
            'inner_structure_subtype': ('1',),
            'len_inner_structure_subtype': 0,
            'list_inner_structure_subtype': [],
            'primary_sem': ('1',),
            'len_primary_sem': 0,
            'list_primary_sem': [],
            'add_sem': ('1',),
            'len_add_sem': 0,
            'list_add_sem': [],
            'languages': ('1',),
            'len_languages': 0,
            'list_languages': [],
            'speech_act': ('1',),
            'len_speech_act': 0,
            'list_speech_act': [],
            'structure': ('1',),
            'len_structure': 0,
            'list_structure': [],
            'intonations': ('1',),
            'len_intonations': 0,
            'list_intonations': []
        }
        cursor.execute("""
                    WITH found_formulas AS (
                        SELECT formula_id, formula, language
                        FROM formulas
                        LEFT JOIN languages
                        ON formulas.language_id = languages.language_id
                        WHERE formula = %s
                       ), inner_structs AS (
                        SELECT realisation_id, inner_structure_type, inner_structure_subtype
                        FROM realisation2inner_structure
                        LEFT JOIN inner_structure_types
                        ON realisation2inner_structure.inner_structure_type_id = inner_structure_types.inner_structure_type_id
                        LEFT JOIN inner_structure_subtypes
                        ON realisation2inner_structure.inner_structure_subtype_id = inner_structure_subtypes.inner_structure_subtype_id
                        GROUP BY realisation_id, inner_structure_type, inner_structure_subtype
                       ), full_lemmas AS (
                        SELECT realisation_id, string_agg(lemma, ' ') AS lemmatized
                        FROM realisation2lemma
                        LEFT JOIN lemmas
                        ON realisation2lemma.lemma_id = lemmas.lemma_id
                        GROUP BY realisation_id
                       ), full_sem AS (
                        SELECT realisation_id, primary_sem, string_agg(additional_sem, ' | ') AS add_sem
                        FROM semantics
                        LEFT JOIN primary_semantics
                        ON semantics.primary_sem_id = primary_semantics.primary_sem_id
                        LEFT JOIN additional_semantics
                        ON semantics.additional_sem_id = additional_semantics.additional_sem_id
                        GROUP BY realisation_id, primary_sem
                       ), source_constr AS (
                        SELECT сonstruction_id, construction, construction_syntax, intonation AS sc_intonation
                        FROM source_constructions
                        LEFT JOIN intonations
                        ON source_constructions.intonation_id = intonations.intonation_id
                       ), sa AS (
                        SELECT realisation_id, string_agg(speech_acts.speech_act, ' | ') AS speech_act,
                        string_agg(speech_acts_1.speech_act, ' | ') AS speech_act_1
                        FROM (SELECT * FROM speech_acts) AS speech_acts_1
                        RIGHT JOIN realisation2speech_acts
                        ON realisation2speech_acts.speech_act_1_id = speech_acts_1.speech_act_id
                        LEFT JOIN speech_acts
                        ON realisation2speech_acts.speech_act_id = speech_acts.speech_act_id
                        GROUP BY realisation_id
                       )
                    SELECT formula
                    FROM realisations
                    JOIN found_formulas
                    ON realisations.formula_id = found_formulas.formula_id
                    LEFT JOIN inner_structs ON realisations.realisation_id = inner_structs.realisation_id
                    LEFT JOIN full_lemmas ON realisations.realisation_id = full_lemmas.realisation_id
                    LEFT JOIN full_sem ON realisations.realisation_id = full_sem.realisation_id
                    LEFT JOIN intonations ON realisations.intonation_id = intonations.intonation_id
                    LEFT JOIN source_constr ON realisations.source_constr_id = source_constr.сonstruction_id
                    LEFT JOIN sa ON realisations.realisation_id = sa.realisation_id
                    LEFT JOIN structures ON realisations.structure_id = structures.structure_id
                    GROUP BY formula, language, inner_structure_type, inner_structure_subtype, primary_sem
                    """, (substring,))
    else:
        aux_dict = {
                    'glosses': tuple(glosses) if glosses else ('1',),
                    'len_glosses': len(glosses) if glosses else 0,
                    'list_glosses': glosses if glosses else [],
                    'lemmas':  tuple(lemmas) if lemmas else ('1',),
                    'len_lemmas': len(lemmas) if lemmas else 0,
                    'list_lemmas': lemmas if lemmas else [],
                    'inner_structure_type':  tuple(inner_structure_type) if inner_structure_type else ('1',),
                    'len_inner_structure_type': len(inner_structure_type) if inner_structure_type else 0,
                    'list_inner_structure_type': inner_structure_type if inner_structure_type else [],
                    'inner_structure_subtype': tuple(inner_structure_subtype) if inner_structure_subtype else ('1',),
                    'len_inner_structure_subtype': len(inner_structure_subtype) if inner_structure_subtype else 0,
                    'list_inner_structure_subtype': inner_structure_subtype if inner_structure_subtype else [],
                    'primary_sem': tuple(primary_sem) if primary_sem else ('1',),
                    'len_primary_sem': len(primary_sem) if primary_sem else 0,
                    'list_primary_sem': primary_sem if primary_sem else [],
                    'add_sem': tuple(add_sem) if add_sem else ('1',),
                    'len_add_sem': len(add_sem) if add_sem else 0,
                    'list_add_sem': add_sem if add_sem else [],
                    'languages': tuple(languages) if languages else ('1',),
                    'len_languages': len(languages) if languages else 0,
                    'list_languages': languages if languages else [],
                    'speech_act': tuple(speech_act) if speech_act else ('1',),
                    'len_speech_act': len(speech_act) if speech_act else 0,
                    'list_speech_act': speech_act if speech_act else [],
                    'structure': tuple(structure) if structure else ('1',),
                    'len_structure': len(structure) if structure else 0,
                    'list_structure': structure if structure else [],
                    'intonations': tuple(intonations) if intonations else ('1',),
                    'len_intonations': len(intonations) if intonations else 0,
                    'list_intonations': intonations if intonations else []
        }
        # print(aux_dict)
        cursor.execute("""WITH glosses
                        AS(
                            SELECT realisation_id, gloss
                        FROM realisation2gloss
                        LEFT JOIN glosses using(gloss_id)),
                        inner_structure AS(SELECT realisation_id, inner_structure_type, inner_structure_subtype
                        FROM realisation2inner_structure
                        LEFT JOIN inner_structure_types using(inner_structure_type_id)
                        LEFT JOIN inner_structure_subtypes using(inner_structure_subtype_id)),
                        semantics AS(SELECT realisation_id, primary_sem, additional_sem
                            FROM semantics
                            LEFT JOIN primary_semantics using(primary_sem_id)
                            LEFT JOIN additional_semantics using(additional_sem_id)),
                        speech_acts
                        AS(
                            SELECT realisation_id, speech_acts.speech_act
                            AS speech_act, string_agg(speech_acts_1.speech_act, '|') AS speech_act_1
                            FROM(SELECT * FROM speech_acts) AS speech_acts_1
                            RIGHT JOIN realisation2speech_acts
                            ON speech_acts_1.speech_act_id = realisation2speech_acts.speech_act_1_id
                            LEFT JOIN speech_acts
                            ON realisation2speech_acts.speech_act_id = speech_acts.speech_act_id
                            GROUP BY realisation_id, speech_acts.speech_act),
                        lemmas AS(
                            SELECT realisation_id, lemma
                            FROM realisation2lemma
                            LEFT JOIN lemmas using(lemma_id)),
                        formulas AS(
                            SELECT formula_id, formula, language
                            FROM formulas
                            LEFT JOIN languages using(language_id)),
                        intonations AS(
                            SELECT realisation_id, intonation
                            FROM realisations
                            LEFT JOIN intonations using(intonation_id)
                        )
                        SELECT formula
                        FROM realisations
                        LEFT JOIN semantics using(realisation_id)
                        LEFT JOIN formulas using(formula_id)
                        WHERE (primary_sem IN %(primary_sem)s OR %(list_primary_sem)s='{}')
                        GROUP BY formula, language
                        HAVING COUNT(DISTINCT primary_sem) >= %(len_primary_sem)s
                        """, aux_dict)
    return cursor.fetchall(), aux_dict


@app.route('/')
def main_page():
    conn = psycopg2.connect(dbname=DBNAME,
                        user=USER,
                        password=PASSWORD,
                        host=HOST)

    cur = conn.cursor()

    cur.execute('SELECT formula FROM formulas')
    formulae = sorted([x[0] for x in cur.fetchall()])

    cur.execute('SELECT lemma FROM lemmas')
    lemmata = sorted([x[0] for x in cur.fetchall()])

    cur.execute('SELECT language FROM languages')
    languages = sorted([x[0] for x in cur.fetchall()])

    cur.execute('SELECT language FROM languages')
    languages = sorted([x[0] for x in cur.fetchall()])

    cur.execute('SELECT inner_structure_type FROM inner_structure_types')
    strucs_type = sorted([x[0] for x in cur.fetchall()])

    cur.execute('SELECT additional_sem FROM additional_semantics')
    add_sems = sorted([x[0] for x in cur.fetchall()])

    cur.execute('SELECT speech_act FROM speech_acts')
    speech_acts = sorted([x[0] for x in cur.fetchall()])

    cur.execute('SELECT primary_sem FROM primary_semantics')
    pragmatics = sorted([x[0] for x in cur.fetchall()])

    cur.execute('SELECT intonation FROM intonations')
    intonations = sorted([x[0] for x in cur.fetchall()])

    cur.execute('SELECT gloss FROM glosses')
    glosses = sorted([x[0] for x in cur.fetchall()])

    conn.close()
    return render_template('index.html', formulae=formulae, lemmata=lemmata,
                           languages=languages, strucs=strucs_type, add_sems=add_sems,
                           speech_acts=speech_acts, pragmatics=pragmatics,
                           intonations=intonations, glosses=glosses)


@app.route('/result', methods=['get'])
def result():
    conn = psycopg2.connect(dbname=DBNAME,
                        user=USER,
                        password=PASSWORD,
                        host=HOST)

    cur = conn.cursor()
    if not request.args:
        return redirect(url_for('main_page'))

    substring = request.args.get('word')
    pragmatics = request.args.getlist('pragmatics')
    add_sem = request.args.getlist('add_sem')
    lemmas = request.args.getlist('lemma')
    glosses = request.args.getlist('glosses')
    lang = request.args.getlist('language')
    syntax = request.args.get('syntax')
    speech_act = request.args.getlist('speech_act')
    structure = request.args.getlist('structure')
    intonation = request.args.getlist('intonation')
    inner_structure_type = request.args.getlist('inner')

    results, aux_dict = search_by_parameters(cur, substring=substring, inner_structure_type=inner_structure_type, inner_structure_subtype='', languages=lang, syntax=syntax,
                                   primary_sem=pragmatics, add_sem=add_sem, speech_act=speech_act, structure=structure, intonations=intonation,
                                   lemmas=lemmas, glosses=glosses, sc_syntax='')
    aux_dict['results'] = [x[0] for x in results]
    # print(aux_dict)
    cur.execute("""
            WITH glosses AS(
                SELECT realisation_id, gloss
                FROM realisation2gloss
                LEFT JOIN glosses using(gloss_id)),
                inner_structure AS(SELECT realisation_id, inner_structure_type, inner_structure_subtype
                FROM realisation2inner_structure
                LEFT JOIN inner_structure_types using(inner_structure_type_id)
                LEFT JOIN inner_structure_subtypes using(inner_structure_subtype_id)),
            semantics AS(SELECT realisation_id, primary_sem, additional_sem
                FROM semantics
                LEFT JOIN primary_semantics using(primary_sem_id)
                LEFT JOIN additional_semantics using(additional_sem_id)),
            speech_acts AS(
                SELECT realisation_id, speech_acts.speech_act
                AS speech_act, string_agg(speech_acts_1.speech_act, '|') AS speech_act_1
                FROM(SELECT * FROM speech_acts) AS speech_acts_1
                RIGHT JOIN realisation2speech_acts
                ON speech_acts_1.speech_act_id = realisation2speech_acts.speech_act_1_id
                LEFT JOIN speech_acts
                ON realisation2speech_acts.speech_act_id = speech_acts.speech_act_id
                GROUP BY realisation_id, speech_acts.speech_act),
            lemmas AS(
                SELECT realisation_id, lemma
                FROM realisation2lemma
                LEFT JOIN lemmas using(lemma_id)),
            formulas AS(
                SELECT formula_id, formula, language
                FROM formulas
                LEFT JOIN languages using(language_id)),
            intonations AS(
                            SELECT realisation_id, intonation
                            FROM realisations
                            LEFT JOIN intonations using(intonation_id)
                        )
            SELECT string_agg(formula, ' | ')
            FROM realisations
            LEFT JOIN glosses using(realisation_id)
            LEFT JOIN inner_structure using(realisation_id)
            LEFT JOIN semantics using(realisation_id)
            LEFT JOIN speech_acts using(realisation_id)
            LEFT JOIN lemmas using(realisation_id)
            LEFT JOIN formulas using(formula_id)
            LEFT JOIN intonations using(realisation_id)
            LEFT JOIN structures using(structure_id)
            WHERE formula=ANY(%(results)s)
            AND (gloss IN %(glosses)s OR %(list_glosses)s='{}')
            AND (lemma IN %(lemmas)s OR %(list_lemmas)s='{}')
            AND (speech_act IN %(speech_act)s OR %(list_speech_act)s='{}')
            AND (inner_structure_type IN %(inner_structure_type)s OR %(list_inner_structure_type)s='{}')
            AND (inner_structure_subtype IN %(inner_structure_subtype)s OR %(list_inner_structure_subtype)s='{}')
            AND (additional_sem IN %(add_sem)s OR %(list_add_sem)s='{}')
            AND (%(list_languages)s='{}' OR language=ANY(%(list_languages)s))
            AND (structure IN %(structure)s OR %(list_structure)s='{}')
            AND (intonation IN %(intonations)s OR %(list_intonations)s='{}')
            GROUP BY realisation, language
            HAVING COUNT(DISTINCT gloss) >= %(len_glosses)s
            AND COUNT(DISTINCT lemma) >= %(len_lemmas)s
            AND COUNT(DISTINCT speech_act) >= %(len_speech_act)s
            AND COUNT(DISTINCT inner_structure_type) >= %(len_inner_structure_type)s
            AND COUNT(DISTINCT inner_structure_subtype) >= %(len_inner_structure_subtype)s
            AND COUNT(DISTINCT additional_sem) >= %(len_add_sem)s
            AND COUNT(DISTINCT structure) >= %(len_structure)s
            AND COUNT(DISTINCT intonation) >= %(len_intonations)s
            """, aux_dict)
    
    results = cur.fetchall()
    # print(results)
    
    cur.execute("""
        WITH glosses AS(
                SELECT realisation_id, gloss
                FROM realisation2gloss
                LEFT JOIN glosses using(gloss_id)),
                inner_structure AS(SELECT realisation_id, inner_structure_type, inner_structure_subtype
                FROM realisation2inner_structure
                LEFT JOIN inner_structure_types using(inner_structure_type_id)
                LEFT JOIN inner_structure_subtypes using(inner_structure_subtype_id)),
            semantics AS(SELECT realisation_id, primary_sem, additional_sem
                FROM semantics
                LEFT JOIN primary_semantics using(primary_sem_id)
                LEFT JOIN additional_semantics using(additional_sem_id)),
            speech_acts AS(
                SELECT realisation_id, speech_acts.speech_act
                AS speech_act, string_agg(speech_acts_1.speech_act, '|') AS speech_act_1
                FROM(SELECT * FROM speech_acts) AS speech_acts_1
                RIGHT JOIN realisation2speech_acts
                ON speech_acts_1.speech_act_id = realisation2speech_acts.speech_act_1_id
                LEFT JOIN speech_acts
                ON realisation2speech_acts.speech_act_id = speech_acts.speech_act_id
                GROUP BY realisation_id, speech_acts.speech_act),
            lemmas AS(
                SELECT realisation_id, lemma
                FROM realisation2lemma
                LEFT JOIN lemmas using(lemma_id)),
            formulas AS(
                SELECT formula_id, formula, language
                FROM formulas
                LEFT JOIN languages using(language_id)),
            intonations AS(
                            SELECT realisation_id, intonation
                            FROM realisations
                            LEFT JOIN intonations using(intonation_id)
                        )
            SELECT formula, language, inner_structure_type, inner_structure_subtype, primary_sem,
            string_agg(CONCAT(realisation, '\n', full_gloss),'+'), string_agg(additional_sem, ' | '),
            string_agg(intonation, ' | '), string_agg(examples, ' | '), string_agg(speech_act, ' | '),
            string_agg(speech_act_1, ' | ')
            FROM realisations
            LEFT JOIN glosses using(realisation_id)
            LEFT JOIN inner_structure using(realisation_id)
            LEFT JOIN semantics using(realisation_id)
            LEFT JOIN speech_acts using(realisation_id)
            LEFT JOIN lemmas using(realisation_id)
            LEFT JOIN formulas using(formula_id)
            LEFT JOIN intonations using(realisation_id)
            LEFT JOIN structures using(structure_id)
            WHERE formula=ANY(%(results)s)
            GROUP BY language, formula, inner_structure_type, inner_structure_subtype, primary_sem, additional_sem, intonation, structure, speech_act, speech_act_1
    """, {'results': [x[0].split(' | ')[0] for x in results]})

    results = cur.fetchall()
    conn.close()
    if not results:
        return render_template('oops.html')
    
    df = pd.DataFrame(results)
    df.set_axis([
        'label', 'language', 'inner_structure_type', 'inner_structure_subtype',
        'primary_sem', 'variants', 'additional_sem', 'intonation', 'examples',
        'speech_act', 'speech_act_1'
        ], axis=1, inplace=True)

    df['DF_lang'] = df.apply(lambda x:'%s_%s' % (x['label'].strip(), x['language']), axis=1).str.strip()
    df = df.groupby(['DF_lang', 'primary_sem'])
    pretty_records = []
    
    for record in df:
        df = record[1]

        df = df.fillna('')
        glossing = df['variants'].to_list()[0].split('+')

        glossing = [[x.split('\n')[0], x.split('\n')[1]] for x in set(glossing)]
        if len(glossing) == 1 and not glossing[0][1]:
            glossing = []
        
        add_sems = set()
        for add_sem in df.additional_sem.to_list():
            add_sems.update(set(add_sem.split(' | ')))
        
        sas = set()
        for sa in df.speech_act.to_list():
            sas.update(set(sa.split(' | ')))
        
        sa_1s = set()
        for sa_1 in df.speech_act_1.to_list():
            sa_1s.update(set(sa_1.split(' | ')))
        pretty_records.append(
            {
                'label': df.label.to_list()[0],
                'Glossing': glossing,
                'Inner structure': df.inner_structure_type.to_list()[0] +\
                                   (':' + df['inner_structure_subtype'].to_list()[0] if df['inner_structure_subtype'].to_list()[0] else ''),
                'Language': df.language.to_list()[0],
                'Pragmatics': df.primary_sem.to_list()[0],
                'Additional semantics': sorted(rlist(add_sems).remove_all('')),
                'Speech act 1': ' | '.join(sorted(rlist(sa_1s).remove_all(''))),
                'Speech act': ' | '.join(sorted(rlist(sas).remove_all(''))),
                'Structure': 'tripartite' if set(df.speech_act_1.to_list()) != {''} else 'bipartite' if set(df.speech_act.to_list()) != {''} else '',
                'Intonation': ' | '.join(set(df.intonation.to_list()[0].split(' | '))),
                'examples': ' | '.join(set(df.examples.to_list()[0].split(' | '))).replace('{', '<b>').replace('}', '</b>')
            }
        )
    records = sorted(pretty_records, key=lambda x: (x['Language'], x['label']))
    
    statistics = pd.DataFrame()
    statistics['formula'] = [x['label'] for x in pretty_records]
    statistics['realisations'] = ['|'.join([x[0] for x in y['Glossing']]) for y in pretty_records]
    statistics['glosses'] = ['|'.join([x[1] for x in y['Glossing']]) for y in pretty_records]
    statistics['inner_structure'] = [x['Inner structure'] for x in pretty_records]
    statistics['language'] = [x['Language'] for x in pretty_records]
    statistics['pragmatics'] = [x['Pragmatics'] for x in pretty_records]
    statistics['additional_semantics'] = ['|'.join([x for x in y['Additional semantics']]) for y in pretty_records]
    statistics['speech_act_1'] = [x['Speech act 1'] for x in pretty_records]
    statistics['speech_act'] = [x['Speech act'] for x in pretty_records]
    statistics['intonation'] = [x['Intonation'] for x in pretty_records]
    statistics['examples'] = [x['examples'] for x in pretty_records]


    name = f'./files/{time.time()}.xlsx'
    statistics.to_excel(name)

    return render_template('result.html', records=records,
                           isinst=isinstance, lst=list, file=name)


@app.route('/about')
def about():
    return render_template('wip.html')


@app.route('/instruction')
def instruction():
    return render_template('wip.html')


@app.route('/publications')
def publications():
    return render_template('wip.html')


@app.route('/download', methods=['GET'])
def create_file():
    path = request.args.get('path')
    response = send_file(path, attachment_filename='Sample.xlsx', as_attachment=True)
    response.headers["x-filename"] = 'Sample.xlsx'
    response.headers["Access-Control-Expose-Headers"] = 'x-filename'
    os.remove(path)
    return response


@app.route('/update')
def fill():
    fill_db()
    return redirect(url_for('main_page'))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)