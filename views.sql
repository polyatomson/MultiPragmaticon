-- public.all_constituents source

CREATE OR REPLACE VIEW public.all_constituents
AS SELECT constituents.constituent,
    constituents.constituent_id AS id,
    constituents.glossed AS full_gloss,
    languages.language AS lang,
    lemmas.lemma,
    min(constituents2glossing.constituents2glossing_id) AS min_constituents2glossing_id,
    string_agg(glosses.gloss::text, '.'::text ORDER BY constituents2glossing.constituents2glossing_id) AS glosses,
        CASE
            WHEN glossing.marker IS NOT NULL THEN concat(glossing.marker, glossing.marker_id)
            ELSE NULL::text
        END AS markers
   FROM constituents2glossing constituents2glossing
     FULL JOIN constituents constituents ON constituents2glossing.constituent_id = constituents.constituent_id
     LEFT JOIN languages languages ON constituents.language_id = languages.language_id
     LEFT JOIN lemmas lemmas ON constituents.lemma_id = lemmas.lemma_id
     LEFT JOIN glossing glossing ON constituents2glossing.glossing_id = glossing.glossing_id
     LEFT JOIN glosses glosses ON glossing.gloss_id = glosses.gloss_id
  GROUP BY constituents.constituent, constituents.constituent_id, constituents.glossed, languages.language, lemmas.lemma, (
        CASE
            WHEN glossing.marker IS NOT NULL THEN concat(glossing.marker, glossing.marker_id)
            ELSE NULL::text
        END)
  ORDER BY constituents.constituent, (min(constituents2glossing.constituents2glossing_id));


-- public.all_constituents_str_agg source

CREATE OR REPLACE VIEW public.all_constituents_str_agg
AS SELECT constituent,
    lang,
    lemma,
    string_agg(glosses, '-'::text) AS glosses_agg,
    string_agg(markers, '-'::text) AS markers_agg
   FROM all_constituents
  GROUP BY constituent, id, full_gloss, lang, lemma
  ORDER BY lang, constituent, id;


-- public.glossed_markers source

CREATE OR REPLACE VIEW public.glossed_markers
AS SELECT s.marker,
    s.glossed,
    s.lang,
    s.n_occurences,
    const.constituent AS example
   FROM ( SELECT concat_ws('_'::text, glossing.marker, glossing.marker_id::character varying) AS marker,
            string_agg(DISTINCT glosses.gloss::text, '.'::text) AS glossed,
            languages.language AS lang,
            count(c2g.glossing_id) AS n_occurences,
            min(c2g.constituent_id) AS example_id
           FROM glossing glossing
             LEFT JOIN constituents2glossing c2g ON c2g.glossing_id = glossing.glossing_id
             LEFT JOIN glosses ON glossing.gloss_id = glosses.gloss_id
             LEFT JOIN languages languages ON glossing.language_id = languages.language_id
          GROUP BY glossing.marker, glossing.marker_id, languages.language
          ORDER BY languages.language, (concat_ws('_'::text, glossing.marker, glossing.marker_id::character varying)), (count(c2g.glossing_id)) DESC, (concat_ws('_'::text, glossing.marker, glossing.marker_id))) s
     JOIN constituents const ON const.constituent_id = s.example_id;


-- public.inner_structure_ru source

CREATE OR REPLACE VIEW public.inner_structure_ru
AS SELECT inst.inner_structure_type AS inns_type,
    insst.inner_structure_subtype AS inns_subtype,
    pragm.pragmatics,
    count(DISTINCT form.formula_id) AS n_formulas,
    sem.semantics,
    array_agg(DISTINCT form.formula) AS formulas_l
   FROM frame2var fr2var
     JOIN frames fr USING (frame_id)
     JOIN variations var USING (variation_id)
     FULL JOIN pragmatics pragm USING (pragmatics_id)
     FULL JOIN semantics sem USING (semantics_id)
     FULL JOIN formulas form USING (formula_id)
     JOIN languages lang USING (language_id)
     FULL JOIN formula2inner_structure fis USING (formula_id)
     JOIN inner_structure instr USING (inner_structure_id)
     FULL JOIN inner_structure_types inst USING (inner_structure_type_id)
     FULL JOIN inner_structure_subtypes insst USING (inner_structure_subtype_id)
  WHERE lang.language = 'ru'::bpchar
  GROUP BY inst.inner_structure_type, insst.inner_structure_subtype, pragm.pragmatics, sem.semantics
  ORDER BY inst.inner_structure_type, insst.inner_structure_subtype, (count(form.formula_id)) DESC;


-- public.inner_structure_ru_simple source

CREATE OR REPLACE VIEW public.inner_structure_ru_simple
AS SELECT inns_type,
    inns_subtype,
    pragmatics,
    sem_l,
    n_formulas,
    formulas_l,
    language
   FROM ( SELECT inst.inner_structure_type AS inns_type,
            insst.inner_structure_subtype AS inns_subtype,
            pragm.pragmatics,
            array_agg(DISTINCT sem.semantics) AS sem_l,
            count(DISTINCT form.formula_id) AS n_formulas,
            array_agg(DISTINCT form.formula) AS formulas_l,
            lang.language
           FROM frame2var fr2var
             JOIN frames fr USING (frame_id)
             JOIN variations var USING (variation_id)
             FULL JOIN pragmatics pragm USING (pragmatics_id)
             FULL JOIN semantics sem USING (semantics_id)
             FULL JOIN formulas form USING (formula_id)
             JOIN languages lang USING (language_id)
             FULL JOIN formula2inner_structure fis USING (formula_id)
             JOIN inner_structure instr USING (inner_structure_id)
             FULL JOIN inner_structure_types inst USING (inner_structure_type_id)
             FULL JOIN inner_structure_subtypes insst USING (inner_structure_subtype_id)
          GROUP BY inst.inner_structure_type, insst.inner_structure_subtype, pragm.pragmatics, lang.language
          ORDER BY inst.inner_structure_type, insst.inner_structure_subtype, (count(DISTINCT form.formula_id)) DESC) dat
  WHERE n_formulas > 1;


-- public.inner_structure_view source

CREATE OR REPLACE VIEW public.inner_structure_view
AS SELECT inns_type,
    inns_subtype,
    pragmatics,
    n_formulas,
    semantics,
    langs,
    list_formulas
   FROM ( SELECT inst.inner_structure_type AS inns_type,
            insst.inner_structure_subtype AS inns_subtype,
            pragm.pragmatics,
            count(DISTINCT form.formula_id) AS n_formulas,
            sem.semantics,
            string_agg(DISTINCT lang.language::text, ', '::text) AS langs,
            array_agg(DISTINCT form.formula) AS list_formulas,
            count(DISTINCT lang.language) AS n_langs
           FROM frame2var fr2var
             JOIN frames fr USING (frame_id)
             JOIN variations var USING (variation_id)
             FULL JOIN pragmatics pragm USING (pragmatics_id)
             FULL JOIN semantics sem USING (semantics_id)
             FULL JOIN formulas form USING (formula_id)
             JOIN languages lang USING (language_id)
             JOIN formula2inner_structure fis USING (formula_id)
             JOIN inner_structure instr USING (inner_structure_id)
             FULL JOIN inner_structure_types inst USING (inner_structure_type_id)
             FULL JOIN inner_structure_subtypes insst USING (inner_structure_subtype_id)
          WHERE fr.pragmatics_id IS NOT NULL
          GROUP BY inst.inner_structure_type, insst.inner_structure_subtype, pragm.pragmatics, sem.semantics
          ORDER BY inst.inner_structure_type, insst.inner_structure_subtype, (count(form.formula_id)) DESC) dat
  WHERE n_formulas > 1;


-- public.polysemic1 source

CREATE OR REPLACE VIEW public.polysemic1
AS SELECT formula,
    frames,
    n_frames,
    lang
   FROM ( SELECT dat.formula,
            array_agg(dat.frame) AS frames,
            count(dat.frame_id) AS n_frames,
            dat.lang
           FROM ( SELECT f.formula,
                    concat_ws(':'::text, p.pragmatics, s.semantics::character varying) AS frame,
                    fr.frame_id,
                    l.language AS lang
                   FROM frame2var fv
                     JOIN variations v USING (variation_id)
                     JOIN formulas f USING (formula_id)
                     JOIN languages l USING (language_id)
                     JOIN frames fr USING (frame_id)
                     JOIN pragmatics p USING (pragmatics_id)
                     FULL JOIN semantics s USING (semantics_id)
                  GROUP BY fr.frame_id, f.formula, p.pragmatics, s.semantics, l.language) dat
          GROUP BY dat.formula, dat.lang
          ORDER BY (count(dat.frame_id)) DESC) dat2
  WHERE n_frames > 1;


-- public.polysemy source

CREATE OR REPLACE VIEW public.polysemy
AS SELECT count(DISTINCT formula) AS n_formulas,
    pragm_tags,
    n_pragmatics,
    array_agg(formula) AS formulas_l
   FROM ( SELECT concat_ws('_'::text, f.formula, l.language) AS formula,
            array_agg(DISTINCT p.pragmatics) AS pragm_tags,
            array_agg(DISTINCT s.semantics) AS sem_tags,
            count(DISTINCT p.pragmatics) AS n_pragmatics
           FROM frame2var fv
             JOIN variations v USING (variation_id)
             JOIN formulas f USING (formula_id)
             JOIN languages l USING (language_id)
             JOIN frames fr USING (frame_id)
             JOIN pragmatics p USING (pragmatics_id)
             FULL JOIN semantics s USING (semantics_id)
          GROUP BY f.formula, l.language) dat
  WHERE n_pragmatics > 1
  GROUP BY pragm_tags, n_pragmatics
  ORDER BY n_pragmatics DESC, (count(DISTINCT formula)) DESC;


  -- public.newview source

CREATE OR REPLACE VIEW public.app
AS 
select

df.formula as form,
l."language" as lang,
array_agg(vars.consts),
array_agg(c_glossed),
array_agg(vars.sem),

with
(select

v.variation_id as variation_id
array_agg(c.constituent) as consts,
array_agg(c.glossed) as c_glossed,
array_agg(e.example) as ex,
array_agg(e."translation") as ex_en,
p.pragmatics as pragm,
s.semantics as sem

from
frame2var fv
join variations v using(variation_id)
join examples e using(example_id)
join variation2constituents vc using(variation_id)
join constituents c USINg(constituent_id)

group by v.variation_id)


from frame2var fv 
join dat using(variation_id)
join frames fr USING(frame_id)
join pragmatics p USING(pragmatics_id)
join semantics s USING(semantics_id)
join formulas df USING(formula_id)
join languages l USING(language_id)

group by df.formula, l."language", vars.consts, c_lossed, p.pragmatics, s.semantics, e.example, e."translation") vars



;