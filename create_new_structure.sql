CREATE TABLE IF NOT EXISTS public.syntax (
	syntax_id integer NOT NULL GENERATED ALWAYS AS IDENTITY,
	syntax varchar NOT NULL,
	CONSTRAINT syntax_pk PRIMARY KEY (syntax_id)
);

CREATE TABLE IF NOT EXISTS public.cx_syntax (
	cx_syntax_id integer NOT NULL GENERATED ALWAYS AS IDENTITY,
	cx_syntax varchar NOT NULL,
	CONSTRAINT cx_syntax_pk PRIMARY KEY (cx_syntax_id)
);

CREATE TABLE IF NOT EXISTS public.cx_semantics (
	cx_semantics_id int4 NOT NULL,
	cx_semantics varchar NOT NULL,
	CONSTRAINT cx_semantics_pk PRIMARY KEY (cx_semantics_id)
);


CREATE TABLE IF NOT EXISTS public.examples (
	example_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	example varchar NOT NULL,
	"translation" varchar NULL,
	"source" varchar NULL,
	CONSTRAINT examples_pk PRIMARY KEY (example_id)
);


CREATE TABLE IF NOT EXISTS public.gloss_types (
	gloss_type_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	gloss_type varchar,
	CONSTRAINT gloss_types_pk PRIMARY KEY (gloss_type_id),
	CONSTRAINT gloss_types_un UNIQUE (gloss_type)
);


CREATE TABLE IF NOT EXISTS public.inner_structure_subtypes (
	inner_structure_subtype_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	inner_structure_subtype varchar(100) NOT NULL,
	CONSTRAINT inner_structure_subtypes_inner_structure_subtype_key UNIQUE (inner_structure_subtype),
	CONSTRAINT inner_structure_subtypes_pkey PRIMARY KEY (inner_structure_subtype_id)
);





CREATE TABLE IF NOT EXISTS public.inner_structure_types (
	inner_structure_type_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	inner_structure_type varchar(100) NOT NULL,
	CONSTRAINT inner_structure_types_inner_structure_type_key UNIQUE (inner_structure_type),
	CONSTRAINT inner_structure_types_pkey PRIMARY KEY (inner_structure_type_id)
);





CREATE TABLE IF NOT EXISTS public.intonations (
	intonation_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	intonation varchar(50) NOT NULL,
	CONSTRAINT intonations_intonation_key UNIQUE (intonation),
	CONSTRAINT intonations_pkey PRIMARY KEY (intonation_id)
);





CREATE TABLE IF NOT EXISTS public.languages (
	language_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	"language" bpchar(2) NOT NULL,
	CONSTRAINT languages_language_key UNIQUE (language),
	CONSTRAINT languages_pkey PRIMARY KEY (language_id)
);





CREATE TABLE IF NOT EXISTS public.lemmas (
	lemma_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	lemma varchar(50) NOT NULL,
	CONSTRAINT lemmas_lemma_key UNIQUE (lemma),
	CONSTRAINT lemmas_pkey PRIMARY KEY (lemma_id)
);





CREATE TABLE IF NOT EXISTS public.pragmatics (
	pragmatics_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	pragmatics varchar NULL,
	CONSTRAINT pragmatics_pkey PRIMARY KEY (pragmatics_id)
);





CREATE TABLE IF NOT EXISTS public.literature (
	reference_id int4 NOT NULL,
	reference varchar NOT NULL,
	author varchar NULL,
	"year" int4 NULL,
	doi varchar NULL,
	CONSTRAINT references_pk PRIMARY KEY (reference_id)
);





CREATE TABLE IF NOT EXISTS public.semantics (
	semantics_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	semantics varchar(100) NOT NULL,
	CONSTRAINT additional_semantics_additional_sem_key UNIQUE (semantics),
	CONSTRAINT additional_semantics_pkey PRIMARY KEY (semantics_id)
);





CREATE TABLE IF NOT EXISTS public.speech_acts (
	speech_act_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	speech_act varchar(50) NOT NULL,
	CONSTRAINT speech_acts_pkey PRIMARY KEY (speech_act_id),
	CONSTRAINT speech_acts_speech_act_key UNIQUE (speech_act)
);


CREATE TABLE IF NOT EXISTS public.frames (
	frame_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	pragmatics_id int4 NULL,
	semantics_id int4 NULL,
	speech_act1_id int4 NULL,
	speech_act2_id int4 NULL,
	structure int4 NULL,
	CONSTRAINT frames_key PRIMARY KEY (frame_id),
	CONSTRAINT pragmatics_fk FOREIGN KEY (pragmatics_id) REFERENCES public.pragmatics(pragmatics_id),
	CONSTRAINT semantics_fk FOREIGN KEY (semantics_id) REFERENCES public.semantics(semantics_id)
);





CREATE TABLE IF NOT EXISTS public.gloss_class (
	gloss_class_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	gloss_class varchar,
	CONSTRAINT gloss_class_pk PRIMARY KEY (gloss_class_id),
	CONSTRAINT gloss_class_un UNIQUE (gloss_class)
);





CREATE TABLE IF NOT EXISTS public.glosses (
	gloss_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	gloss varchar(50) NOT NULL,
	gloss_type_id int4 NULL,
	gloss_class_id int4 NULL,
	CONSTRAINT glosses_gloss_key UNIQUE (gloss, gloss_type_id),
	CONSTRAINT glosses_pkey PRIMARY KEY (gloss_id),
	CONSTRAINT gloss_class_fk FOREIGN KEY (gloss_class_id) REFERENCES public.gloss_class(gloss_class_id),
	CONSTRAINT gloss_type_fk FOREIGN KEY (gloss_type_id) REFERENCES public.gloss_types(gloss_type_id)
);

CREATE TABLE IF NOT EXISTS public.glossing (
	glossing_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	gloss_id int4 NOT NULL,
	marker varchar NOT NULL,
	language_id int4 NOT NULL,
	marker_id int4 NULL,
	CONSTRAINT glossing_pk PRIMARY KEY (glossing_id),
	CONSTRAINT glossing_un UNIQUE (marker_id, marker, gloss_id, language_id),
	CONSTRAINT gloss_fk FOREIGN KEY (gloss_id) REFERENCES public.glosses(gloss_id),
	CONSTRAINT language_fk FOREIGN KEY (language_id) REFERENCES public.languages(language_id)
);


CREATE TABLE IF NOT EXISTS public.constituents (
	constituent_id integer NOT NULL GENERATED ALWAYS AS IDENTITY,
	constituent varchar NOT NULL,
	glossed varchar NULL,
	language_id integer,
	lemma_id integer,
	CONSTRAINT constituents_pk PRIMARY KEY (constituent_id),
	CONSTRAINT constituents_un UNIQUE (constituent, language_id, lemma_id),
	CONSTRAINT language_fk FOREIGN KEY (language_id) REFERENCES public.languages(language_id),
	CONSTRAINT lemma_fk FOREIGN KEY (lemma_id) REFERENCES public.lemmas(lemma_id)
);


CREATE TABLE IF NOT EXISTS public.constituents2glossing (
	constituents2glossing_id integer NOT NULL GENERATED ALWAYS AS IDENTITY,
	glossing_id int4 NOT NULL,
	constituent_id integer NOT NULL,
	CONSTRAINT constituents2glossing_pk PRIMARY KEY (constituents2glossing_id),
	CONSTRAINT constituents2glossing_un UNIQUE (glossing_id, constituent_id),
	CONSTRAINT glossing_fk FOREIGN KEY (glossing_id) REFERENCES public.glossing(glossing_id),
	CONSTRAINT constituent_fk FOREIGN KEY (constituent_id) REFERENCES public.constituents(constituent_id)
);

CREATE TABLE IF NOT EXISTS public.inner_structure (
	inner_structure_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	inner_structure_type_id int4 NOT NULL,
	inner_structure_subtype_id int4 NULL,
	CONSTRAINT inner_structure_pk PRIMARY KEY (inner_structure_id),
	CONSTRAINT inner_structure_subtype_fk FOREIGN KEY (inner_structure_subtype_id) REFERENCES public.inner_structure_subtypes(inner_structure_subtype_id),
	CONSTRAINT inner_structure_type_fk FOREIGN KEY (inner_structure_type_id) REFERENCES public.inner_structure_types(inner_structure_type_id)
);





CREATE TABLE IF NOT EXISTS public.source_constructions (
	construction_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	construction varchar(200) NOT NULL,
	cx_syntax_id integer NULL,
	cx_semantics_id integer NULL,
	language_id integer NOT NULL,
	cx_intonation varchar(100) NULL,

	CONSTRAINT source_constructions_construction_key UNIQUE (construction),
	CONSTRAINT source_constructions_pkey PRIMARY KEY (construction_id),
	CONSTRAINT cx_semantics_fk FOREIGN KEY (cx_semantics_id) REFERENCES public.cx_semantics(cx_semantics_id),
	CONSTRAINT cx_lang FOREIGN KEY (language_id) REFERENCES public.languages(language_id),
	CONSTRAINT cx_syntax_fk FOREIGN KEY (cx_syntax_id) REFERENCES public.cx_syntax(cx_syntax_id)
);





CREATE TABLE IF NOT EXISTS public.formulas (
	formula_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	formula varchar(50) NOT NULL,
	language_id int4 NULL,
	construction_id int4 NULL,
	CONSTRAINT formulas_pkey PRIMARY KEY (formula_id),
	CONSTRAINT formulas_un UNIQUE (formula, language_id, construction_id),
	CONSTRAINT construction_fk FOREIGN KEY (construction_id) REFERENCES public.source_constructions(construction_id),
	CONSTRAINT fk_language FOREIGN KEY (language_id) REFERENCES public.languages(language_id) ON DELETE SET NULL
);





CREATE TABLE IF NOT EXISTS public.frame2speech_acts (
	frame2speech_act_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	frame_id int4 NOT NULL,
	speech_act_1_id int4 NULL,
	speech_act_id int4 NULL,
	CONSTRAINT frame2speech_acts_pkey PRIMARY KEY (frame2speech_act_id),
	CONSTRAINT fk_frame FOREIGN KEY (frame_id) REFERENCES public.frames(frame_id) ON DELETE CASCADE,
	CONSTRAINT fk_speech_act FOREIGN KEY (speech_act_id) REFERENCES public.speech_acts(speech_act_id) ON DELETE SET NULL,
	CONSTRAINT fk_speech_act_1 FOREIGN KEY (speech_act_1_id) REFERENCES public.speech_acts(speech_act_id) ON DELETE SET NULL
);





CREATE TABLE IF NOT EXISTS public.variations (
	variation_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	variation varchar(100) NOT NULL,
	formula_id int4 NOT NULL,
	main bool NOT NULL,
	syntax_id integer NULL,
	intonation_id int4 NULL,
	CONSTRAINT variations_pkey PRIMARY KEY (variation_id),
	CONSTRAINT variations_variation_key UNIQUE (variation, formula_id, intonation_id),
	CONSTRAINT fk_formula FOREIGN KEY (formula_id) REFERENCES public.formulas(formula_id),
	CONSTRAINT fk_syntax FOREIGN KEY (syntax_id) REFERENCES public.syntax(syntax_id),
	CONSTRAINT intionation_fk FOREIGN KEY (intonation_id) REFERENCES public.intonations(intonation_id)
);





CREATE TABLE IF NOT EXISTS public.formula2inner_structure (
	formula2inner_structure_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	formula_id int4 NOT NULL,
	inner_structure_id int4 NOT NULL,
	CONSTRAINT formula2inner_structure_pk PRIMARY KEY (formula2inner_structure_id),
	CONSTRAINT formula2inner_structure_un UNIQUE (formula_id, inner_structure_id),
	CONSTRAINT formula_fk FOREIGN KEY (formula_id) REFERENCES public.formulas(formula_id),
	CONSTRAINT inner_structure_fk FOREIGN KEY (inner_structure_id) REFERENCES public.inner_structure(inner_structure_id)
);


-- DROP TABLE public.frame2var;


CREATE TABLE IF NOT EXISTS public.frame2var (
	frame2var_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	frame_id int4 NOT NULL,
	variation_id int4 NULL,
	example_id int4 NULL,
	commentary varchar NULL,
	reference_id int4 NULL,
	CONSTRAINT real2var_pk PRIMARY KEY (frame2var_id),
	CONSTRAINT example_fk FOREIGN KEY (example_id) REFERENCES public.examples(example_id),
	CONSTRAINT frame_fk FOREIGN KEY (frame_id) REFERENCES public.frames(frame_id),
	CONSTRAINT reference_fk FOREIGN KEY (reference_id) REFERENCES public.literature(reference_id),
	CONSTRAINT variation_fk FOREIGN KEY (variation_id) REFERENCES public.variations(variation_id)
);


-- DROP TABLE public.variation2gloss;

CREATE TABLE IF NOT EXISTS public.variation2constituents (
	variation2constituent_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	variation_id int4 NOT NULL,
	constituent_id int4 NULL,
	CONSTRAINT variation2constituents_pkey PRIMARY KEY (variation2constituent_id),
	CONSTRAINT variation2constituents_un UNIQUE (variation_id, constituent_id),
	CONSTRAINT fk_gloss FOREIGN KEY (constituent_id) REFERENCES public.constituents(constituent_id) ON DELETE SET NULL,
	CONSTRAINT fk_variation FOREIGN KEY (variation_id) REFERENCES public.variations(variation_id) ON DELETE CASCADE
);



