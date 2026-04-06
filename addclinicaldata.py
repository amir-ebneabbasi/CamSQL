"""
Add cognitive and clinical data to the SQL database
"""

import sqlite3
import pandas as pd
import numpy as np
from redcaptosql import redcaptosql
from requests import post
from itertools import chain


def uploadtest(formname,
               tokdict,
               sessionslist,
               sqldb="charmed.db"):
    """
    Function to upload test data to SQL
    """
    # filter by completed tests
    completevals = {'ace_revised': "acer_37_acertot",
                    'aes_carer': "aes_19_motiv_carer",
                    'aes_clinician': "aes_19_motiv_clinicn",
                    'aes_self_rated': "aes_19_motiv_selfr",
                    'aes': "aes_19_motiv",
                    'bads': 'badsks_rawscr',
                    'badls': "bads_27_transp",
                    'barratt_impulsiveness': "bis_futureoriented",
                    'bisbas': "biba_24_mistake",
                    'bdi': "bdi_01_total",
                    'cai': "cai_24_meetfriends",
                    'cbdrs': "cbdrs_31_find",
                    'cbi': "cbi_102_totscr",
                    'cbi_revised': "cbir_67_total",
                    'cbi_revised_patient': "cbir_48_intrst_survey_pt",
                    'cdr': "cdr_16_boxsum",
                    'cbs': "cbs_q22",
                    'digit_span': 'dsp_12_totscr',
                    'fab': "fab_14_fabtot",
                    'frs': "frs_37_bedbnd",
                    'gds': "gdss_15",
                    'hads_gds': "hg_13_gadscrnum",
                    "hallucinations_fop": "vh_part3_total",
                    "hayling": 'ha_total_time',
                    'ineco_frontal_screening': "ifs_17_totscr",
                    'ineco_frontal_screening': "ifs_17_totscr",
                    'letter_fluency': 'letf_09_pcorr',
                    'moca': "moca_27_total",
                    'npi_new': "npi_61_steabs_v2",
                    'npi': "npi_22_npitot",
                    'one_day_fluctuation_assessment_scale':
                    "odf_17_1daytot",
                    'pal': "pal_07_errtot",
                    'parkinsons_sleep_scale': "pdss_unexpectedsleep",
                    'psp_rating_scale': "psp_42_tot",
                    'pyramids_and_palm_trees': "ppt_10_recog",
                    'ravlt': "ravlt_learning",
                    'reading_the_eyes': 'rte_scr',
                    'seadl': "seadl_01_rating",
                    'srt': "srt_11_perctrial",
                    "trails_a": "tra_93_atimenum",
                    "trails_b": "tra_180_berror",
                    'updrs': "upd_59_updrst",
                    "pippin_clinical_data": 'clex_total'}

    # print form name
    print("\n" + formname)
    testall = {}
    for study in tokdict.items():
        print(study[0])
        if study[0] == "imprint":
            studyobj = redcaptosql.Study(study[0],
                                         token=study[1]['token'],
                                         idfield=study[1]['idfield'],
                                         pdevent="baseline_arm_1",
                                         visdatefield='medical_history_date')
        else:
            studyobj = redcaptosql.Study(study[0],
                                         token=study[1]['token'],
                                         idfield=study[1]['idfield'])

        test = studyobj.test(formname)

        testall = testall | test

    testall = {visit[0]: visit[1] for visit in testall.items()
               if completevals[formname] in visit[1].keys()}
    testall = {visit[0]: visit[1] for visit in testall.items()
               if visit[1][completevals[formname]]}

    # add charmed data
    # get charmed token
    with open("token_charmed.txt", encoding="UTF-8") as tokopen:
        token = tokopen.readlines()[0].rstrip("\n")

    # set url
    url = 'https://redcap.camide.cam.ac.uk/api/'

    # get charmed data
    print("Charmed")
    pl_test = {'token': token,
               'content': 'record',
               'action': 'export',
               'format': 'json',
               'type': 'flat',
               'csvDelimiter': '',
               'fields[0]': 'id_01',
               'fields[3]': 'vis_01_dt',
               'forms[0]': formname,
               'events[0]': 'admin_arm_1',
               'events[1]': 'visits_arm_1',
               'rawOrLabel': 'raw',
               'rawOrLabelHeaders': 'raw',
               'exportCheckboxLabel': 'false',
               'exportSurveyFields': 'false',
               'exportDataAccessGroups': 'false',
               'returnFormat': 'json'}
    req_chtest = post(url,
                      pl_test)
    print('HTTP Status: ' + str(req_chtest.status_code))

    if req_chtest.status_code == 200:
        data_chtest = {(record["id_01"],
                        record["vis_01_dt"].split()[0]):
                       {field[0]: field[1]
                        for field in record.items()
                        if not field[0].startswith("pd_")
                        and not field[0].startswith("redcap_")
                        and not field[0].endswith("__ni")
                        and not field[0] == "record_id"
                        and not field[0] == "vis_01_dt"
                        and not field[0] == "visits_clin_pkey"}
                       for record in req_chtest.json()
                       if record["vis_01_dt"]
                       if (record["id_01"],
                           record["vis_01_dt"].split()[0])
                       in sessionslist}

        data_chtest = {record[0]: record[1]
                       for record in data_chtest.items()
                       if record[1][completevals[formname]]}

        # make corrections to record for charmed ID and complete field
        for record in data_chtest:
            data_chtest[record]["charmed"] = record[0]
            del data_chtest[record]["id_01"]
            data_chtest[record][formname + "_complete"] = "2"

        # remove incomplete values and duplicates
        data_chtest = {visit[0]: visit[1] for visit in data_chtest.items()
                       if visit[1][completevals[formname]]
                       and not visit[0] in testall.keys()}

        # add to existing data from other studies
        testall = testall | data_chtest

    for testsession in testall:
        try:
            testall[testsession]["visits_clin_pkey"] = sessionslist[testsession]
        except KeyError as e:
            print("Key issue with sessions data " + str(e))

    # remove tests with no session ID
    testall = {visit:
               {key: val
                for key, val in record.items()
                if not key.endswith("_ts")
                and not key.endswith("_infnm")
                }
               for visit, record in testall.items()
               if 'visits_clin_pkey' in record.keys()}

    # merge sessions with the same session ID
    visits_clin_pkeylist = [visit["visits_clin_pkey"] for visit in testall.values()]
    multisesslist = list(set([sess for sess in visits_clin_pkeylist
                              if len([visits_clin_pkey for visits_clin_pkey in visits_clin_pkeylist
                                      if visits_clin_pkey == sess]) > 1]))

    for visits_clin_pkey in multisesslist:
        multidict = {visit[0]: visit[1]
                     for visit in testall.items()
                     if str(visit[1]['visits_clin_pkey']) == str(visits_clin_pkey)}

        # pick the first
        keylist = list(multidict.keys())
        keylist.sort()
        for datekey in keylist[1:]:
            del testall[datekey]

    # convert to pandas dataframe
    conn = sqlite3.connect(sqldb)
    cur = conn.cursor()

    # add prefix to form name to create the table name
    tablename = "clin_" + formname

    try:
        # get a list of all fields for all versions of the test
        headerlist = list(set(chain.from_iterable(testall[sub].keys()
                                                  for sub in testall)))
        headerlist.sort()
        cur.execute('DROP TABLE IF EXISTS ' + tablename)
        conn.commit()

        try:
            testalltable = pd.DataFrame.from_dict(testall,
                                                  orient='index')

            cur.execute(' '.join(["CREATE TABLE",
                                  tablename,
                                  "(" + tablename + "_pkey PRIMARY KEY,",
                                  "visits_clin_pkey TEXT,",
                                  "charmed TEXT,",
                                  ', '.join([' ' .join([item,
                                                        redcaptosql.checktypes(testalltable[item])])
                                             for item in headerlist
                                             if item not in ("visits_clin_pkey",
                                                             "charmed")]),
                                  ")"]))
            conn.commit()

            # convert blank spaces to NaN
            testalltable = testalltable.map(lambda x: np.nan
                                            if isinstance(x, str)
                                            and (x.isspace()
                                                 or not x)
                                            else x)

            # add primary key
            testalltable.insert(0, tablename + "_pkey",
                                range(len(testalltable)))

            # add to SQL
            conn = sqlite3.connect(sqldb)
            testalltable.to_sql(tablename,
                                conn,
                                if_exists="append",
                                index=False)
            conn.close()
        except sqlite3.OperationalError as err:
            print(err)
    except IndexError:
        print("Looks like there are no results for this test")


# import token list
with open("tokenlist.txt", encoding="UTF-8") as TOKOPEN:
    TOKDICT = {line.split("\t")[0]: {'idfield': line.split("\t")[1],
                                     'token': line.split("\t")[2].rstrip("\n")}
               for line in TOKOPEN.readlines()}

# set SQL location
SQLDB = "path/to/charmed.db"

# get a table of sessions and charmed IDs
CONN = sqlite3.connect(SQLDB)
CUR = CONN.cursor()
CUR.execute(" ".join(["SELECT",
                      ", ".join(["visits_clin_pkey",
                                 "charmed",
                                 "visits_clin_date"]),
                      "FROM visits_clin"]))

SESSIONSLIST = CUR.fetchall()
SESSIONSLIST = {(session[1], session[2]): session[0]
                for session in SESSIONSLIST}
CONN.close()

tests = [
    "ace_revised",
    "aes_carer",
    "aes_clinician",
    "aes_self_rated",
    "aes",
    "bads",
    "badls",
    "barratt_impulsiveness",
    "bdi",
    "cai",
    "cbdrs",
    "cbi_revised",
    "cbi_revised_patient",
    "cbs",
    "cdr",
    "digit_span",
    "fab",
    "frs",
    "gds",
    "hads_gds",
    "hallucinations_fop",
    "hayling",
    "ineco_frontal_screening",
    "letter_fluency",
    "moca",
    "npi_new",
    "one_day_fluctuation_assessment_scale",
    "pal",
    "parkinsons_sleep_scale",
    "pyramids_and_palm_trees",
    "psp_rating_scale",
    "ravlt",
    "reading_the_eyes",
    "seadl",
    "srt",
    "trails_a",
    "trails_b",
    "updrs",
    "pippin_clinical_data"
]

for test in tests:
    uploadtest(test, TOKDICT, SESSIONSLIST, SQLDB)