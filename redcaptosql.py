"""
Code to support creating an SQL database from the various Charmed RedCAP
databases
"""

from datetime import datetime
from requests import post


class Study:
    """
    Class object to extract data from a study specific RedCAP database
    """
    def __init__(self,
                 study,
                 token,
                 pdevent="personal_details_arm_1",
                 idfield="pd_02_studyid",
                 visdatefield="vis_01_dt",
                 chid="pd_03_charmedid",
                 recid="record_id"):
        """
        Object for adding studies to the SQL database.

        study = name of the study
        token = API token for the study specific RedCAP database
        pdevent = personal details event in RedCAP
        visdatafield = visit date field in RedCAP
        """
        # RedCAP self.url
        self.url = 'https://redcap.camide.cam.ac.uk/api/'

        # set token
        self.token = token

        # set study name
        self.study = study

        # set persons details event
        self.pdevent = pdevent

        # set ID field
        self.idfield = idfield

        # set visit date field
        self.visdatefield = visdatefield

        # set charmed ID
        self.chid = chid

        # set record ID
        self.recid = recid

    def iddict(self):
        """
        Add ID and participants to the idtable in the SQL database
        """

        pl_study = {'token': self.token,
                    'content': 'record',
                    'action': 'export',
                    'format': 'json',
                    'type': 'flat',
                    'csvDelimiter': '',
                    'fields[0]': self.idfield,
                    'fields[1]': self.chid,
                    'events[0]': self.pdevent,
                    'rawOrLabel': 'raw',
                    'rawOrLabelHeaders': 'raw',
                    'exportCheckboxLabel': 'false',
                    'exportSurveyFields': 'false',
                    'exportDataAccessGroups': 'false',
                    'returnFormat': 'json'}
        req_study = post(self.url,
                         data=pl_study,
                         timeout=60)
        return {record[self.chid]:
                record[self.idfield]
                for record in req_study.json()
                if record[self.chid]
                and record[self.idfield]}

    def visit(self):
        """
        Get study visits
        """
        pl_visit = {'token': self.token,
                    'content': 'record',
                    'action': 'export',
                    'format': 'json',
                    'type': 'flat',
                    'csvDelimiter': '',
                    'fields[0]': self.recid,
                    'fields[1]': self.idfield,
                    'fields[2]': self.chid,
                    'fields[3]': self.visdatefield,
                    'events[0]': self.pdevent,
                    'events[1]': 'visit_arm_1',
                    'rawOrLabel': 'raw',
                    'rawOrLabelHeaders': 'raw',
                    'exportCheckboxLabel': 'false',
                    'exportSurveyFields': 'false',
                    'exportDataAccessGroups': 'false',
                    'returnFormat': 'json'}

        # make adjustments for PIPPIN
        if self.study == 'pippin':
            pl_visit['events[1]'] = 'pippin_1_arm_1'
            pl_visit['events[2]'] = 'pippin_2_arm_1'
            pl_visit['events[3]'] = 'pippin_3_arm_1'

        # make adjustments for IMPRINT
        if self.study == "imprint":
            pl_visit['events[0]'] = "baseline_arm_1"
            pl_visit['events[1]'] = '18_month_follow_up_arm_1'
            pl_visit['fields[3]'] = 'medical_history_date'

        # make adjustments for CHARMED
        if self.study == "charmed":
            pl_visit['fields[1]'] = self.visdatefield
            pl_visit['events[1]'] = "visits_arm_1"
            del pl_visit["fields[2]"]
            del pl_visit["fields[3]"]

        req_visit = post(self.url,
                         data=pl_visit,
                         timeout=60)

        if self.study == "charmed":
            outlist = [(record["id_01"],
                        record["vis_01_dt"])
                       for record in req_visit.json()
                       if record["vis_01_dt"]]

        elif req_visit.json() and req_visit.status_code == 200:
            idlookup = {record[self.recid]: record[self.chid]
                        for record in req_visit.json()
                        if record[self.chid]
                        and record[self.idfield]}

            outlist = [(idlookup[record[self.recid]],
                        record[self.visdatefield])
                       for record in req_visit.json()
                       if record[self.visdatefield]
                       and record[self.recid] in idlookup]

        else:
            outlist = []

        return outlist

    def mri(self):
        """
        Adds MRI sessions to visit dates.
        """
        pl_mri = {'token': self.token,
                  'content': 'record',
                  'action': 'export',
                  'format': 'json',
                  'type': 'flat',
                  'csvDelimiter': '',
                  'fields[0]': self.recid,
                  'fields[1]': self.idfield,
                  'fields[2]': self.chid,
                  'forms[0]': 'mri',
                  'events[0]': self.pdevent,
                  'events[1]': 'mri_arm_1',
                  'rawOrLabel': 'raw',
                  'rawOrLabelHeaders': 'raw',
                  'exportCheckboxLabel': 'false',
                  'exportSurveyFields': 'false',
                  'exportDataAccessGroups': 'false',
                  'returnFormat': 'json'}

        # corrections for Charmed
        if self.study == "charmed":
            pl_mri["forms[0]"] = "mri_data"
            del pl_mri["fields[1]"]
            del pl_mri["fields[2]"]

        req_mri = post(self.url,
                       data=pl_mri,
                       timeout=60)
        print("MRI HTTP Status: " + str(req_mri.status_code))

        # correct for NTAD and SHINE
        mridate = "mridt_acqdt"
        mrisacnnertype = "mridt_manufmodel"

        # only include studies with MRI tables
        if req_mri.status_code == 200:
            # create a dictionary of record IDs and study IDs
            # exclude if no study ID or Charmed ID
            if self.study == "charmed":
                outlist = [(record["id_01"],
                            record[mridate].split()[0],
                            record["mridt_prcppsdesc"],
                            record[mrisacnnertype],
                            record["mridt_magfieldstr"])
                           for record in req_mri.json()
                           if record[mridate]]

            else:
                stiddict = {record[self.recid]: record[self.chid]
                            for record in req_mri.json()
                            if record[self.idfield]
                            and record[self.chid]}

                # create a list of tuples of ID, MRI date, and scanner details
                if self.study in ("ntad", "shine"):
                    mridate = "mri_01_dt"
                    mrisacnnertype = "scanner_name_mri"
                    if self.study == "ntad":
                        protocol = "p00457"
                    if self.study == "shine":
                        protocol = "p00588"

                    outlist = [(stiddict[record[self.recid]],
                                record[mridate].split()[0],
                                protocol,
                                record[mrisacnnertype],
                                "3")
                               for record in req_mri.json()
                               if record[mridate]
                               and record[self.recid] in stiddict]
                else:
                    outlist = [(stiddict[record[self.recid]],
                                record[mridate].split()[0],
                                record["mridt_prcppsdesc"],
                                record[mrisacnnertype],
                                record["mridt_magfieldstr"])
                               for record in req_mri.json()
                               if record[mridate]
                               and record[self.recid] in stiddict]
        else:
            outlist = []

        return outlist

    def pet(self, ligand):
        """
        Add PET sessions. Preferentially adds to visits within 6 months with
        MRI. The ligand determines which radiotracer to search for.
        """
        # query to search for radiotracer
        tracerdict = {"AV1451": "0",
                      "PK11195": "10",
                      "Altanserin": "20",
                      "Amyvid": "30",
                      "BCPP-EF": "150",
                      "Fallypride": "40",
                      "Florbetaben": "170",
                      "FLT": "50",
                      "Flumazenil": "60",
                      "H2O": "70",
                      "MHED": "80",
                      "MTO": "90",
                      "NET": "100",
                      "PBr3": "110",
                      "PiB": "120",
                      "Raclopride": "130",
                      "SA4503": "160",
                      "UCB-J": "140"}

        pl_pet = {'token': self.token,
                  'content': 'record',
                  'action': 'export',
                  'format': 'json',
                  'type': 'flat',
                  'csvDelimiter': '',
                  'fields[0]': self.recid,
                  'fields[1]': self.idfield,
                  'fields[2]': self.chid,
                  'forms[0]': 'pet_imaging',
                  'events[0]': self.pdevent,
                  'events[1]': 'pet_arm_1',
                  'rawOrLabel': 'raw',
                  'rawOrLabelHeaders': 'raw',
                  'exportCheckboxLabel': 'false',
                  'exportSurveyFields': 'false',
                  'exportDataAccessGroups': 'false',
                  'returnFormat': 'json'}
        req_pet = post(self.url,
                       data=pl_pet,
                       timeout=60)
        print(ligand + ' HTTP Status: ' + str(req_pet.status_code))

        # only include studies with PET tables
        if req_pet.status_code == 200:
            # create a dictionary of record IDs and study IDs
            # exclude if no study ID or Charmed ID
            stiddict = {record[self.recid]: record[self.chid]
                        for record in req_pet.json()
                        if record[self.idfield]
                        and record[self.chid]}

            # create a list of tuples of ID, QMINC no and PET date
            outlist = [(stiddict[record[self.recid]],
                        record["pet_01_dt"].split()[0])
                       for record in req_pet.json()
                       if record["pet_01_dt"]
                       and record["pet_07_mode___" +
                                  tracerdict[ligand]] == "1"
                       and record[self.recid] in stiddict]

        else:
            outlist = []

        return outlist

    def bloods(self):
        """
        Add bloods sessions.
        """
        pl_bloods = {'token': self.token,
                     'content': 'record',
                     'action': 'export',
                     'format': 'json',
                     'type': 'flat',
                     'csvDelimiter': '',
                     'fields[0]': self.recid,
                     'fields[1]': self.idfield,
                     'fields[2]': self.chid,
                     'forms[0]': 'blood_samples',
                     'events[0]': self.pdevent,
                     'events[1]': 'bloods_arm_1',
                     'rawOrLabel': 'raw',
                     'rawOrLabelHeaders': 'raw',
                     'exportCheckboxLabel': 'false',
                     'exportSurveyFields': 'false',
                     'exportDataAccessGroups': 'false',
                     'returnFormat': 'json'}
        req_bloods = post(self.url,
                          data=pl_bloods,
                          timeout=60)
        print("Bloods HTTP Status: " + str(req_bloods.status_code))

        bloodsdatefield = "blo_01_sampledt"

        # only include studies with blood tables
        if req_bloods.status_code == 200:
            # create a dictionary of record IDs and study IDs
            # exclude if no study ID or Charmed ID
            stiddict = {record[self.recid]: record[self.chid]
                        for record in req_bloods.json()
                        if record[self.idfield]
                        and record[self.chid]}

            # create a list of tuples of ID and blood test date
            outlist = [(stiddict[record[self.recid]],
                        record[bloodsdatefield].split()[0])
                       for record in req_bloods.json()
                       if record[bloodsdatefield]
                       and record[self.recid] in stiddict]

        else:
            outlist = []

        return outlist


    def test(self, test):
        """
        Get data for a specific test
        """
        pl_test = {'token': self.token,
                   'content': 'record',
                   'action': 'export',
                   'format': 'json',
                   'type': 'flat',
                   'csvDelimiter': '',
                   'fields[0]': self.recid,
                   'fields[1]': self.idfield,
                   'fields[2]': self.chid,
                   'fields[3]': self.visdatefield,
                   'forms[0]': test,
                   'events[0]': self.pdevent,
                   'events[1]': 'visit_arm_1',
                   'rawOrLabel': 'raw',
                   'rawOrLabelHeaders': 'raw',
                   'exportCheckboxLabel': 'false',
                   'exportSurveyFields': 'false',
                   'exportDataAccessGroups': 'false',
                   'returnFormat': 'json'}

        # make adjustments for PIPPIN
        if self.study == 'pippin':
            pl_test['events[1]'] = 'pippin_1_arm_1'
            pl_test['events[2]'] = 'pippin_2_arm_1'
            pl_test['events[3]'] = 'pippin_3_arm_1'

        # make adjustments for IMPRINT
        if self.study == "imprint":
            pl_test['events[0]'] = "baseline_arm_1"
            pl_test['events[1]'] = '18_month_follow_up_arm_1'

        req_test = post(self.url,
                        pl_test,
                        timeout=60)
        print('HTTP Status: ' + str(req_test.status_code))
        if req_test.status_code == 200:
            # create a dictionary of record IDs and study IDs
            # exclude if no study ID or Charmed ID
            stiddict = {record[self.recid]: record[self.chid]
                        for record in req_test.json()
                        if record[self.idfield]
                        and record[self.chid]}

            # create a list of tuples of ID and test date
            outlist = {(stiddict[record[self.recid]],
                        record[self.visdatefield].split()[0]):
                       record
                       for record in req_test.json()
                       if record[self.visdatefield]
                       and record[self.recid] in stiddict
                       and record[test +
                       "_complete"] == '2'}

            for record in outlist:
                outlist[record]["charmed"] = \
                    stiddict[outlist[record][self.recid]]
                outlist[record] = {field[0]: field[1]
                                   for field in outlist[record].items()
                                   if not field[0].startswith("pd_")
                                   and not field[0].startswith("redcap_")
                                   and not field[0] == self.recid
                                   and not field[0] == self.visdatefield}

        else:
            outlist = {}

        return outlist


def comparedates(visit, sessionsdict, datefield):
    """
    Function to find the correct corresponding visit by comparing dates
    visit is a tuple with ("Charmed ID", "date")
    """
    # variable for comparing visit dates
    visitdatefield = "visitdate"

    # list of visits for the participant
    visit_subj = {session[0]: session[1]
                  for session in sessionsdict.items()
                  if session[1]["charmed"] == visit[0]}

    # identify closest visit to other visits
    nearvisit = {sessid[0]: abs(datetime.strptime(sessid[1]["visitdate"],
                                                  "%Y-%m-%d") -
                                datetime.strptime(visit[1],
                                                  "%Y-%m-%d"))
                 for sessid in visit_subj.items()
                 if sessid[1]['visitdate']}

    # if within 6 months
    nearvisit = {session[0]: session[1] for session in nearvisit.items()
                 if session[1].days < 365/2}

    if datefield != 'mridate':
        # filter by MRI sessions if they exist
        nearvisit_mri = {session[0]: session[1]
                         for session in nearvisit.items()
                         if sessionsdict[session[0]]["mridate"]}

        if nearvisit_mri:
            nearvisit = nearvisit_mri

            visitdatefield = "mridate"

    for visdatef in ["mridate",
                     "av1451date",
                     "ucbjdate",
                     "pk11195date",
                     "florbetabendate",
                     "pibdate",
                     "amyviddate",
                     "bcppefdate",
                     "sa4503date",
                     "megdate",
                     "bloodsdate",
                     "csfdate"]:
        if not nearvisit:
            nearvisit_vis = {sessid[0]:
                             abs(datetime.strptime(sessid[1][visdatef],
                                                   "%Y-%m-%d") -
                                 datetime.strptime(visit[1],
                                                   "%Y-%m-%d"))
                             for sessid in visit_subj.items()
                             if sessionsdict[sessid[0]][visdatef]}

            # limit to 6 months
            nearvisit_vis = {session[0]: session[1]
                             for session
                             in nearvisit_vis.items()
                             if session[1].days < 365/2}
            if nearvisit_vis:
                nearvisit = nearvisit_vis

            visitdatefield = visdatef

    if nearvisit:
        choice = [session[0]
                  for session in nearvisit.items()
                  if session[1].days == min(sessdate.days
                                            for sessdate
                                            in nearvisit.values())][0]
        choice = (choice, sessionsdict[choice])

        # assign date
        # if a visit has already been assigned, check if this is closer
        if choice[1][datefield]:
            newdatediff = abs(datetime.strptime(visit[1],
                                                "%Y-%m-%d") -
                              datetime.strptime(choice[1][visitdatefield],
                                                "%Y-%m-%d"))
            olddatediff = abs(datetime.strptime(choice[1][datefield],
                                                "%Y-%m-%d") -
                              datetime.strptime(choice[1][visitdatefield],
                                                "%Y-%m-%d"))
            if newdatediff < olddatediff:
                choice[1][datefield] = visit[1]

            else:
                visitn = max(sessionsdict.keys()) + 1
                choice = (visitn, {"charmed": visit[0],
                                   "sessid": visitn,
                                   "visitdate": None,
                                   "bloodsdate": None,
                                   "csfdate": None,
                                   "mridate": None,
                                   "av1451date": None,
                                   "ucbjdate": None,
                                   "pk11195date": None,
                                   "florbetabendate": None,
                                   "pibdate": None,
                                   "amyviddate": None,
                                   "bcppefdate": None,
                                   "sa4503date": None,
                                   "megid": None,
                                   "megdate": None})
                choice[1][datefield] = visit[1]
        else:
            choice[1][datefield] = visit[1]
            choice[1]["charmed"] = visit[0]

    else:
        visitn = max(sessionsdict.keys()) + 1
        choice = (visitn, {"charmed": visit[0],
                           "sessid": visitn,
                           "visitdate": None,
                           "bloodsdate": None,
                           "csfdate": None,
                           "mridate": None,
                           "av1451date": None,
                           "ucbjdate": None,
                           "pk11195date": None,
                           "florbetabendate": None,
                           "pibdate": None,
                           "amyviddate": None,
                           "bcppefdate": None,
                           "sa4503date": None,
                           "megid": None,
                           "megdate": None})
        choice[1][datefield] = visit[1]

    return choice


def getidlist(record, iddict):
    """
    Get a list of ID and ID numbers for studies in a record
    """
    # start output directory
    idoutdict = {"charmed": record["id_01"]}

    idoutdict = idoutdict | {studyname: None
                             for studyname in iddict.values()
                             if studyname}

    for znum in range(10):
        studyfield = "cons_" + \
                      str((znum*3) + 1).zfill(2) + \
                      "_study_" + str(znum+1)
        if record[studyfield]:
            studyid = record["cons_" +
                             str((znum*3) + 3).zfill(2) +
                             "_no_" + str(znum+1)]
            idoutdict[iddict[record[studyfield].split(" - ")[0]]] = studyid

    return idoutdict


def checktypes(column):
    """
    Function to check column types
    """
    # remove NAs
    column = [val for val in column if val]

    # check if all are numbers
    try:
        if all(float(val).is_integer() for val in column):
            return "INTEGER"
        return "NUMERIC"
    except ValueError:
        try:
            column = [datetime.strptime(val, "%Y-%m-%d")
                      for val in column]
            return "DATE"
        except (TypeError, ValueError):
            return "TEXT"
