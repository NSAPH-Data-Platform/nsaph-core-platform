# Handling Medicaid data

## Introduction

[Centers for Medicare & Medicaid Services (CMS)](https://www.cms.gov/) 
provide 
[Medicaid Medicaid Analytic eXtract (MAX) data](https://www.cms.gov/Research-Statistics-Data-and-Systems/Computer-Data-and-Systems/MedicaidDataSourcesGenInfo/MAXGeneralInformation). 
The MAX data is export from 
[SAS](https://www.sas.com/en_us/software/stat.html) 
to CSV format, similar to Excel in its semantic. 
We refer to the original MAX data as RAW data.

The pipeline steps are being put into a framework and wrapped
as CWL workflows.

## Importing raw data   

The first step is to parse File transfer summary (FTS)
included with the data and generate YAML schema for
raw data. This is done by running module 
[medicaid](../src/python/nsaph/cms/medicaid.py).
                                                       
    pyhton -m nsaph.cms.medicaid

Once the schema is generated, the 
[Universal Data Loader](Datamodels.md) can import it
by running the following command:

    nohup python -u -m nsaph.data_model.model2 --domain cms -t ps --domain cms --incremental --data /data/incoming/rce/ci3_d_medicaid/original_data/cms_medicaid-max/data/  -t ps --pattern "**/maxdata_*_ps_*.csv*"  --threads 4 --page 1000 --log 100000 2>&1 > ps-2021-09-25--21-37.log&


## Processing data

## Schema

The root table for Medicaid is `beneficiaries` table (currently
called demographics)

    |--- beneficiaries
        |--- enrollments
            |--- eligibility
                |--- admissions


### Beneficiaries

Beneficiaries table is based on BENE_ID column in the raw data.
The [BENE_ID](https://resdac.org/cms-data/variables/encrypted-723-ccw-beneficiary-id) 
is a unique beneficiary identifier encrypted 
specifically to the researcher/Data Use Agreement (DUA). 
This identifier is unique to the Chronic Condition 
Data Warehouse (CCW) and protects the identity of the 
Medicaid and/or Medicare beneficiary.

However, BENE_ID are unreliable. About 7% of records are missing 
BENE_ID. There are records with conflicting information such
as different dates of birth, dates of death, sex and ethnicity
that have the same BENE_ID. Sometimes the date of birth differ 
by 80 (eighty) years.

[Official documentation](https://www2.ccwdata.org/documents/10280/19002246/ccw-max-user-guide.pdf)
states in the section _Assignment of a Beneficiary Identifier_:

>To construct the CCW BENE_ID, the CMS CCW team developed an internal cross-reference file
consisting of historical Medicaid and Medicare enrollment information using CMS data sources
such as the Enterprise Cross Reference (ECR) file. When a new MAX PS file is received, the
MSIS_ID, STATE_CD, SSN, DOB, Gender and other beneficiary identifying information is compared
against the historical enrollment file. If there is a single record in the historical enrollment file
that “best matches” the information in the MAX PS record, then the BENE_ID on that historical
record is assigned to the MAX PS record. If there is no match or no “best match” after CCW has
exhausted a stringent matching process, a null (or missing) BENE_ID is assigned to the MAX PS
record. For any given year, approximately 7% to 8% of MAX PS records have a BENE_ID that is
null. Once a BENE_ID is assigned to a MAX PS record for a particular year (with the exception of
those assigned to a null value), it will not change. When a new MAX PS file is received, CCW
attempts to reassign those with missing BENE_IDs.


                                          
'''flow
st=>start: Start
fts2yaml=>operation: Parse FTS and generate YAML Schema
ps2db=>operation: Import Patient Summary files
ben=>operation: Create Beneficiaries View 
e=>:end

st->fts2yaml->ps2db->ben->e
'''


'''
Medicaid Domain for NSAPH

Running this module will create/update data model for CMS data

https://github.com/NSAPH/data_model

Demographics:   /data/incoming/rce/ci3_d_medicaid/processed_data/cms_medicaid-max/data_cms_medicaid-max-demographics_patient
Description of columns:
https://gitlab-int.rc.fas.harvard.edu/rse/francesca_dominici/dominici_data_pipelines/-/blob/master/medicaid/1_create_demographics_data.R
Enrollments:    /data/incoming/rce/ci3_d_medicaid/processed_data/cms_medicaid-max/data_cms_medicaid-max-ps_patient-year
Description of columns:
https://gitlab-int.rc.fas.harvard.edu/rse/francesca_dominici/dominici_data_pipelines/-/blob/master/medicaid/2_process_enrollment_data.R
https://github.com/NSAPH/data_requests/tree/master/request_projects/dec2019_medicaid_platform_cvd
Admissions:     /data/incoming/rce/ci3_health_data/medicaid/cvd/1999_2012/desouza/data
Description of columns:
https://gitlab-int.rc.fas.harvard.edu/rse/francesca_dominici/dominici_data_pipelines/-/blob/master/medicaid/code/2_create_cvd_data.R
https://github.com/NSAPH/data_requests/blob/master/request_projects/dec2019_medicaid_platform_cvd/cvd_readme.md

Sample user request: https://github.com/NSAPH/data_requests/tree/master/request_projects/feb2021_jenny_medicaid_resp
'''

## Examples of ingestion of processed data:

All paths are on `nsaph-sandbox01.rc.fas.harvard.edu`

### Demographics:

    python -u -m nsaph.model2 /data/incoming/rce/ci3_d_medicaid/processed_data/cms_medicaid-max/csv/maxdata_demographics.csv.gz

### Enrollments (yearly) and Eligibility (monthly)

    nohup python -u -m nsaph.data_model.model2 --data /data/incoming/rce/ci3_d_medicaid/processed_data/cms_medicaid-max/data_cms_medicaid-max-ps_patient-year/medicaid_mortality_2005.fst -t enrollments_year --threads 4 --page 5000 &

### Admissions

    for f in /data/incoming/rce/ci3_d_medicaid/processed_data/cms_medicaid-max/data_cms_medicaid-max-ip_patient-admission-date/maxdata_*_ip_${year}.fst ; do 
	    date
	    echo $f
	    python -u -m nsaph.data_model.model2 --data $f  -t admissions --page 5000 --log 10000 --threads 2
    done

