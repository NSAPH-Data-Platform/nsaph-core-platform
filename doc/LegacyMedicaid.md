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

