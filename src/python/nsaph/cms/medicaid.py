'''
Medicaid Domain for NSPAH

Running this module will create/update data model for raw CMS data

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

import os
from pathlib import Path

import yaml

from nsaph.cms.fts2yaml import MedicaidFTS


def create_yaml():
    name = "cms"
    domain = {
        name: {
            "reference": "https://resdac.org/getting-started-cms-data",
            "schema": name,
            "index": "explicit",
            "quoting": 3,
            "header": False,
            "tables": {
            }
        }
    }
    for x in ["ps", "ip"]:
        domain[name]["tables"].update(MedicaidFTS(x).init().to_dict())
    return yaml.dump(domain)


def update_model():
    src = Path(__file__).parents[3]
    registry_path = os.path.join(src, "yml", "cms.yaml")
    # output_path = "."
    # registry_path = os.path.join(output_path, "yml", "cms.yaml")
    with open(registry_path, "wt") as f:
        f.write(create_yaml())





if __name__ == '__main__':
    update_model()