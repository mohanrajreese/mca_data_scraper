import pandas as pd

def csv_to_target_url_list(csv_name):
    data = pd.read_csv(csv_name)
    base_url = "https://www.startupwala.com/company-registration/private-limited"
    cin_list = data['cin'].tolist()
    company_list = data['company_name'].tolist()
    roc_list = data['roc'].tolist()
    target_url_list = []
    for i in range(len(cin_list)):
        print(roc_list[i])
        build_url = base_url +"-"+ company_list[i].replace(" ","-") + roc_list[i][3:] + "-" + cin_list[i]
        target_url_list.append(build_url)
        print(build_url)
    return target_url_list
    
new = csv_to_target_url_list(csv_name="/home/mohanraj/projects/mca_data/db_merger/100.csv")
print(new)
