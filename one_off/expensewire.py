# %%
import requests
from bs4 import BeautifulSoup
import xmltodict
from collections import OrderedDict
import traceback2 as traceback
from sgs.util.convert import _response
import logging


def get_data(data_set:str="ExpenseDetail", query:str=""):
    """Pull data from expense wire api"""
    
    try:
        cleaned_expense_details = []
        lists = {}
        for d in detials:
            d = dict(d)
            new_dict = {}
            for k, v in d.items():
                # if 1st order value is a dict then flatten
                if type(v) in [OrderedDict, dict]:
                    # flatten out the dict
                    nv = _flatten_dict(v, prefix=k, seperate_lists=False)
                    # add the flattened data to the main dict
                    new_dict.update(nv.get('d', {}))
                    # save the extracted lists to their own object for later use
                    if nv.get('l', False):
                        for listkey, lv in nv.get('l', {}).items():
                            if not lists.get(listkey):
                                lists[listkey] = []
                            lists[listkey].extend(lv)
                # if first order value is a list then parse each item and save to apprpriate list 
                elif type(v) == list:
                    nlvs = []
                    for lv in v:
                        # if list item is ordered dict then convert
                        if type(lv) in [OrderedDict, dict]:
                            # flatten out the dict
                            nlv = flatten_dict(lv, prefix=k, seperate_lists=False)
                            # save the extracted lists to their own object for later use
                            if nlv.get('l', False):
                                for listkey, lvv in nlv.get('l', {}).items():
                                    if not lists.get(listkey):
                                        lists[listkey] = []
                                    lists[listkey].extend(lvv)
                            # add the flattened data to the main dict
                            nlvs.append(nlv.get('d', {}))
                        else:
                            nlvs.append(lv)
                    new_dict[k] = nlvs
                    if not lists.get(k):
                        lists[k] = []
                    lists[k].extend(nlvs)
                else:
                    new_dict[k] = v
            
            cleaned_expense_details.append(new_dict)
        return _response(200, data=cleaned_expense_details, message=traceback.format_exc())
    except Exception as e:
        logging.error(e)
        logging.error(results['ExpenseWire']['SendDataTransaction'])
        return _response(500, message=traceback.format_exc())

# %% #= GET THE EXPENSE DETAILS (lowest granualirty)
expesnses = []
errors = []
# * Had trouble loading all the data at once so need to iterate thru the 6 status codes
for code in [1,2,3,4,5,6]:
    resp_g = get_data(data_set="Expense", query=f"<StatusID>{code}</StatusID>")
    if resp_g['status'] != 200:
        logging.error(resp_g)
        errors.append({code: resp_g})
        continue
        
    print(len(resp_g['data']))
    expesnses.extend(resp_g['data'])
    
#%% format the data for sending to FiveTran
# %% #* get the 
    resp_g = get_data(data_set="Expense", query=f"<StatusID>{code}</StatusID>")

# %%
