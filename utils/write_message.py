#!/usr/bin/python3
import os
import yaml

def write_message(cfg, opendap_links, failures):
    '''
    opendap_links: a list of links to where products are on OPeNDAP.
    failures: a list of products that were not processed successfully.
    '''

    if len(opendap_links) == 0:
        opendap_links_string = 'No products were processed successfully'
    else:
        opendap_links_string = '\n'.join(opendap_links)

    if len(failures) == 0:
        failures_string = '**All products were processed successfully**'
    else:
        failures_string = '\n'.join(failures)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    message_template_path = os.path.join(script_dir, "../static/message_template.txt")

    with open(message_template_path, "r") as file:
        template = file.read()

    operational_keep_days = cfg['operational_products_keep_days']
    tmp_keep_days = cfg['tmp_products_keep_days']

    message = template.format(
        opendap_links=opendap_links_string,
        failures=failures_string,
        tmp_keep_days=tmp_keep_days,
        operational_keep_days=operational_keep_days
        )

    return message