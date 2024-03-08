#!/usr/bin/python3
import os
import yaml

def write_message(opendap_links, failures):
    '''
    opendap_links: a list of links to where products are on OPeNDAP.
    failures: a list of products that were not processed successfully.
    '''

    if len(opendap_links) == 0:
        opendap_links_string = 'No products were processed successfully'
    else:
        opendap_links_string = '\n'.join(opendap_links)

    if len(failures) == 0:
        failures_string = 'All products were processed successfully'
    else:
        failures_string = '\n'.join(failures)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    message_template_path = os.path.join(script_dir, "../static/message_template.txt")

    with open(message_template_path, "r") as file:
        template = file.read()

    config_path = os.path.join(script_dir, "../config/config.yml")

    with open(config_path, "r") as yaml_file:
        cfg = yaml.safe_load(yaml_file)
        product_keep_hours = cfg['product_keep_hours']

    message = template.format(
        opendap_links=opendap_links_string,
        failures=failures_string,
        keep_hours=product_keep_hours
        )

    return message