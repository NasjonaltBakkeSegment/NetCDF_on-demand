#!/usr/bin/python3
import os
import re

def write_message(cfg, opendap_links, failures):
    '''
    opendap_links: a list of links to where products are on OPeNDAP.
    failures: a list of products that were not processed successfully.
    '''

    operational_keep_days = cfg['operational_products_keep_days']
    tmp_keep_days = cfg['tmp_products_keep_days']

    # Loading the main email message template
    script_dir = os.path.dirname(os.path.abspath(__file__))
    message_template_path = os.path.join(script_dir, "../static/message_template.txt")

    with open(message_template_path, "r") as file:
        template = file.read()

    # Creating a message for products processed successfully
    if len(opendap_links) > 0:
        success_message_file_path = 'static/success_message_template.txt'
        with open(success_message_file_path, 'r') as file:
            success_message = file.read()
        opendap_links_string = '\n'.join(opendap_links)
        success_message_with_links = success_message.format(
            opendap_links=opendap_links_string,
            tmp_keep_days=tmp_keep_days
        )
    else:
        success_message_with_links = ''

    # Creating a message for products not processed successfully
    if len(failures) > 0:
        failures_string = '**All products were processed successfully**'
        failure_message_file_path = 'static/failure_message_template.txt'
        with open(failure_message_file_path, 'r') as file:
            failure_message = file.read()
        failures_string = '\n'.join(failures)
        failure_message_with_products = failure_message.format(
            failures=failures_string
        )
    else:
        failure_message_with_products = 'All the products requested were successfully processed.'

    message = template.format(
        success_message=success_message_with_links,
        failure_message=failure_message_with_products,
        operational_keep_days=operational_keep_days
        )

    # Removing multiple blank lines and replacing them with single blank lines
    message_cleaned = re.sub(r'\n\s*\n+', '\n\n', message)

    return message_cleaned