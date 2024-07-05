"""
Wrapper for pulling a SAFE product from colhub archive and converting it to a NetCDF file
"""
import logging
import datetime as dt
import sys
from pathlib import Path
import argparse
import sentinelsat
from sentinelsat import SentinelAPI
import zipfile
import os
import yaml
import uuid
import shutil
import re
import json
from urllib.parse import urljoin
from safe_to_netcdf.s1_reader_and_NetCDF_converter import Sentinel1_reader_and_NetCDF_converter
from safe_to_netcdf.s2_reader_and_NetCDF_converter import Sentinel2_reader_and_NetCDF_converter
from send_email.mailer import email_sender
from utils.write_message import write_message

logger = logging.getLogger(__name__)

def get_config():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, "config", "config.yml")
    with open(file_path, "r") as yaml_file:
        cfg = yaml.safe_load(yaml_file)
        return cfg

def get_credentials(cfg):
    url = cfg['hub']['url']
    user = cfg['hub']['user']
    password = cfg['hub']['password']
    return url, user, password

def get_file_age_in_days(file_path):
    modified_time = os.path.getmtime(file_path)
    modified_time_datetime = dt.datetime.fromtimestamp(modified_time)
    current_date = dt.datetime.now()
    age_in_days = (current_date - modified_time_datetime).days
    return age_in_days

class Product():

    def __init__(self, product_name, cfg):
        self.product_name = product_name
        self.cfg = cfg
        # Creating subdirectories to temporarily storing logs
        self.tmp_products_dir = Path(cfg['tmp_products_dir'])
        self.tmp_product_path = self.tmp_products_dir / str(self.product_name + '.nc')
        self.safe_tmp = self.tmp_products_dir / str(self.product_name + '.zip')
        if not os.path.exists(self.tmp_products_dir):
            os.makedirs(self.tmp_products_dir)

        operational_NetCDFs_path = Path(self.cfg['operational_NetCDFs_path'])
        product_type = self.product_name.split('_')[0]
        if product_type.startswith('S1'):
            beam = self.product_name.split('_')[1]
        date_match = re.search(r'(\d{4})(\d{2})(\d{2})T', self.product_name)
        year = date_match.group(1)
        month = date_match.group(2)
        day = date_match.group(3)
        if product_type.startswith('S1'):
            self.relative_path = Path(product_type + '/' + year + '/' + month + '/' + day + '/' + beam)
        elif product_type.startswith('S2'):
            self.relative_path = Path(product_type + '/' + year + '/' + month + '/' + day)
        self.operational_product_path = operational_NetCDFs_path / self.relative_path / str(self.product_name + '.nc')
        logger.info(f"Creating directory if it doesn't already exist {self.relative_path}")
        self.operational_product_path.parent.mkdir(parents=True, exist_ok=True)

    def netcdf_file_exists(self):
        '''
        Files will kept longer in the operational directory (for example 4 weeks after creation)
        Files will be kept shorter in the netcdf_ondemand directory (for example 7 days after request)
        If the file is in the operational directory and has less time remaining than the lifespan of the products in the netcdf_ondemand directory,
            - copy the file across to the netcdf_ondemand directory
        If the file exists in the netcdf_ondemand directory
            - update the file modified date
        '''
        operational_file_exists = os.path.exists(self.operational_product_path)
        #operational_file_exists = False
        if operational_file_exists:
            logger.info(f'Operational NetCDF file {str(self.product_name) + ".nc"} already exists')
            age_in_days = get_file_age_in_days(self.operational_product_path)
            if age_in_days < self.cfg['operational_products_keep_days'] - self.cfg['tmp_products_keep_days']:
                shutil.copyfile(self.operational_product_path, self.tmp_product_path)
                # Construct URL with double forward slashes using urljoin
                opendap_route_path = 'https://nbstds.met.no/thredds/dodsC/NetCDF_ondemand/'
                self.opendap_product_path = urljoin(opendap_route_path, str(self.product_name + '.nc.html'))
            else:
                opendap_route_path = 'https://nbstds.met.no/thredds/dodsC/NBS/'
                self.opendap_product_path = urljoin(opendap_route_path, str(self.relative_path / str(self.product_name + '.nc.html')))
            return True
        else:
            logger.info(f'Operational NetCDF file {str(self.product_name) + ".nc"} does not exist')
            exists_in_tmp_storage = os.path.exists(self.tmp_product_path)

            # Construct URL with double forward slashes using urljoin
            opendap_route_path = 'https://nbstds.met.no/thredds/dodsC/NetCDF_ondemand/'
            self.opendap_product_path = urljoin(opendap_route_path, str(self.product_name + '.nc.html'))

            if exists_in_tmp_storage:
                logger.info(f'NetCDF file {str(self.product_name) + ".nc"} exists in NetCDF ondemand temporary storage directory')
                self.update_time_modified()
                return True
            else:
                logger.info(f'NetCDF file {str(self.product_name) + ".nc"} does not exist in NetCDF ondemand temporary storage directory')
                return False


    def download_safe_product(self):
        # Connect to datahub to download data
        logger.debug('Logging to datahub')
        url, user, pwd = get_credentials(self.cfg)
        hub = SentinelAPI(user, pwd, url, show_progressbars=False)

        # Qcheck_locksuery datahub to get uuid from filename (ie product name)
        logger.debug('Querying datahub to get uuid from product name.')
        product_info = list(hub.query(filename=self.product_name + '*'))
        if len(product_info) > 1:
            logger.error(f'Found more than one dataset with filename {self.product_name}. Exiting.')

        elif len(product_info) == 0:
            logger.error(f"Product not found on {url}")

        logger.debug(product_info)
        uuid = product_info[0]
        logger.debug(f"uuid: {uuid}")

        # Download SAFE
        logger.debug('Starting to download SAFE')
        if not self.safe_tmp.is_file():
            try:
                hub.download(uuid, directory_path=self.tmp_products_dir)
            except (sentinelsat.sentinel.SentinelAPIError, sentinelsat.sentinel.InvalidChecksumError) as e:
                logger.error(f"Could not download product {self.product_name} from colhub.met.no. Hence terminating.")
                logger.error(e)
                return False
            # sentinelAPI sometimes throws 'non-sentinelAPI' exceptions
            # See Issue #18
            except BaseException as e:
                logger.error(f"Could not download product {self.product_name} from colhub.met.no. Hence terminating.")
                logger.error('Un-expected exception. See traceback below:')
                logger.error(e)
            logger.debug('Done downloading SAFE')

    def unzip_safe_product(self):
        if not self.safe_tmp.is_file():
            self.safe_tmp = self.tmp_products_dir / str(self.product_name + '.SAFE.zip')
            if self.safe_tmp.is_file():
                logger.info('Usual zip archive not found. But instead found one with SAFE extension in addition (.SAFE.zip instead of .zip).')
            else:
                logger.error('Archive not found. Hence exiting')
                #return False

        logger.debug('Starting unzipping SAFE')
        try:
            with zipfile.ZipFile(self.safe_tmp, 'r') as zip_ref:
                zip_ref.extractall(self.tmp_products_dir)
        except zipfile.BadZipfile as e:
            logger.error("Could not unzip SAFE archive. Hence terminating.")
            logger.error(e)
            #return False

        logger.debug('Done unzipping SAFE')
        logger.info('Download and unzip of SAFE archive OK.')

    def safe_to_netcdf(self):
        # Create NC from SAFE
        logger.debug('Starting to create NC file')
        start = dt.datetime.now()

        if self.product_name.startswith('S2'):
            conversion_object = Sentinel2_reader_and_NetCDF_converter(product=self.product_name, indir=self.tmp_products_dir, outdir=self.tmp_products_dir)
        else:
            conversion_object = Sentinel1_reader_and_NetCDF_converter(product=self.product_name, indir=self.tmp_products_dir, outdir=self.tmp_products_dir)
        if not conversion_object.read_ok:
            logger.error("Something went wrong in reading SAFE product. Hence exiting.")
        conversion_OK = conversion_object.write_to_NetCDF(self.tmp_products_dir, compression_level=1)
        logger.info(f"It took {(dt.datetime.now() - start).total_seconds()} seconds to the create nc file.")

        if conversion_OK:
            logger.info('Conversion to NetCDF OK.')
        else:
            logger.error(f"\nERROR: Something went wrong in converting {self.product_name} to NetCDF")

    def update_time_modified(self):
        Path(self.tmp_product_path).touch()
        logger.info(f"Updated the time {str(self.product_name + '.nc')} was last modified to extend the time until the file will be deleted")

    def remove_safe(self):
        # Get the list of files in the directory
        files_and_dirs = os.listdir(self.tmp_products_dir)

        # Iterate over each file in the directory
        for entry in files_and_dirs:

            entry_path = os.path.join(self.tmp_products_dir, entry)

            # Check if the entry starts with the product name
            if entry.startswith(str(self.product_name)) and not entry.endswith('.nc'):
                logger.info('Deleting ' + entry)

                # Check if the entry is a directory
                if os.path.isdir(entry_path):
                    shutil.rmtree(entry_path)
                    logger.info(entry + ' was successfully deleted')
                # Check if the entry is a file
                elif os.path.isfile(entry_path):
                    os.remove(entry_path)
                    logger.info(entry + ' file was successfully deleted')


def main(email, product_names):
    cfg = get_config()
    tmp_logs_dir = Path(cfg['tmp_logs_dir'])

    # Creating subdirectories to temporarily store logs
    if not os.path.exists(tmp_logs_dir):
        os.makedirs(tmp_logs_dir)

    # Log to console
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    log_info = logging.StreamHandler(sys.stdout)
    log_info.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(log_info)

    log_file_name = tmp_logs_dir / f"logfile_{uuid.uuid4()}.log"
    log_file = logging.FileHandler(log_file_name)
    log_file.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(log_file)

    # Create recipients list
    if isinstance(email, str):
        recipients = [email]
    elif isinstance(email, list):
        recipients = email
    else:
        logger.error("Invalid email format")
        sys.exit(1)

    # Create product_names list from fileList
    opendap_links = []  # List of opendap links to include in email message
    failures = []  # List of failures to include in email message

    for product_name in product_names:
        if product_name.startswith('S1') or product_name.startswith('S2'):
            product = Product(product_name, cfg)
            exists = product.netcdf_file_exists()
            if exists:
                opendap_links.append(str(product.opendap_product_path))
                product.remove_safe()
            else:
                product.download_safe_product()
                product.unzip_safe_product()
                product.safe_to_netcdf()
                product.remove_safe()
                logger.info(f"---------Downloaded and converted {product_name}-----------")
                if product.netcdf_file_exists():
                    opendap_links.append(str(product.opendap_product_path))
                else:
                    failures.append(product.product_name)
        else:
            logger.info(f"---------{product_name} does not begin with S1 or S2. Skipping-----------")

    logger.info("---------Sending an email to user-----------")
    subject = 'NetCDF files created and ready to use'
    message = write_message(cfg, opendap_links, failures)
    attachment_path = log_file_name

    email_sender(recipients, subject, message, attachment_path=attachment_path)
    logger.info(f"------------END OF JOB-------------")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script to download SAFE files from Colhub Archive and convert them to NetCDF.")

    # Add arguments for email and products
    parser.add_argument("--email", type=str, required=True, help="Email address where notifications or results will be sent.")
    parser.add_argument("--products", type=str, required=True, help="Comma-separated list of product names to serve NetCDF files for.")

    args = parser.parse_args()
    product_list = args.products.split(",")
    main(args.email, product_list)
