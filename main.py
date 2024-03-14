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
from safe_to_netcdf.s1_reader_and_NetCDF_converter import Sentinel1_reader_and_NetCDF_converter
from safe_to_netcdf.s2_reader_and_NetCDF_converter import Sentinel2_reader_and_NetCDF_converter
from send_email import send_email
from utils.write_message import write_message

logger = logging.getLogger(__name__)

def get_config():
    file_path = "config/config.yml"
    with open(file_path, "r") as yaml_file:
        cfg = yaml.safe_load(yaml_file)
        return cfg

def get_credentials(cfg):
    url = cfg['hub']['url']
    user = cfg['hub']['user']
    password = cfg['hub']['password']
    return url, user, password

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

        lustre_NetCDFs_path = Path(self.cfg['lustre_NetCDFs_path'])
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
        self.lustre_product_path = lustre_NetCDFs_path / self.relative_path / str(self.product_name + '.nc')
        logger.info(f"Creating directory if it doesn't already exist {self.lustre_product_path}")
        self.lustre_product_path.parent.mkdir(parents=True, exist_ok=True)
        opendap_route_path = Path('https://nbstds.met.no/thredds/dodsC/NBS/')
        self.opendap_product_path = opendap_route_path / self.relative_path / str(self.product_name + '.nc.html')

    def netcdf_file_exists(self):
        exists = os.path.exists(self.lustre_product_path)
        if exists == True:
            logger.info(f'NetCDF file {str(self.product_name + '.nc')} already exists on lustre')
        return os.path.exists(self.lustre_product_path)

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

    def move_to_lustre(self):

        logger.info(f'Moving NetCDF file to {self.lustre_product_path}')
        shutil.move(self.tmp_product_path, self.lustre_product_path)

    def update_time_modified(self):
        Path(self.lustre_product_path).touch()
        logger.info(f"Updated the time {str(self.product_name + '.nc')} was last modified to extend the time until the file will be deleted")


def main(args):

    cfg = get_config()

    tmp_logs_dir = Path(cfg['tmp_logs_dir'])

    # Creating subdirectories to temporarily storing logs
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

    product_names = args.product_names.split(',')
    opendap_links = [] # List of opendap links to include in email message
    failures = [] # List of failures to include in email message

    for product_name in product_names:
        if product_name.startswith('S1') or product_name.startswith('S2'):
            product = Product(product_name, cfg)
            if product.netcdf_file_exists() == True:
                product.update_time_modified()
                opendap_links.append(str(product.opendap_product_path))
            else:
                product.download_safe_product()
                product.unzip_safe_product()
                product.safe_to_netcdf()
                product.move_to_lustre()
                logger.info(f"---------Downloaded and converted {product_name}-----------")
                if product.netcdf_file_exists() == True:
                    opendap_links.append(str(product.opendap_product_path))
                else:
                    failures.append(product.product_name)
        else:
            logger.info(f"---------{product_name} does not begin with S1 or S2. Skipping-----------")

    logger.info("---------Sending an email to user-----------")
    recipients = [
        {'name': 'Luke Marsden', 'email': 'lukem@met.no'}
    ]
    subject = 'NetCDF files created and ready to use'
    message = write_message(opendap_links, failures)
    attachment_path = log_file_name

    send_email(recipients, subject, message, attachment_path=attachment_path)
    logger.info(f"------------END OF JOB-------------")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script to download SAFE files from Colhub Archive and convert them to NetCDF.")

    parser.add_argument("--product_names", type=str, required=True, help="Comma separated list of name of the Sentinel product to serve as NetCDF files")

    args = parser.parse_args()
    main(args)