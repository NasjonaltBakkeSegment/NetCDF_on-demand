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
    """
    Loads the configuration from a YAML file.

    Returns:
        dict: Configuration dictionary loaded from config.yml.
    """
    script_directory = os.path.dirname(os.path.abspath(__file__))
    config_file_path = os.path.join(script_directory, "config", "config.yml")

    if not os.path.exists(config_file_path):
        logger.error(f"Configuration file not found: {config_file_path}")
        sys.exit(1)

    with open(config_file_path, "r") as config_file:
        config = yaml.safe_load(config_file)

    return config

def get_credentials(config):
    """
    Extracts the credentials from the configuration dictionary.

    Args:
        config (dict): Configuration dictionary.

    Returns:
        tuple: A tuple containing the URL, username, and password.
    """
    hub_config = config['hub']
    url = hub_config['url']
    username = hub_config['user']
    password = hub_config['password']

    return url, username, password

def get_file_age_in_days(file_path):
    """
    Calculates the age of a file in days based on its last modification time.

    Args:
        file_path (str): Path to the file.

    Returns:
        int: Age of the file in days.
    """
    last_modified_time = os.path.getmtime(file_path)
    last_modified_datetime = dt.datetime.fromtimestamp(last_modified_time)
    current_datetime = dt.datetime.now()

    age_in_days = (current_datetime - last_modified_datetime).days

    return age_in_days

class Product:
    def __init__(self, product_name, config, opendap_base_dir, opendap_netcdf_ondemand_dir, request_dir):
        """
        Initializes a Product instance.

        Args:
            product_name (str): Name of the product.
            config (dict): Configuration dictionary.
            opendap_base_dir (Path): Path to the opendap base directory
            opendap_netcdf_ondemand_dir (Path): Path to where NetCDF-ondemand requested files are stored
            request_dir (str): Path to the request-specific directory where products and logs are stored
        """
        self.product_name = product_name
        self.config = config
        self.opendap_base_dir = Path(opendap_base_dir)
        self.opendap_netcdf_ondemand_dir = Path(opendap_netcdf_ondemand_dir)
        self.request_dir = Path(request_dir)

        # Set up temporary directories for storing logs
        self._define_product_path()

        # Determine the relative path and operational product path
        self._define_operational_path()

    def _define_product_path(self):
        """
        Define filepath for NetCDF and SAFE product
        """
        self.tmp_product_path = self.request_dir / f"{self.product_name}.nc"
        self.safe_tmp = self.request_dir / f"{self.product_name}.zip"

    def _define_operational_path(self):
        """
        Define the filepath where the operational product could be.
        """
        operational_NetCDFs_path = Path(self.config['operational_NetCDFs_path'])
        product_type = self.product_name.split('_')[0]

        date_match = re.search(r'(\d{4})(\d{2})(\d{2})T', self.product_name)
        if not date_match:
            raise ValueError(f"Invalid product name format: {self.product_name}")

        year, month, day = date_match.groups()

        if product_type.startswith('S1'):
            beam = self.product_name.split('_')[1]
            relative_path = Path(f"{product_type}/{year}/{month}/{day}/{beam}")
        elif product_type.startswith('S2'):
            relative_path = Path(f"{product_type}/{year}/{month}/{day}")
        else:
            raise ValueError(f"Unsupported product type: {product_type}")

        self.operational_product_path = operational_NetCDFs_path / relative_path / f"{self.product_name}.nc"

    def netcdf_file_exists(self):
        """
        Checks if the NetCDF file exists in either the operational directory or temporary storage.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        if self.tmp_product_path.exists():
            logger.info(f'NetCDF file {self.product_name}.nc exists in request subdirectory')
            self.opendap_product_path = self._construct_opendap_path()
            return True

        elif self.operational_product_path.exists():
            logger.info(f'Operational NetCDF file {self.product_name}.nc already exists')
            shutil.copyfile(self.operational_product_path, self.tmp_product_path)
            self.opendap_product_path = self._construct_opendap_path()
            return True

        else:
            logger.info(f'Operational NetCDF file {self.product_name}.nc does not already exist. Will try to create it.')
            path_from_previous_request = self.find_product_from_previous_requests()
            if path_from_previous_request is not None:
                logger.info(f'NetCDF file {self.product_name}.nc was requested recently and is still in storage')
                shutil.copyfile(path_from_previous_request, self.tmp_product_path)
                # Touching to update the time the file was last modified
                Path(path_from_previous_request).touch()
                logger.info(f'Copied {self.product_name}.nc to subdirectory for this request')
                self.opendap_product_path = self._construct_opendap_path()
                return True
            else:
                logger.info(f'NetCDF file {self.product_name}.nc does not exist in NetCDF ondemand temporary storage directory')
                self.opendap_product_path = self._construct_opendap_path()
                return False

    def _construct_opendap_path(self):
        """
        Constructs the OPeNDAP path for the product.

        Returns:
            str: Complete OPeNDAP URL.
        """
        relative_path = self.request_dir.relative_to(self.opendap_base_dir)
        opendap_route_path = 'https://nbstds.met.no/thredds/dodsC/' + str(relative_path) + '/'
        return urljoin(opendap_route_path, f"{self.product_name}.nc.html")

    def find_product_from_previous_requests(self):
        """
        Checking whether the product has been requested recently and is still in storage

        Returns:
            None if doesn't exist
            str: Filepath to product if it exists
        """
        for root, dirs, files in os.walk(self.opendap_netcdf_ondemand_dir):
            if f'{self.product_name}.nc' in files:
                return os.path.abspath(os.path.join(root, f'{self.product_name}.nc'))
        return None


    def download_safe_product(self):
        """
        Downloads the SAFE product from the datahub.
        """
        logger.debug('Logging to datahub')
        url, user, pwd = get_credentials(self.config)
        hub = SentinelAPI(user, pwd, url, show_progressbars=False)

        logger.debug('Querying datahub to get uuid from product name.')
        product_info = list(hub.query(filename=f"{self.product_name}*"))
        if len(product_info) > 1:
            logger.error(f'Found more than one dataset with filename {self.product_name}. Exiting.')
            return
        elif len(product_info) == 0:
            logger.error(f"Product not found on {url}")
            return

        uuid = product_info[0]#['uuid']
        logger.debug(f"uuid: {uuid}")

        logger.debug('Starting to download SAFE')
        if not self.safe_tmp.is_file():
            try:
                hub.download(uuid, directory_path=self.request_dir)
            except (sentinelsat.sentinel.SentinelAPIError, sentinelsat.sentinel.InvalidChecksumError) as e:
                logger.error(f"Could not download product {self.product_name} from colhub.met.no. Hence terminating.")
                logger.error(e)
                return
            except BaseException as e:
                logger.error(f"Could not download product {self.product_name} from colhub.met.no. Hence terminating.")
                logger.error('Un-expected exception. See traceback below:')
                logger.error(e)
                return
            logger.debug('Done downloading SAFE')

    def unzip_safe_product(self):
        """
        Unzips the SAFE product.
        """
        if not self.safe_tmp.is_file():
            self.safe_tmp = self.request_dir / f"{self.product_name}.SAFE.zip"
            if not self.safe_tmp.is_file():
                logger.error('Archive not found. Hence exiting')
                return

        logger.debug('Starting unzipping SAFE')
        try:
            with zipfile.ZipFile(self.safe_tmp, 'r') as zip_ref:
                zip_ref.extractall(self.request_dir)
        except zipfile.BadZipFile as e:
            logger.error("Could not unzip SAFE archive. Hence terminating.")
            logger.error(e)
            return
        logger.debug('Done unzipping SAFE')
        logger.info('Download and unzip of SAFE archive OK.')

    def safe_to_netcdf(self):
        """
        Converts the SAFE product to NetCDF format.
        """
        logger.debug('Starting to create NC file')
        start = dt.datetime.now()

        if self.product_name.startswith('S2'):
            conversion_object = Sentinel2_reader_and_NetCDF_converter(
                product=self.product_name,
                indir=self.request_dir,
                outdir=self.request_dir
            )
        else:
            conversion_object = Sentinel1_reader_and_NetCDF_converter(
                product=self.product_name,
                indir=self.request_dir,
                outdir=self.request_dir
            )

        if not conversion_object.read_ok:
            logger.error("Something went wrong in reading SAFE product. Hence exiting.")
            return

        conversion_OK = conversion_object.write_to_NetCDF(self.request_dir, compression_level=1)
        logger.info(f"It took {(dt.datetime.now() - start).total_seconds()} seconds to create the NC file.")

        if conversion_OK:
            logger.info('Conversion to NetCDF OK.')
        else:
            logger.error(f"ERROR: Something went wrong in converting {self.product_name} to NetCDF")

    def remove_safe(self):
        """
        Removes the SAFE files associated with the product from the temporary directory.
        """
        files_and_dirs = os.listdir(self.request_dir)

        for entry in files_and_dirs:
            entry_path = os.path.join(self.request_dir, entry)

            if entry.startswith(self.product_name) and not entry.endswith('.nc'):
                logger.info(f'Deleting {entry}')
                if os.path.isdir(entry_path):
                    shutil.rmtree(entry_path)
                    logger.info(f'{entry} was successfully deleted')
                elif os.path.isfile(entry_path):
                    os.remove(entry_path)
                    logger.info(f'{entry} file was successfully deleted')


def main(email, product_names):
    cfg = get_config()
    request_id = str(uuid.uuid4())

    # Create the request_dir path by joining the base bath for NetCDF on-demand products and the request_id
    opendap_base_dir = Path(cfg['opendap_base_dir'])
    opendap_netcdf_ondemand_dir = Path(opendap_base_dir / cfg['opendap_netcdf_ondemand_dir'])
    request_dir = opendap_netcdf_ondemand_dir / request_id

    # Creating subdirectories to temporarily store logs
    if not os.path.exists(request_dir):
        os.makedirs(request_dir)

    # Set up logging to console and file
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(console_handler)

    log_file_name = request_dir / f"logfile_{request_id}.log"
    file_handler = logging.FileHandler(log_file_name)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)

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
        try:
            if product_name.startswith('S1') or product_name.startswith('S2'):
                product = Product(product_name, cfg, opendap_base_dir, opendap_netcdf_ondemand_dir, request_dir)
                if product.netcdf_file_exists():
                    opendap_links.append(str(product.opendap_product_path))
                    product.remove_safe()
                else:
                    product.download_safe_product()
                    product.unzip_safe_product()
                    product.safe_to_netcdf()
                    product.remove_safe()
                    logger.info(f"Downloaded and converted {product_name}")
                    if product.netcdf_file_exists():
                        opendap_links.append(str(product.opendap_product_path))
                    else:
                        failures.append(product.product_name)
            else:
                logger.info(f"{product_name} does not begin with S1 or S2. Skipping")
                failures.append(product_name)
        except:
            failures.append(product_name)

    logger.info("Sending an email to user")
    subject = 'Requested NetCDF files'
    message = write_message(cfg, opendap_links, failures)
    if len(failures) > 0:
        email_sender(recipients, subject, message, attachment_path=log_file_name, cc=['nbs-helpdesk@met.no'])
    else:
        email_sender(recipients, subject, message, attachment_path=log_file_name)
    logger.info("End of job")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script to download SAFE files from Colhub Archive and convert them to NetCDF."
    )

    parser.add_argument(
        "--email", type=str, required=True,
        help="Email address where notifications or results will be sent."
    )
    parser.add_argument(
        "--products", type=str, required=True,
        help="Comma-separated list of product names to serve NetCDF files for."
    )

    try:
        args = parser.parse_args()
        product_list = [product.strip() for product in args.products.split(",")]
        main(args.email, product_list)
    except argparse.ArgumentError as e:
        logger.error(f"Argument parsing error: {e}")
        sys.exit(1)
