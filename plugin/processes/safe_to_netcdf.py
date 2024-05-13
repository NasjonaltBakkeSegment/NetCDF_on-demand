# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
#
# Copyright (c) 2022 Tom Kralidis
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import subprocess

LOGGER = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.0.1',
    'id': 'safe-to-netcdf',
    'title': {
        'en': 'Safe to NetCDF'
    },
    'description': {
        'en': 'A process that converts a SAFE file to a NetCDF file'
    },
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['safe', 'netcdf'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'product_names': {
            'title': 'Product names',
            'description': 'Comma separated list of product names to be downloaded and converted from ColHub Archive',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use?
            'keywords': ['product names']
        }
    },
    'outputs': {
        'filepath': {
            'title': 'Hello, world',
            'description': 'The filepath to the output NetCDF file',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    }
}


class SafeToNetCDFProcessor(BaseProcessor):
    """Safe to NetCDF Processor example"""

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: pygeoapi.process.safe_to_netcdf.SafeToNetCDFProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):

        mimetype = 'application/json'
        product_names = data.get('product_names')

        if product_names is None:
            raise ProcessorExecuteError('Cannot process without a product name')

        script_path = 'netcdf_ondemand.py'
        arguments = ['--product_names', product_names]
        subprocess.run(['python', script_path] + arguments)

        message = 'Files downloaded and converted to NetCDF format'

        outputs = {
            'id': 'filepath',
            'value': f'{message}'.strip()
        }

        return mimetype, outputs

    def __repr__(self):
        return f'<SafeToNetCDFProcessor> {self.filepath}'
