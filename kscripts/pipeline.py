'''
A pipeline for loading SPL data into Couchbase
'''

import os
import glob
import logging
import common
import re
import json
import shutil
from lxml import etree
from lxml.etree import XMLSyntaxError

DAILY_MED_ARCH_EXTRACT_DIR = ("./output/openfda-tmp")
DAILY_MED_FLATTEN_DIR = ("./output/flattened")
DAILY_MED_CONSOLIDATED_DIR = ("./output/consolidated")
# Change this to a folder that ONLY has your DailyMed extracted SPL archives
DAILY_MED_ARCH_INPUT_DIR = ("/home/ky/Downloads/PSKA_STORE/DailyMed/splArchives/")

SPL_JS = './kscripts/spl_to_json.js'
LOINC = './kscripts/sections.csv'

SPL_INDEX_DIR = ("./output/spl_index/index.json")
SPL_JSON_DIR = ("./output/spl_json/")

CONCATENATED_SPL_INDICES = {}

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def ExtractDailyMedSPL():
    # Find all archive files
    src_dir = DAILY_MED_ARCH_INPUT_DIR
    extract_dir = DAILY_MED_ARCH_EXTRACT_DIR
    os.system('mkdir -p "%s"' % extract_dir)
    pattern = os.path.join(src_dir, '*.zip')
    zip_files = glob.glob(pattern)
    num_zip_files = len(zip_files)

    # Handle exception cases.
    if num_zip_files == 0:
        logging.warning("Expected to find at least one DailyMed SPL archive")

    logging.debug("Found %(num_zip_files)s DailyMed Archives. Beginning extraction..." % locals())

    current_file = 0
    for zip_file in zip_files:
        common.progress_bar(current_file, num_zip_files, 10)
        common.shell_cmd_quiet('unzip -oq -d %(extract_dir)s %(zip_file)s' % locals())
        current_file+=1
    common.progress_bar(current_file, num_zip_files, 10)

def FlattenDailyMedSPL():
    # This can probably be done in a better format via a Luigi pipeline, but uh, we'll get to that
    src_dir = os.path.join(DAILY_MED_ARCH_EXTRACT_DIR, "prescription")
    extract_dir = DAILY_MED_FLATTEN_DIR
    consolidated_dir = DAILY_MED_CONSOLIDATED_DIR
    os.system('mkdir -p "%s"' % extract_dir)
    os.system('mkdir -p "%s"' % consolidated_dir)
    pattern = os.path.join(src_dir, '*.zip')
    zip_files = glob.glob(pattern)
    num_zip_files = len(zip_files)
    logging.debug("Found %(num_zip_files)s SPL Archives. Beginning extraction..." % locals())

    current_zip_file = 0
    for zip_file in zip_files:
        # Read the zip contents so we can investigate the XML file inside
        cmd = 'zipinfo -1 %(zip_file)s' % locals()
        xml_file_name = None
        zip_contents = common.shell_cmd_quiet(cmd)
        
        # Don't include this line if you value your terminal not spamming you
        # logging.debug("Found zip contents:\n%(zip_contents)s" % locals())
        
        xml_match = re.search('^([0-9a-f-]{36})\.xml$', zip_contents.decode(), re.I | re.M)
        if(xml_match):
            
            xml_file_name = xml_match.group()
            spl_dir_name = os.path.join(extract_dir, xml_match.group(1))
            os.system('mkdir -p "%s"' % spl_dir_name)
            common.progress_bar(current_zip_file, num_zip_files)
            common.shell_cmd_quiet('unzip -oq %(zip_file)s -d %(spl_dir_name)s' % locals())
            # From here, we now will consolidate the XML files by moving them to the consolidated directory
            os.system('cp "%(spl_dir_name)s/%(xml_file_name)s" "%(consolidated_dir)s/%(xml_file_name)s"' % locals())
            os.system('rm -r "%(spl_dir_name)s"' % locals())
        
        current_zip_file +=1
    common.progress_bar(current_zip_file, num_zip_files)

def DetermineSPLToIndex():
    # Magic string?
    NS = {'ns': 'urn:hl7-org:v3'}
    
    src_dir = DAILY_MED_CONSOLIDATED_DIR

    pattern = os.path.join(src_dir, '*.xml')
    xml_files = glob.glob(pattern)
    num_xml_files = len(xml_files)
    logging.debug("Found %(num_xml_files)s XML files. Determining Index from SPL..." % locals())

    current_xml_file = 0
    for xml_file in xml_files:
        if os.path.getsize(xml_file) > 0:
            #logging.debug("Non-Zero sized file #%(current_xml_file)s" % locals())
            # # Uncompress Gzipped XML files here
            # filetype = common.shell_cmd_quiet('file %(xml_file)s' % locals())
            # common.progress_bar(current_xml_file, num_xml_files)
            # if "gzip compressed data" in filetype.decode() or "DOS/MBR boot sector" in filetype.decode():
            #     logging.warning("SPL XML is gzipped: " + xml_file)

            common.progress_bar(current_xml_file, num_xml_files)
            p = etree.XMLParser(huge_tree=True)
            try:
                tree = etree.parse(open(xml_file), parser=p)
                code = next(iter(
                    tree.xpath("//ns:document/ns:code[@codeSystem='2.16.840.1.113883.6.1']/@displayName", namespaces=NS)),
                    '')
                if code.lower().find('human') != -1:
                    spl_id = tree.xpath('//ns:document/ns:id/@root', namespaces=NS)[0].lower()
                    spl_set_id = tree.xpath('//ns:document/ns:setId/@root', namespaces=NS)[0].lower()
                    version = tree.xpath('//ns:document/ns:versionNumber/@value', namespaces=NS)[0]
                    # Here we need to store the output of the relevant XML data
                    if spl_set_id not in CONCATENATED_SPL_INDICES.keys():
                        CONCATENATED_SPL_INDICES[spl_set_id] = {'spl_id': spl_id, 'version': version}
                elif len(code) == 0:
                    logging.warning("Not a drug label SPL file: " + xml_file)
            except XMLSyntaxError as e:
                logging.warning("Invalid SPL file: " + xml_file)
                logging.warning(e)
            except:
                logging.error("Error processing SPL file: " + xml_file)
                raise
        else:
            logging.warning("Zero Length SPL file: " + xml_file)
        current_xml_file+=1
    # Before exiting the function, we must now write the in-memory dictionary out to a JSON
    logging.debug("Dumping SPL Indices to JSON...")
    # Check if file exists before creating it
    if not os.path.exists(SPL_INDEX_DIR):
        logging.debug("Creating database file for SPL anno")
        (index_db_dir, index_db_filename) = os.path.split(SPL_INDEX_DIR)
        os.system('mkdir -p "%s"' % index_db_dir)
        os.system('touch "%s"' % index_db_filename)
    with open(SPL_INDEX_DIR, 'w') as fp:
        json.dump(CONCATENATED_SPL_INDICES, fp)
    return

def SPL2JSON():
    spl_path = DAILY_MED_CONSOLIDATED_DIR

    pattern = os.path.join(spl_path, '*.xml')
    xml_files = glob.glob(pattern)
    num_xml_files = len (xml_files)
    logging.debug("Found %(num_xml_files)s XML files. Running SPL2JSON on each one now..." % locals())
    spl_js = SPL_JS
    loinc = LOINC

    # Create a JSON output directory if it doesn't exist
    if not os.path.exists(SPL_JSON_DIR):
        logging.debug("Creating JSON output directory...")
        os.system('mkdir -p "%s"' % SPL_JSON_DIR)
    
    current_xml_file = 0
    for xml_file in xml_files:
        cmd = 'node %(spl_js)s %(xml_file)s %(loinc)s' % locals()
        json_str = ''
        try:
           json_str = common.shell_cmd_quiet(cmd)
           json_obj = json.loads(json_str)
           if not json_obj.get('set_id'):
               raise RuntimeError("SPL File has no set_id: %s", xml_file)
           else:
               #logging.debug("XML File %(xml_file)s has set_id %(set_id)s" % locals())
               common.progress_bar(current_xml_file, num_xml_files)
               # Write the JSON to a file
               json_filename = os.path.join(SPL_JSON_DIR, json_obj['set_id'] + '.json')
               with open(json_filename, 'w') as fp:
                   json.dump(json_obj, fp)
        except:
           logging.error("Error processing SPL XML file: " + xml_file)
           raise
        current_xml_file+=1
    common.progress_bar(current_xml_file, num_xml_files)


# Script execution begins here
# Modify the following lines to change the script's behavior
ExtractDailyMedSPL()
FlattenDailyMedSPL()
# This script is really not needed, but it's here for reference
# DetermineSPLToIndex()
SPL2JSON()