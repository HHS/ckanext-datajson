import re

# from the iso8601 package, plus ^ and $ on the edges
ISO8601_REGEX = re.compile(r"^([0-9]{4})(-([0-9]{1,2})(-([0-9]{1,2})"
    r"((T)([0-9]{2}):([0-9]{2})(:([0-9]{2})(\.([0-9]+))?)?"
    r"(Z|(([-+])([0-9]{2}):([0-9]{2})))?)?)?)?$")

URL_REGEX = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https:// or ftp:// or ftps://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

ACCRUAL_PERIODICITY_VALUES = ("Annual", "Bimonthly", "Semiweekly", "Daily", "Biweekly", "Semiannual", "Biennial", "Triennial", "Three times a week", "Three times a month", "Continuously updated", "Monthly", "Quarterly", "Semimonthly", "Three times a year", "Weekly", "Completely irregular")

LANGUAGE_REGEX = re.compile("^[A-Za-z]{2}(-[A-Za-z]{2})?$")

COMMON_MIMETYPES = ("application/zip", "application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "text/csv", "application/xml", "application/rdf+xml", "application/json", "text/plain", "application/rss+xml")
MIMETYPE_REGEX = re.compile("^(application|text)/([a-z\-\.\+]+)(;.*)?$")

import lepl.apps.rfc3696
email_validator = lepl.apps.rfc3696.Email()

# load the OMB bureau codes on first load of this module
import urllib, csv
omb_burueau_codes = set()
for row in csv.DictReader(urllib.urlopen("https://raw.github.com/seanherron/OMB-Agency-Bureau-and-Treasury-Codes/master/omb-agency-bureau-treasury-codes.csv")):
    omb_burueau_codes.add(row["OMB Agency Code"] + ":" + row["OMB Bureau Code"])

# main function for validation
def do_validation(doc, src_url, errors_array):
    errs = { }
    
    if type(doc) != list:
        add_error(errs, 0, "Bad JSON Structure", "The file must be an array at its top level. That means the file starts with an open bracket [ and ends with a close bracket ].")
    elif len(doc) == 0:
        add_error(errs, 0, "Catalog Is Empty", "There are no entries in your file.")
    else:
        seen_identifiers = set()
        
        for i, item in enumerate(doc):
            # Required
            
            # title
            dataset_name = "dataset %d" % (i+1)
            if check_string_field(item, "title", 5, dataset_name, errs):
                dataset_name = '"%s"' % item.get("title", "").strip()

            # No fields should be null or an empty list, according to GSA. After this point we treat nulls as if they were
            # not present. This check must be after dataset_name is set above.
            for k, v in item.items():
                if v is None:
                    add_error(errs, 2, "File Format Issues", "The '%s' field is set to 'null'. If there is no value, the field must not be present." % k, dataset_name)
                if isinstance(v, list) and len(v) == 0:
                    add_error(errs, 2, "File Format Issues", "The '%s' field is an empty list. If there is no value, the field must not be present." % k, dataset_name)
                
            # description
            check_string_field(item, "description", 30, dataset_name, errs)
                
            # keyword
            if isinstance(item.get("keyword"), (str, unicode)):
                add_error(errs, 5, "Update Your File!", "The 'keyword' field used to be a string but now it must be an array.", dataset_name)
                
            elif check_required_field(item, "keyword", list, dataset_name, errs):
                for kw in item["keyword"]:
                    if not isinstance(kw, (str, unicode)):
                        add_error(errs, 5, "Invalid Required Field Value", "Each keyword in the keyword array must be a string", dataset_name)
                    elif len(kw.strip()) == 0:
                        add_error(errs, 5, "Invalid Required Field Value", "A keyword in the keyword array was an empty string.", dataset_name)
                    
            # bureauCode
            # (skip if this is known to not be a federal dataset)
            if item.get("_is_federal_dataset", True) == True and \
               check_required_field(item, "bureauCode", list, dataset_name, errs):
                for bc in item["bureauCode"]:
                    if not isinstance(bc, (str, unicode)):
                        add_error(errs, 5, "Invalid Required Field Value", "Each bureauCode must be a string", dataset_name)
                    elif ":" not in bc:
                        add_error(errs, 5, "Invalid Required Field Value", "The bureau code \"%s\" is invalid. Start with the agency code, then a colon, then the bureau code." % bc, dataset_name)
                    elif bc not in omb_burueau_codes:
                        add_error(errs, 5, "Invalid Required Field Value", "The bureau code \"%s\" was not found in our list." % bc, dataset_name)
                
            # modified
            check_date_field(item, "modified", dataset_name, errs)
            
            # publisher
            check_string_field(item, "publisher", 1, dataset_name, errs)
            
            # contactPoint
            check_string_field(item, "contactPoint", 3, dataset_name, errs)
            
            # mbox
            if check_string_field(item, "mbox", 3, dataset_name, errs):
                if not email_validator(item["mbox"]):
                    add_error(errs, 5, "Invalid Required Field Value", "The email address \"%s\" is not a valid email address." % item["mbox"], dataset_name)
            
            # identifier
            if check_string_field(item, "identifier", 1, dataset_name, errs):
                if item["identifier"] in seen_identifiers:
                    add_error(errs, 5, "Invalid Required Field Value", "The dataset identifier \"%s\" is used more than once." % item["identifier"], dataset_name)
                seen_identifiers.add(item["identifier"])
                
            # programCode
            # (don't bother reporting a missing programCode if no bureauCode is set)
            if isinstance(item.get("bureauCode"), list) and check_required_field(item, "programCode", list, dataset_name, errs):
                for s in item["programCode"]:
                    if not isinstance(s, (str, unicode)):
                        add_error(errs, 5, "Invalid Required Field Value", "Each value in the programCode array must be a string", dataset_name)
                    elif len(s.strip()) == 0:
                        add_error(errs, 5, "Invalid Required Field Value", "A value in the programCode array was an empty string.", dataset_name)
                
            # accessLevel
            if check_string_field(item, "accessLevel", 0, dataset_name, errs):
                if item["accessLevel"] not in ("public", "restricted public", "non-public"):
                    add_error(errs, 5, "Invalid Required Field Value", "The field 'accessLevel' had an invalid value: \"%s\"" % item["accessLevel"], dataset_name)
                elif item["accessLevel"] == "non-public":
                    add_error(errs, 1, "Possible Private Data Leakage", "A dataset appears with accessLevel set to \"non-public\".", dataset_name)
            
            # Required-If-Applicable
            
            # accessLevelComment
            if item.get("accessLevel") != "public":
                check_string_field(item, "accessLevelComment", 10, dataset_name, errs)
            
            # accessURL & webService
            if check_url_field(False, item, "accessURL", dataset_name, errs):
                if item.get("accessURL") and isinstance(item.get("distribution"), list):
                    # If accessURL and distribution are both given, the accessURL must be one of the distributions.
                    for d in item["distribution"]:
                        if d.get("accessURL") == item.get("accessURL"):
                            break # found
                    else: # not found
                        add_error(errs, 20, "Where's the Dataset?", "If a top-level 'accessURL' and 'distribution' are both present, the accessURL must be one of the distributions.", dataset_name)

            check_url_field(False, item, "webService", dataset_name, errs)
            if item.get("accessLevel") == "public" and item.get("accessURL") is None and item.get("webService") is None:
                add_error(errs, 20, "Where's the Dataset?", "A public dataset has neither an accessURL nor a webService.", dataset_name)

            # the first entry should be an entry for the catalog itself
            if i == 0 and item.get("accessURL") != src_url:
                add_error(errs, 2, "File Format Issues", "The first entry in the data.json file should be for the data.json file itself. Its accessURL should match the URL \"%s\"." % src_url, dataset_name)

            # format
            if item.get("accessURL") is None:
                if item.get("format") != None:
                    add_error(errs, 50, "Invalid Field Value", "Datasets without an 'accessURL' should not have a 'format'.", dataset_name)
            elif check_string_field(item, "format", -1, dataset_name, errs):
                check_mime_type(item.get("format"), "format", dataset_name, errs)
                            
            # license
            if item.get("license") is None:
                add_error(errs, 75, "Check Required-If-Applicable Fields", "Add a 'license' field to datasets. This field is required-if-applicable.", dataset_name)
            elif not isinstance(item.get("license"), (str, unicode)):
                add_error(errs, 50, "Invalid Field Value (Optional Fields)", "The field 'license' must be a string value if specified.", dataset_name)
            
            # spatial
            # TODO: There are more requirements than it be a string.
            if item.get("spatial") is None:
                add_error(errs, 75, "Check Required-If-Applicable Fields", "Add a 'spatial' field to datasets. This field is required if the dataset is spatial in nature.", dataset_name)
            elif not isinstance(item.get("spatial"), (str, unicode)):
                add_error(errs, 50, "Invalid Field Value (Optional Fields)", "The field 'spatial' must be a string value if specified.", dataset_name)
                
            # temporal
            if item.get("temporal") is None:
                add_error(errs, 75, "Check Required-If-Applicable Fields", "Add a 'temporal' field to datasets. This field is required if the dataset is temporal in nature.", dataset_name)
            elif not isinstance(item["temporal"], (str, unicode)):
                add_error(errs, 50, "Invalid Field Value (Optional Fields)", "The field 'temporal' must be a string value if specified.", dataset_name)
            elif "/" not in item["temporal"]:
                add_error(errs, 50, "Invalid Field Value (Optional Fields)", "The field 'temporal' must be two dates separated by a forward slash.", dataset_name)
            else:
                d1, d2 = item["temporal"].split("/", 1)
                if not ISO8601_REGEX.match(d1):
                    add_error(errs, 50, "Invalid Field Value (Optional Fields)", "The field 'temporal' has an invalid start date: %s." % d1, dataset_name)
                if not ISO8601_REGEX.match(d2):
                    add_error(errs, 50, "Invalid Field Value (Optional Fields)", "The field 'temporal' has an invalid end date: %s." % d2, dataset_name)
            
            # Expanded Fields
            
            # theme
            if item.get("theme") is None:
                add_error(errs, 90, "Add Suggested Fields to Improve Data Quality", "Add a 'theme' field to datasets.", dataset_name)
            elif not isinstance(item["theme"], list):
                add_error(errs, 50, "Invalid Field Value (Optional Fields)", "The field 'theme' must be an array.", dataset_name)
            else:
                for s in item["theme"]:
                    if not isinstance(s, (str, unicode)):
                        add_error(errs, 50, "Invalid Field Value (Optional Fields)", "Each value in the theme array must be a string", dataset_name)
                    elif len(s.strip()) == 0:
                        add_error(errs, 50, "Invalid Field Value (Optional Fields)", "A value in the theme array was an empty string.", dataset_name)
            
            # dataDictionary
            if check_url_field(False, item, "dataDictionary", dataset_name, errs):
                if item.get("dataDictionary") is None:
                    add_error(errs, 120, "Add Other Optional Fields (Suggested)", "Add a 'dataDictionary' field to datasets.", dataset_name)
            
            # dataQuality
            if item.get("dataQuality") is None:
                add_error(errs, 120, "Add Other Optional Fields (Suggested)", "Add a 'dataQuality' field to datasets.", dataset_name)
            elif not isinstance(item["dataQuality"], bool):
                add_error(errs, 50, "Invalid Field Value (Optional Fields)", "The field 'theme' must be true or false, as a JSON boolean literal (not the string \"true\" or \"false\").", dataset_name)
                
            # distribution
            if item.get("distribution") is None:
                pass # not required, and missing just means there's only one access URL and it's in the accessURL field
            elif not isinstance(item["distribution"], list):
                add_error(errs, 50, "Invalid Field Value (Optional Fields)", "The field 'distribution' must be an array, if present.", dataset_name)
            else:
                if len(item["distribution"]) > 0 and item.get("accessURL") is None:
                    add_error(errs, 10, "Missing Required Fields", "The 'accessURL' field is missing on a dataset with one or more distributions.", dataset_name)
                    
                for j, d in enumerate(item["distribution"]):
                    resource_name = dataset_name + (" distribution %d" % (j+1))
                    check_url_field(True, d, "distribution accessURL", resource_name, errs)
                    if check_string_field(d, "format", 1, resource_name, errs):
                        check_mime_type(d["format"], "distribution format", resource_name, errs)
                
            # accrualPeriodicity
            if item.get("accrualPeriodicity") is None:
                add_error(errs, 90, "Add Suggested Fields to Improve Data Quality", "Add a 'accrualPeriodicity' field to datasets.", dataset_name)
            elif item.get("accrualPeriodicity") not in ACCRUAL_PERIODICITY_VALUES:
                add_error(errs, 50, "Invalid Field Value (Optional Fields)", "The field 'accrualPeriodicity' had an invalid value.", dataset_name)
            
            # landingPage
            if check_url_field(False, item, "landingPage", dataset_name, errs):
                if item.get("landingPage") is None:
                    add_error(errs, 90, "Add Suggested Fields to Improve Data Quality", "Add a 'landingPage' field to datasets.", dataset_name)
            
            # language
            if item.get("language") is None:
                add_error(errs, 120, "Add Other Optional Fields (Suggested)", "Add a 'language' field to datasets.", dataset_name)
            elif not isinstance(item["language"], list):
                add_error(errs, 50, "Invalid Field Value (Optional Fields)", "The field 'language' must be an array, if present.", dataset_name)
            else:
                for s in item["language"]:
                    if not LANGUAGE_REGEX.match(s):
                        add_error(errs, 50, "Invalid Field Value (Optional Fields)", "The field 'language' had an invalid language: \"%s\"" % s, dataset_name)
                    
            # PrimaryITInvestmentUII
            if item.get("PrimaryITInvestmentUII") is None:
                add_error(errs, 120, "Add Other Optional Fields (Suggested)", "Add a 'PrimaryITInvestmentUII' field to datasets.", dataset_name)
            elif not isinstance(item["PrimaryITInvestmentUII"], (str, unicode)):
                add_error(errs, 50, "Invalid Field Value (Optional Fields)", "The field 'PrimaryITInvestmentUII' must be a string, if present.", dataset_name)
                
            # references
            if item.get("references") is None:
                add_error(errs, 120, "Add Other Optional Fields (Suggested)", "Add a 'references' field to datasets.", dataset_name)
            elif not isinstance(item["references"], list):
                add_error(errs, 50, "Invalid Field Value (Optional Fields)", "The field 'references' must be an array, if present.", dataset_name)
            else:
                for s in item["references"]:
                    if not URL_REGEX.match(s):
                        add_error(errs, 50, "Invalid Field Value (Optional Fields)", "The field 'references' had an invalid URL: \"%s\"" % s, dataset_name)
            
            # issued
            if item.get("issued") is None:
                add_error(errs, 90, "Add Suggested Fields to Improve Data Quality", "Add a 'issued' field to datasets.", dataset_name)
            else:
                check_date_field(item, "issued", dataset_name, errs)
            
            # systemOfRecords
            # TODO: No details in the schema!
    
    # Form the output data.
    for err_type in sorted(errs):
        errors_array.append( (
            err_type[1], # heading
            [ err_item + (" (%d locations)" % len(errs[err_type][err_item]) if len(errs[err_type][err_item]) else "")
              for err_item in sorted(errs[err_type], key=lambda x:(-len(errs[err_type][x]), x))
            ]) )
    
def add_error(errs, severity, heading, description, context=None):
    s = errs.setdefault((severity, heading), { }).setdefault(description, set())
    if context: s.add(context)

def nice_type_name(data_type):
    if data_type == (str, unicode) or data_type in (str, unicode):
        return "a string"
    elif data_type == list:
        return "an array"
    else:
        return "a " + str(data_type)

def check_required_field(obj, field_name, data_type, dataset_name, errs, display_field_name=None):
    # checks that a field exists and has the right type
    if not display_field_name: display_field_name = field_name
    if field_name not in obj:
        add_error(errs, 10, "Missing Required Fields", "The '%s' field is missing." % display_field_name, dataset_name)
        return False
    elif obj[field_name] is None:
        add_error(errs, 10, "Missing Required Fields", "The '%s' field is set to null." % display_field_name, dataset_name)
        return False
    elif not isinstance(obj[field_name], data_type):
        add_error(errs, 5, "Invalid Required Field Value", "The '%s' field must be %s but it is %s." % (display_field_name, nice_type_name(data_type), nice_type_name(type(obj[field_name]))), dataset_name)
        return False
    elif isinstance(obj[field_name], list) and len(obj[field_name]) == 0:
        add_error(errs, 10, "Missing Required Fields", "The '%s' field is an empty array." % display_field_name, dataset_name)
        return False
    return True

def check_string_field(obj, field_name, min_length, dataset_name, errs):
    # checks that a required field exists, is typed as a string, and has a minimum length
    if not check_required_field(obj, field_name, (str, unicode), dataset_name, errs):
        return False
    elif len(obj[field_name].strip()) == 0:
        add_error(errs, 10, "Missing Required Fields", "The '%s' field is present but empty." % field_name, dataset_name)
        return False
    elif len(obj[field_name].strip()) <= min_length:
        add_error(errs, 100, "Are These Okay?", "The '%s' field is very short: \"%s\"" % (field_name, obj[field_name]), dataset_name)
        return False
    return True
    
def check_date_field(obj, field_name, dataset_name, errs):
    # checks that a required date field exists and looks like a date
    if not check_required_field(obj, field_name, (str, unicode), dataset_name, errs):
        return False
    elif len(obj[field_name].strip()) == 0:
        add_error(errs, 10, "Missing Required Fields", "The '%s' field is present but empty." % field_name, dataset_name)
        return False
    else:
        if not ISO8601_REGEX.match(obj[field_name]):
            add_error(errs, 5, "Invalid Required Field Value", "The '%s' field has an invalid ISO 8601 date or date-time value: \"%s\"." % (field_name, obj[field_name]), dataset_name)
            return False
    return True
    
def check_url_field(required, obj, field_name, dataset_name, errs):
    # checks that a required or optional field, if specified, looks like a URL
    
    display_field_name = field_name
    field_name = field_name.split(" ")[-1] # turn 'distribution accessURL' into 'accessURL', leave other field names unchanged
    
    if not required and (field_name not in obj or obj[field_name] is None): return True # not required, so OK
    if not check_required_field(obj, field_name, (str, unicode), dataset_name, errs, display_field_name=display_field_name): return False # for a non-required field, just checking data type
    if not URL_REGEX.match(obj[field_name]):
        add_error(errs, 5, "Invalid Required Field Value", "The '%s' field has an invalid URL: \"%s\"." % (display_field_name, obj[field_name]), dataset_name)
        return False
    return True

def check_mime_type(format, field_name, dataset_name, errs):
    if format.lower() in ("csv", "xls", "xml", "rdf", "json", "xlsx", "text", "api", "feed"):
        add_error(errs, 5, "Update Your File!", "The '%s' field used to be a file extension but now it must be a MIME type." % field_name, dataset_name)
    elif not MIMETYPE_REGEX.match(format):
        add_error(errs, 5, "Invalid Required Field Value", "The '%s' field has an invalid MIME type: \"%s\"." % (field_name, format), dataset_name)
    elif format.split(";")[0] not in COMMON_MIMETYPES:
        # if there's an optional parameter like "; charset=UTF-8" chop it off before checking the COMMON_MIMETYPES list
        add_error(errs, 100, "Are These Okay?", "The '%s' field has an unusual MIME type: \"%s\"" % (field_name, format), dataset_name)
        
