import simplejson as json
import urllib2, os, sys

#Change these variables based on environment
big_datajson_source = 'http://localhost/data.json'
enterprise_datajson_source = 'http://localhost/enterprisedata.json'
base_path = os.path.dirname(os.path.realpath(__file__))
dest_folder = 'organizations'
default_dest = os.path.join(base_path, dest_folder)

def main(dest=None):
    """
    Build out the JSON file for each organization
    """
    output_dir = dest if dest else default_dest

    #Make sure the destination folder exists
    if not dest and not os.path.exists(default_dest):
        os.makedirs(default_dest)

    #Get the contents of the big data.json file
    response = None
    try:
        response = urllib2.urlopen(big_datajson_source)
    except urllib2.URLError:
        #Fall back on default
        response = urllib2.urlopen('http://localhost:5000/data.json')

    datasets = json.load(response)

    #group datasets by organization
    datasets_grouped_by_org = {}
    for dataset in datasets:
        org = dataset['organization']
        if org in datasets_grouped_by_org:
            datasets_grouped_by_org[org].append(dataset)
        else:
            datasets_grouped_by_org[org]=[dataset]

    #write the organization.json files
    for org_name, org_datasets in datasets_grouped_by_org.iteritems():
        with open(os.path.join(output_dir,org_name + '.json'), 'w') as outfile:
            json.dump(org_datasets, outfile)

if __name__=="__main__":
    if len(sys.argv)>2:
        print "build_organizations.py <optional:/path/to/output/folder>"

    if len(sys.argv)==2:
        if not os.path.exists(sys.argv[1]):
            print "Path does not exist: {0}".format(sys.argv[1])
        elif not os.path.isdir(sys.argv[1]):
            print "Not a directory: {0}".format(sys.argv[1])
        else:
            main(sys.argv[1])
    else:
        main()
        
def enterprise_main(dest=None):
    """
    Build out the JSON file for each organization
    """
    output_dir = dest if dest else default_dest

    #Make sure the destination folder exists
    if not dest and not os.path.exists(default_dest):
        os.makedirs(default_dest)

    #Get the contents of the big data.json file
    response = None
    try:
        response = urllib2.urlopen(enterprise_datajson_source)
    except urllib2.URLError:
        #Fall back on default
        response = urllib2.urlopen('http://localhost:5000/enterprisedata.json')

    datasets = json.load(response)

    #group datasets by organization
    datasets_grouped_by_org = {}
    for dataset in datasets:
        org = dataset['organization']
        if org in datasets_grouped_by_org:
            datasets_grouped_by_org[org].append(dataset)
        else:
            datasets_grouped_by_org[org]=[dataset]

    #write the organization.json files
    for org_name, org_datasets in datasets_grouped_by_org.iteritems():
        with open(os.path.join(output_dir,org_name + '_enterprise.json'), 'w') as outfile:
            json.dump(org_datasets, outfile)

if __name__=="__main__":
    if len(sys.argv)>2:
        print "build_organizations.py <optional:/path/to/output/folder>"

    if len(sys.argv)==2:
        if not os.path.exists(sys.argv[1]):
            print "Path does not exist: {0}".format(sys.argv[1])
        elif not os.path.isdir(sys.argv[1]):
            print "Not a directory: {0}".format(sys.argv[1])
        else:
            main(sys.argv[1])
    else:
        main()