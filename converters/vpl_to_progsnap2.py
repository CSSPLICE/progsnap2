import zipfile
import argparse


def load_vpl_submissions(submissions_filename):
    '''
    Arguments:
        submissions_filename (str): The file path to the zip file.
    '''
    if not zipfile.is_zipfile(submissions_filename):
        raise Exception("I expected a Zipfile for "+str(submissions_filename))
    zipped = zipfile.ZipFile(submissions_filename)
    students = set()
    for name in sorted(zipped.namelist()):
        print(name)
        r_dir = name.split('/')
        student = r_dir[0]
        students.add(student)

def load_vpl_events(events_filename):
    pass

def load_vpl_logs(events_filename, submissions_filename):
    '''
    Arguments:
        events_filename (str): The file path to the CSV file
        submissions_filename (str): The file path to the zip file.
    '''
    submissions = load_vpl_submissions(submissions_filename)
    events = load_vpl_events(events_filename)
    #return list(students)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert event logs from VPL into the progsnap2 format.')
    parser.add_argument('events', type=str,
                        help='The events CSV source filename for the course.')
    parser.add_argument('submissions', type=str,
                        help='The submissions zip file source filename for the assignment.')
    parser.add_argument('--target', dest='target',
                        default=None,
                        help='The filename or directory to save this in.')
    parser.add_argument('--unzipped', dest='unzipped',
                        default=False, action='store_true',
                        help='Create an unzipped directory instead of a zipped file.')

    args = parser.parse_args()
    
    data = load_vpl_logs(args.events, args.submissions)
    print(data)
