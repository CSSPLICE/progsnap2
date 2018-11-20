import zipfile
import argparse


def load_vpl_logs(filename):
    '''
    Arguments:
        filename (str): The file path to the zip file.
    '''
    if not zipfile.is_zipfile(filename):
        raise Exception("I expected a Zipfile for "+str(filename))
    zipped = zipfile.ZipFile(filename)
    students = set()
    for name in sorted(zipped.namelist()):
        print(name)
        r_dir = name.split('/')
        student = r_dir[0]
        students.add(student)
    #return list(students)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert event logs from VPL into the progsnap2 format.')
    parser.add_argument('source', type=str,
                       help='The source filename')
    parser.add_argument('--target', dest='target',
                       default=None,
                       help='The filename or directory to save this in.')
    parser.add_argument('--unzipped', dest='unzipped',
                       default=False, action='store_true',
                       help='Create an unzipped directory instead of a zipped file.')

    args = parser.parse_args()
    
    data = load_vpl_logs(args.source)
    print(data)
