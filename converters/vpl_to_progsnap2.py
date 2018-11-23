'''
A command line tool for turning VPL logs into ProgSnap2 format

Refer to:
    Protocol Draft: https://docs.google.com/document/d/1bZPu8LIUPOfobWsCO_9ayi5LC9_1wa1YCAYgbKGAZfA/edit#
    CodeState Representation: https://docs.google.com/document/d/1FZHBcHYAG9uC9tRdhyoPIsCrJZP_jUSNXTswDHCi-ys/edit#
    
TODO:
    I could have done more to decouple the zipfile reading from the ProgSnap2
    class, which would probably make this more reusable for others.
'''

import zipfile
import argparse
import os
import sys
import csv
import io
import shutil
from pprint import pprint

VPL_INSTANCE = 'VPL 3.3.1'
ENCODING = 'utf-8'
DUMMY_CODE_STATES_DIR = "__CodeStates__"

# Some events trigger at distinct timestamps, so we arbitrarily order
# certain events over others.
ARBITRARY_EVENT_ORDER = [
    'Submit',
    'Compile',
    'Compile.Error',
    'Program.Run',
    'Program.Test',
    'Feedback.Grade',
]

# When writing out columns, we want them in a certain order to make the
# whole thing more readable
ARBITRARY_COLUMN_ORDER = ['EventID', 'Order', 'SubjectID',
                          'EventType', 'CodeStateID',
                          'ServerTimestamp', 'ToolInstances']

class Event:
    '''
    Representation of a given event.
    
    Attributes:
        event_type (str): Taken from parameter
        event_id (int): Assigned from an auto-incrementing counter
        order (int): Assigned after all the events are created.
        subject_id (str): Taken from parameter
        tool_instances (str): Taken from global constant
        code_state_id (int): The current code state for this event.
        server_timestamp (str): Taken from parameter
        EVENT_ID (int): Unique, auto-incrementing ID for the events
    '''
    EVENT_ID = 0
    def __init__(self, server_timestamp, subject_id, event_type, **kwargs):
        self.server_timestamp = server_timestamp
        self.subject_id = subject_id
        self.event_type = event_type
        self._optional_parameters = kwargs
        self.event_id = Event.EVENT_ID
        Event.EVENT_ID += 1
        self.code_state_id = None
        self.order = None
    
    def set_ordering(self, order, code_state_id=None):
        '''
        Ironically, this does not set the `order` attribute, which is a
        more or less absolute representation of time. Instead this method is
        meant to update the relative attributes after all the events have
        been processed and ordered appropriately.
        
        Arguments:
            order (int): The new order for this event
            code_state_id (int|None): The new code state for this event.
        '''
        self.order = order
        if code_state_id is not None:
            self.code_state_id = code_state_id
    
    def finalize(self, default_parameter_values):
        '''
        Fill in any missing optional parameters for this row, sort the all
        parameters into the right order.
        
        Arguments:
            default_parameter_values (dict[str: Any]): A dictionary of the
                                                       default values for
                                                       all of the optional
                                                       parameters.
        '''
        # Avoid mutating original
        parameter_values = dict(default_parameter_values)
        parameter_values.update(self._optional_parameters)
        sorted_parameters = sorted(parameter_values.items(),
                                   key=lambda i: Event.get_parameter_order(i[0]))
        ordered_values = [value for parameter, value in sorted_parameters]
        return [self.event_id, self.order, self.subject_id, 
                self.event_type, self.code_state_id,
                self.server_timestamp, VPL_INSTANCE] + ordered_values
    
    @staticmethod
    def distill_parameters(events):
        '''
        Given a set of events, finds all of the optional parameters by
        unioning the parameters of all the events.
        
        Arguments:
            events (list[Event]): The events to distill all the parameters
                                    from.
        Returns:
            dict[str:str]: The mapping of parameters to empty strings.
                           TODO: The plan was to have default values, but
                                 that seems unnecessary now. Maybe should
                                 just be a set instead?
        '''
        optional_parameters = set()
        for event in events:
            optional_parameters.update(event._optional_parameters)
        return {p:"" for p in optional_parameters}
    
    def get_order(self):
        '''
        Create a value representing the absolute position of a given
        event. Useful as a key function for a sorting.
        
        Returns:
            str: The timestamp
        '''
        return self.server_timestamp
    
    @staticmethod
    def get_parameter_order(parameter):
        '''
        Identifies what order this parameter should go in. Useful as a key
        function for sorting. It uses the ARBITRARY_COLUMN_ORDER, but if
        the number isn't found, then the sorting will rely on
        alphabetical ordering of the parameters.
        
        Arguments:
            parameter (str): A column name for a ProgSnap file.
        
        Returns:
            tuple[int,str]: A pair of the arbitrary column order and the
                            parameter's value, allowing you to break ties with
                            the latter.
        '''
        if parameter in ARBITRARY_COLUMN_ORDER:
            return (ARBITRARY_COLUMN_ORDER.index(parameter), parameter)
        return (len(ARBITRARY_COLUMN_ORDER), parameter)

class ProgSnap2:
    '''
    A representation of the ProgSnap2 data file being generated.
    
    Directory is a tuple of N files, where each element of the tuple is a
        tuple having a filename and contents paired together. This allows us
        to hash directories of files and perform deduplication.
        
    Attributes
        main_table (list[Event]): The current list of events.
        main_table_header (list[str]): The default headers for the table.
        csv_writer_options (dict[str:str]): Options to pass to the CSV
                                            writer, to maintain some
                                            flexibility for later.
        code_files (dict[Directory: str]): The dictionary mapping the
                                           filename/contents to the code
                                           instance IDs.
        CODE_ID (int): The auto-incrementing ID to apply to new codes.
        VERSION (int): The current Progsnap Standard Version
    '''
    VERSION = 3
    def __init__(self, csv_writer_options=None):
        if csv_writer_options is None:
            csv_writer_options = {'delimiter': ',', 'quotechar': '"',
                                  'quoting': csv.QUOTE_MINIMAL}
        self.csv_writer_options = csv_writer_options
        # Actual data contents
        self.main_table_header = ARBITRARY_COLUMN_ORDER
        self.main_table = []
        
        self.code_files = {tuple(): 0}
        self.CODE_ID = 1
    
    def export(self, directory):
        '''
        Create a concrete, on-disk representation of this event database.
        
        Arguments:
            directory (str): The location to store the generated files.
        '''
        self.export_metadata(directory)
        self.export_main_table(directory)
        self.export_code_states(directory)
    
    def export_metadata(self, directory):
        '''
        Create the metadata table, which is more or less a constant file.
        
        Arguments:
            directory (str): The location to store the generated files.
        '''
        metadata_filename = os.path.join(directory, "DatasetMetadata.csv")
        with open(metadata_filename, 'w', newline='') as metadata_file:
            metadata_writer = csv.writer(metadata_file, 
                                         **self.csv_writer_options)
            metadata_writer.writerow(['Property', 'Value'])
            metadata_writer.writerow(['Version', self.VERSION])
            metadata_writer.writerow(['AreEventsOrdered', 'true'])
            metadata_writer.writerow(['IsEventOrderingConsistent', 'true'])
            metadata_writer.writerow(['CodeStateRepresentation', 'Directory'])
    
    def export_main_table(self, directory):
        '''
        Create the main table file.
        
        Arguments:
            directory (str): The location to store the generated files.
        '''
        main_table_filename = os.path.join(directory, "MainTable.csv")
        with open(main_table_filename, 'w', newline='') as main_table_file:
            main_table_writer = csv.writer(main_table_file, 
                                           **self.csv_writer_options)
            self.finalize_table()
            optionals = Event.distill_parameters(self.main_table)
            header = self.main_table_header + list(optionals.keys())
            header.sort(key=Event.get_parameter_order)
            main_table_writer.writerow(header)
            for row in self.main_table:
                finalized_row = row.finalize(optionals)
                main_table_writer.writerow(finalized_row)
    
    def export_code_states(self, directory):
        '''
        Create the CodeStates directory and all of the code state files,
        organized by their unique ID.
        
        Arguments:
            directory (str): The location to store the generated files.
        '''
        code_states_dir = os.path.join(directory, "CodeStates")
        # Remove any existing CodeStates in this directory
        if os.path.exists(code_states_dir):
            # Avoid bug on windows where a handle is sometimes kept
            dummy_dir = os.path.join(directory, DUMMY_CODE_STATES_DIR)
            os.rename(code_states_dir, dummy_dir)
            shutil.rmtree(dummy_dir)
        os.mkdir(code_states_dir)
        for files, code_state_id in self.code_files.items():
            code_state_dir = os.path.join(code_states_dir, str(code_state_id))
            if not os.path.exists(code_state_dir):
                os.mkdir(code_state_dir)
            for filename, contents in files:
                code_state_filename = os.path.join(code_state_dir, filename)
                with open(code_state_filename, 'w', encoding=ENCODING) as code_state_file:
                    code_state_file.write(contents)
    
    def finalize_table(self):
        '''
        Sort the timestamps of the events.
        Add in order (and code_state_id if it's missing)
        '''
        self.main_table.sort(key= Event.get_order)
        # Fix order attribute, make sure code_state_id is correct
        order = 0
        code_state_id = 0
        subject_code_states = {}
        for event in self.main_table:
            current_code_state = subject_code_states.get(event.subject_id, 0)
            if event.code_state_id is None:
                event.set_ordering(order, current_code_state)
            else:
                current_code_state = event.code_state_id
                event.set_ordering(order)
            order += 1
            subject_code_states[event.subject_id] = current_code_state
    
    def log_event(self, when, subject_id, event_type, **kwargs):
        '''
        Add in a new event to the ProgSnap2 instance.
        
        Arguments:
            when (str): the timestamp to use when ordering these events.
                        Currently using the ServerTimestamp.
            subject_id (str): Uniquely identifying user id.
            event_id (str): An EventType, such as the ones documented for
                            the standard.
            kwargs (dict[str:Any]): Any optional columns for this row; the
                                    keys must match to actual columns in
                                    the progsnap standard (e.g., ParentEventID)
        Returns:
            Event: The newly created event
        '''
        new_event = Event(when, subject_id, event_type, **kwargs)
        self.main_table.append(new_event)
        return new_event
    
    def log_submit(self, when, subject_id, submission_directory, zipped):
        '''
        Add in a Submit event, which has associated code in the zip file.
        
        Arguments:
            when (str): the timestamp to use when ordering these events.
                        Currently using the ServerTimestamp.
            subject_id (str): Uniquely identifying user id.
            submission_directory (dict[str:str]): A dictionary that maps
                                                  local filenames to their
                                                  absolute path in the zip
                                                  file.
            zipped (ZipFile): A zipfile that has the students' code in it.
        Returns:
            Event: The newly created event
        '''
        new_event = self.log_event(when, subject_id, 'Submit')
        code = []
        for filepath, full in submission_directory.items():
            contents = load_file_contents(zipped, full)
            code.append((filepath, contents))
        code = tuple(sorted(code))
        new_event.code_state_id = self.hash_code_directory(code)
        return new_event
    
    def hash_code_directory(self, code):
        '''
        Take in a tuple of tuple of code files and hash them into unique IDs,
        returning the ID of this particularly given code file.
        Note: Currently hashing just based on order received - possibly need
        something more sophisticated?
        
        Arguments:
            code (tuple of tuple of str): A series of filename/contents paired
                                          into a tuple of tuples, sorted by
                                          filenames.
        Returns:
            int: A unique ID of the given code files.
        '''
        if code in self.code_files:
            code_state_id = self.code_files[code]
        else:
            code_state_id = self.CODE_ID
            self.code_files[code] = self.CODE_ID
            self.CODE_ID += 1
        return code_state_id
        
def vpl_timestamp_to_iso8601(timestamp):
    '''
    Converts VPL style timestamps into an ISO-8601 compatible timestamp.
    
    > vpl_timestamp_to_iso8601(2018-10-31-12-02-25)
    2018-10-31T12:02:25
    
    Arguments:
        timestamp (str): A VPL-style timestamp
    Returns:
        str: The ISO-8601 timestamp.
    '''
    date = timestamp[:10]
    time = timestamp[-8:].replace("-", ":")
    return date + "T" + time

def add_path(structure, path):
    '''
    Given a path and a structure representing a filesystem, parses the path
    to add the components in the appropriate place of the structure.
    
    Note: This modifies the given structure!
    
    TODO: This shouldn't actually dive into student code directories. Those
    should be "flat". We should either limit the depth or just unroll the loop.
    
    Structure:
        dict[str:Structure]: A folder with nesting
        dict[str:str]: A terminal level mapping to an absolute path name.
    
    Arguments:
        structure (Structure): The representation of the filesystem.
    '''
    components = path.split("/")
    while len(components) > 1:
        current = components.pop(0)
        if current not in structure:
            structure[current] = {}
        structure = structure[current]
    if components[0]:
        structure[components[0]] = path
        
def load_file_contents(zipped, path):
    '''
    Reads the contents of the zipfile, respecting Unicode encoding... I think.
    
    Arguments:
        zipped (ZipFile): A zipfile to read from.
        path (str): The path to the file in the zipfile.
    
    Returns:
        str: The contents of the file.
    '''
    data_file = zipped.open(path, 'r')
    data_file  = io.TextIOWrapper(data_file, encoding=ENCODING)
    return data_file.read()

def log_ceg(progsnap, student, timestamp, ceg_directory, zipped, parent_event):
    '''
    VPL stores CEG directories alongside the submission directories. I believe
    their name stands for "Compilation-Execution-Grade" which hints at the
    files stored within. Usually, there are four:
        compilation.txt: I believe this is any extra information spat out by
                         the compiler. Haven't tried running any C++/Java/etc
                         code to see what it does...
        execution.txt: Results from executing the students' code. Doesn't
                       indicate if it was a good or bad execution - it depends
                       on what was set up with the autograder.
        gradecomments.txt: This is the parsed output of reading the execution
                           results, looking for "<|-- ... --|>" comments
                           and grades. Not useful for my purposes.
        grade.txt: The numeric grade assigned to this compilation by the
                   autograder.
    
    Arguments:
        progsnap (ProgSnap2): The progsnap instance to log events to.
        student (str): The unique ID for the student.
        timestamp (str): A timestamp for when this event occurred.
        ceg_directory (dict[str:str]) A mapping of the localfilename to their
                                      absolute paths within the zipfile.
        zipped (ZipFile): The zip file to get data from.
        parent_event (Event): The events here are subordinate to a Submission
                              event; we need to get that event's ID.
    '''
    if 'execution.txt' not in ceg_directory:
        if 'compilation.txt' in ceg_directory:
            compile_message_data = load_file_contents(zipped, ceg_directory['compilation.txt'])
        else:
            compile_message_data = ""
        progsnap.log_event(timestamp, student, 'Compile.Error',
                           CompileMessageType='Error',
                           CompileMessageData=compile_message_data,
                           ParentEventID=parent_event.event_id)
    else:
        intervention_message = load_file_contents(zipped, ceg_directory['execution.txt'])
        progsnap.log_event(timestamp, student, 'Run.Program',
                           InterventionType='Feedback',
                           InterventionMessage=intervention_message,
                           ParentEventID=parent_event.event_id)
    if 'grade.txt' in ceg_directory:
        grade = load_file_contents(zipped, ceg_directory['grade.txt'])
        progsnap.log_event(timestamp, student, 'Feedback.Grade',
                           InterventionType='Grade',
                           InterventionMessage=grade,
                           ParentEventID=parent_event.event_id)

def load_vpl_submissions(progsnap, submissions_filename):
    '''
    Open up a submission file downloaded from VPL and process its events,
    putting all the events into the progsnap instance.
    
    Arguments:
        progsnap (ProgSnap2): The progsnap instance to log events to.
        submissions_filename (str): The file path to the zip file.
    '''
    if not zipfile.is_zipfile(submissions_filename):
        raise Exception("I expected a Zipfile for "+str(submissions_filename))
    zipped = zipfile.ZipFile(submissions_filename)
    filesystem = {}
    for name in zipped.namelist():
        add_path(filesystem, name)
    for student, student_directory in filesystem.items():
        sorted_student_directory = sorted(student_directory.items())
        for timestamp, submission_directory in sorted_student_directory:
            if timestamp.endswith('.ceg'):
                continue
            submission = progsnap.log_submit(vpl_timestamp_to_iso8601(timestamp), 
                                             student, submission_directory, zipped)
            ceg_path = timestamp+'.ceg'
            if ceg_path in student_directory:
                ceg_directory = student_directory[ceg_path]
                log_ceg(progsnap, student, vpl_timestamp_to_iso8601(timestamp),
                        ceg_directory, zipped, submission)
    #pprint(filesystem)

def load_vpl_events(progsnap, events_filename):
    '''
    Processes any interesting events from the VPL event file.
    
    TODO: I thought there'd be some more interesting stuff in here. But
          maybe when they use the Edit menu, it will be more interesting?
          Probably not.
    '''
    pass

def load_vpl_logs(events_filename, submissions_filename, target="exported/"):
    '''
    Load all the logs from the given files.
    
    Arguments:
        events_filename (str): The file path to the CSV file
        submissions_filename (str): The file path to the zip file.
        target (str): The directory to store all the generated files in.
    '''
    progsnap = ProgSnap2()
    submissions = load_vpl_submissions(progsnap, submissions_filename)
    events = load_vpl_events(progsnap, events_filename)
    progsnap.export(target)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert event logs from VPL into the progsnap2 format.')
    parser.add_argument('events', type=str,
                        help='The events CSV source filename for the course.')
    parser.add_argument('submissions', type=str,
                        help='The submissions zip file source filename for the assignment.')
    parser.add_argument('--target', dest='target',
                        default="exported/",
                        help='The filename or directory to save this in.')
    parser.add_argument('--unzipped', dest='unzipped',
                        default=False, action='store_true',
                        help='Create an unzipped directory instead of a zipped file.')

    args = parser.parse_args()
    
    
    load_vpl_logs(args.events, args.submissions, args.target)
    
