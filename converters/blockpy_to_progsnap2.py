'''
A command line tool for turning blockpy logs into ProgSnap2 format

Refer to:
    Protocol Draft: https://docs.google.com/document/d/1bZPu8LIUPOfobWsCO_9ayi5LC9_1wa1YCAYgbKGAZfA/edit#
    CodeState Representation: https://docs.google.com/document/d/1FZHBcHYAG9uC9tRdhyoPIsCrJZP_jUSNXTswDHCi-ys/edit#
    
TODO:
    I could have done more to decouple the zipfile reading from the ProgSnap2
    class, which would probably make this more reusable for others.
'''

import zipfile
import tarfile
import json
import argparse
import os
import sys
import csv
import io
import shutil
import random
import re
from datetime import datetime
from collections import Counter
from pprint import pprint

#try:
#    from tqdm import tqdm
#except:
#    print("TQDM is not installed")
#    tqdm = list
from tqdm import tqdm

BLOCKPY_INSTANCE = 'BPY4'
ENCODING = 'utf8'
DUMMY_CODE_STATES_DIR = "__CodeStates__"
TEMPORARY_DIRECTORY = "__temp__"

# Some events trigger at distinct timestamps, so we arbitrarily order
# certain events over others.
ARBITRARY_EVENT_ORDER = [
    'Session.Start',
    'X-File.Upload',
    'File.Edit',
    'Submit',
    'Compile',
    'Run.Program',
    'Compile.Error',
    'Feedback.Grade',
    'Intervention'
]
# When writing out columns, we want them in a certain order to make the
# whole thing more readable
ARBITRARY_COLUMN_ORDER = ['EventID', 'Order', 'SubjectID', 'AssignmentID', 'CourseID',
                          'EventType', 'CodeStateID',
                          'ParentEventID',
                          'ClientTimestamp',
                          'Score',
                          'EditType',
                          'CompileMessageType', 'CompileMessageData', 'SourceLocation',
                          'ExecutionResult',
                          'ProgramErrorOutput',
                          'InterventionType',
                          'InterventionMessage',
                          'ServerTimestamp', 'ToolInstances']
                          
class UnclassifiedEventType(Exception):
    pass

class Event:
    '''
    Representation of a given event.
    
    Attributes:
        EventType (str): Taken from parameter
        EventID (int): Assigned from an auto-incrementing counter
        Order (int): Assigned after all the events are created.
        SubjectID (str): Taken from parameter
        tool_instances (str): Taken from global constant
        CodeStateID (int): The current code state for this event.
        ServerTimestamp (str): Taken from parameter
        EVENT_ID (int): Unique, auto-incrementing ID for the events
    '''
    MAX_EVENT_ID = 0
    def __init__(self, ClientTimestamp, SubjectID, EventType, AssignmentID,
                 ServerTimestamp, Score=None, CodeStateID=None, **kwargs):
        self.ClientTimestamp = ClientTimestamp
        self.ServerTimestamp = ServerTimestamp
        self.SubjectID = SubjectID
        self.AssignmentID = AssignmentID
        self.EventType = EventType
        self.CodeStateID = CodeStateID
        self.Score = Score
        self.ToolInstances = BLOCKPY_INSTANCE
        self._optional_parameters = kwargs
        # Keep track of events
        self.EventID = self._track_new_event()
        # Private fields not related to dataset
        self.Order = None
    
    @classmethod
    def _track_new_event(cls):
        new_event_id = cls.MAX_EVENT_ID
        cls.MAX_EVENT_ID += 1
        return new_event_id
    
    def set_ordering(self, Order, CodeStateID=None):
        '''
        This method is meant to update the relative attributes after all the
        events have been processed and ordered appropriately.
        
        Arguments:
            Order (int): The new order for this event
            CodeStateID (int|None): The new code state for this event.
        '''
        self.Order = Order
        if CodeStateID is not None:
            self.CodeStateID = CodeStateID
    
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
        required_columns = {COLUMN: getattr(self, COLUMN)
                            for COLUMN in ARBITRARY_COLUMN_ORDER
                            if hasattr(self, COLUMN)}
        parameter_values.update(required_columns)
        sorted_parameters = sorted(parameter_values.items(),
                                   key=lambda i: Event.get_parameter_order(i[0]))
        ordered_values = [value for parameter, value in sorted_parameters]
        return ordered_values
    
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
        try:
            return (self.ClientTimestamp, ARBITRARY_EVENT_ORDER.index(self.EventType))
        except ValueError:
            return (self.ClientTimestamp, len(ARBITRARY_EVENT_ORDER))
    
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
            #print(ARBITRARY_COLUMN_ORDER.index(parameter))
            return (ARBITRARY_COLUMN_ORDER.index(parameter), parameter)
        return (len(ARBITRARY_COLUMN_ORDER), parameter)
        
def _make_file(filename):
    return open(filename, 'w', newline='', encoding=ENCODING)

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
        self.main_table_header = list(ARBITRARY_COLUMN_ORDER)
        self.main_table = []
        
        self.code_files = {'': 0} #{tuple(): 0}
        self.CODE_ID = 1
    
    def export(self, directory):
        '''
        Create a concrete, on-disk representation of this event database.
        
        Arguments:
            directory (str): The location to store the generated files.
        '''
        self.report("Exporting Metadata")
        self.export_metadata(directory)
        self.report("Exporting Main Table")
        self.export_main_table(directory)
        self.report("Exporting CodeState files")
        self.export_code_states(directory)
    
    def export_metadata(self, directory):
        '''
        Create the metadata table, which is more or less a constant file.
        
        Arguments:
            directory (str): The location to store the generated files.
        '''
        metadata_filename = os.path.join(directory, "DatasetMetadata.csv")
        with _make_file(metadata_filename) as metadata_file:
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
        with _make_file(main_table_filename) as main_table_file:
            main_table_writer = csv.writer(main_table_file, 
                                           **self.csv_writer_options)
            self.finalize_table()
            optionals = Event.distill_parameters(self.main_table)
            header = self.main_table_header
            header.sort(key=Event.get_parameter_order)
            main_table_writer.writerow(header)
            for row in self.main_table:
                finalized_row = row.finalize(optionals)
                main_table_writer.writerow(finalized_row)
                
    def _new_code_states_directory(self, directory):
        '''
        Creates the CodeStates directory in the given `directory`. If the
        CodeStates folder is already there, it wipes it (using a trick) to
        prevent windows from fussing.
        
        Args:
            directory (str): The location to make the new CodeStates directory.
        '''
        code_states_dir = os.path.join(directory, "CodeStates")
        # Remove any existing CodeStates in this directory
        if os.path.exists(code_states_dir):
            # Avoid bug on windows where a handle is sometimes kept
            dummy_dir = os.path.join(directory, DUMMY_CODE_STATES_DIR)
            os.rename(code_states_dir, dummy_dir)
            shutil.rmtree(dummy_dir)
        os.mkdir(code_states_dir)
        return code_states_dir
    
    def export_code_states(self, directory):
        '''
        Create the CodeStates directory and all of the code state files,
        organized by their unique ID.
        
        Arguments:
            directory (str): The location to store the generated files.
        '''
        code_states_dir = self._new_code_states_directory(directory)
        for files, CodeStateID in tqdm(self.code_files.items()):
            code_state_dir = os.path.join(code_states_dir, str(CodeStateID))
            if not os.path.exists(code_state_dir):
                os.mkdir(code_state_dir)
            if isinstance(files, str):
                code_state_filename = os.path.join(code_state_dir, '__main__.py')
                with _make_file(code_state_filename) as code_state_file:
                    code_state_file.write(files)
            else:
                for filename, contents in files:
                    code_state_filename = os.path.join(code_state_dir, filename)
                    with _make_file(code_state_filename) as code_state_file:
                        code_state_file.write(contents)
                        
    def report(self, *messages):
        print(*messages)
    
    def finalize_table(self):
        '''
        Sort the timestamps of the events.
        Add in order (and CodeStateID if it's missing)
        '''
        self.main_table.sort(key= Event.get_order)
        
        BAD_EVENTS = set()
        
        self.report("Fixing any Upload events to happen BEFORE the next File.Edit")
        remapped_uploads = {}
        FOUND_UPLOADS = 0
        REMAPPED_UPLOADS = 0
        for event in self.main_table:
            identifier = (event.SubjectID, event.AssignmentID)
            if event.EventType == 'X-File.Upload':
                remapped_uploads[identifier] = event
                FOUND_UPLOADS += 1
            elif event.EventType == 'File.Edit':
                if identifier in remapped_uploads:
                    remapped_uploads[identifier].CodeStateID = event.CodeStateID
                    REMAPPED_UPLOADS += 1
                    del remapped_uploads[identifier]
        self.report("Found", FOUND_UPLOADS, "uploads, remapped", REMAPPED_UPLOADS)
        if remapped_uploads:
            aids = Counter()
            sids = Counter()
            for (SID, AID), event in remapped_uploads.items():
                self.report("\tUnmatched Upload:", SID, AID, event.ClientTimestamp)
                aids[AID] += 1
                sids[SID] += 1
            print(aids.items())
            print(sids.items())
        
        # Go fetch first code_states for everything
        self.report("Finding initial code states")
        first_code_states = {}
        for event in self.main_table:
            identifier = (event.SubjectID, event.AssignmentID)
            if identifier in first_code_states:
                continue
            current_code_state = first_code_states.get(identifier, 0)
            if event.CodeStateID is not None:
                first_code_states[identifier] = event.CodeStateID
        
        # Attach Compile.Error events to latest relevant compile
        self.report("Attaching parent Compile events to Compile.Error events")
        reattached, unattached = 0, 0
        previous_compile = {}
        for event in self.main_table:
            identifier = (event.SubjectID, event.AssignmentID)
            if event.EventType == 'Compile':
                previous_compile[identifier] = event.EventID
            elif event.EventType == 'Compile.Error':
                if identifier not in previous_compile:
                    unattached += 1
                else:
                    reattached += 1
                event.ParentEventID = previous_compile.get(identifier, -1)
        print("Reattached:", reattached, ", Failed on", unattached, ", Unfinished:", len(previous_compile))
        
        # Fix order attribute, make sure CodeStateID is correct
        self.report("Fixing order/CodeStateID based on last File.Edit events")
        order = 0
        CodeStateID = 0
        code_states = {}
        score_state = {}
        for event in self.main_table:
            identifier = (event.SubjectID, event.AssignmentID)
            if identifier not in first_code_states:
                first_code_states[identifier] = 0
            current_code_state = code_states.get(identifier, first_code_states[identifier])
            current_score_state = score_state.get(identifier, 0)
            if event.Score is None:
                event.Score = current_score_state
            else:
                current_score_state = event.Score
            if event.CodeStateID is None:
                event.set_ordering(order, current_code_state)
            else:
                current_code_state = event.CodeStateID
                event.set_ordering(order)
            order += 1
            code_states[identifier] = current_code_state
            score_state[identifier] = current_score_state
        
        # Filter out bad events
        self.report("Filtering out", len(BAD_EVENTS), "bad events")
        if BAD_EVENTS:
            for i, event in enumerate(self.main_table):
                if event.EventID in BAD_EVENTS:
                    del self.main_table[i]
    
    def log_event(self, **kwargs):
        '''
        Add in a new event to the ProgSnap2 instance.
        
        Arguments:
            when (str): the timestamp to use when ordering these events.
                        Currently using the ClientTimestamp.
            SubjectID (str): Uniquely identifying user id.
            EventID (str): An EventType, such as the ones documented for
                            the standard.
            kwargs (dict[str:Any]): Any optional columns for this row; the
                                    keys must match to actual columns in
                                    the progsnap standard (e.g., ParentEventID)
        Returns:
            Event: The newly created event
        '''
        new_event = Event(**kwargs)
        self.main_table.append(new_event)
        return new_event
    
    def log_code_state(self, submission):
        '''
        Add in a Submit event, which has associated code in the zip file.
        
        Arguments:
            submission (str or dict[str:str]): A dictionary that maps
                                               local filenames to their
                                               absolute path in the zip
                                               file. Alternatively, the raw
                                               string of the code.
            zipped (ZipFile): A zipfile that has the students' code in it.
        Returns:
            Event: The newly created event
        '''
        if isinstance(submission, str):
            code = submission
        else:
            code = []
            for filepath, full in submission.items():
                contents = load_file_contents(zipped, full)
                code.append((filepath, contents))
            code = tuple(sorted(code))
        return self.hash_code_directory(code)
    
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
            CodeStateID = self.code_files[code]
        else:
            CodeStateID = self.CODE_ID
            self.code_files[code] = self.CODE_ID
            self.CODE_ID += 1
        return CodeStateID
        
def blockpy_timestamp_to_iso8601(timestamp):
    '''
    Converts blockpy style timestamps into an ISO-8601 compatible timestamp.
    
    > blockpy_timestamp_to_iso8601(2018-10-31-12-02-25)
    2018-10-31T12:02:25
    
    Arguments:
        timestamp (str): A blockpy-style timestamp
    Returns:
        str: The ISO-8601 timestamp.
    '''
    return datetime.fromtimestamp(int(timestamp)).isoformat()

def add_path(structure, path, limit_depth=1):
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
    depth = 0
    while len(components) > 1:
        current = components.pop(0)
        if current not in structure:
            structure[current] = {}
        structure = structure[current]
        depth += 1
        if depth > limit_depth:
            break
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

def load_zipfile(input_filename, extraction_directory):
    needed_files = ['log.json']
    compressed = zipfile.ZipFile(input_filename)
    for need in needed_files:
        target = extraction_directory+"/"+need
        if os.path.exists(target.strip()):
            yield need, target
            continue
        for potential_path in ['db/'+need, need]:
            names = {zip_info.filename:zip_info for zip_info in compressed.infolist()}
            if potential_path in names:
                member = names[potential_path]
                member.filename = os.path.basename(member.filename)
                compressed.extract(need, extraction_directory)
                yield need, target
                break
        else:
            raise Exception("Could not find log.json in given file: "+input_filename)
                           
def load_tarfile(input_filename, extraction_directory):
    needed_files = ['log.json']
    compressed = tarfile.open(input_filename)
    for need in needed_files:
        target = extraction_directory+"/"+need
        # TODO: Doesn't work - why?
        if os.path.exists(target.strip()):
            yield need, target
            continue
        # Otherwise, we need to extract it
        for potential_path in ['db/'+need, need]:
            names = [tar_info.name for tar_info in compressed.getmembers()]
            if potential_path in names:
                member = compressed.getmember(potential_path)
                member.name = os.path.basename(member.name)
                compressed.extract(need, extraction_directory, set_attrs=False)
                yield need, target
                break
        else:
            raise Exception("Could not find log.json in given file: "+input_filename)
    
def make_directory(directory):
    # Remove any existing CodeStates in this directory
    if os.path.exists(directory):
        # Avoid bug on windows where a handle is sometimes kept
        dummy_dir = directory+"_old"
        os.rename(directory, dummy_dir)
        shutil.rmtree(dummy_dir)
    os.mkdir(directory)
    return directory
    
def chomp_iso_time_decimal(a_time):
    a_time = "T".join(a_time.split())
    if '.' in a_time:
        return a_time[:a_time.find('.')]
    else:
        return a_time
        
line_finder = re.compile(r"line (\d+)")

def map_blockpy_event_to_progsnap(event, action, body):
    if event == 'code' and action == 'set':
        return {'EventType': "File.Edit", 'EditType': "GenericEdit"}
    # NOTE: We treat the feedback delivered to the student as the actual run
    elif event == 'engine' and action == 'on_run':
        return 'Compile'
    elif event == 'editor':
        if action == 'load':
            return 'Session.Start'
        elif action == 'reset':
            return {'EventType': "File.Edit", 'EditType': "Reset"}
        elif action == 'blocks':
            return 'X-View.Blocks'
        elif action == 'text':
            return 'X-View.Text'
        elif action == 'split':
            return 'X-View.Split'
        elif action == 'instructor':
            return 'X-View.Settings'
        elif action == 'history':
            return 'X-View.History'
        elif action == 'trace':
            return 'X-View.Trace'
        elif action == 'upload':
            return 'X-File.Upload'
        elif action == 'download':
            return 'X-File.Download'
        elif action in ('changeIP', 'change'):
            return 'X-Session.Move'
        elif action == 'import':
            return 'X-Dataset.Import'
        elif action in ('run', 'on_run'):
            # NOTE: Don't care about redundant news that "run" button was clicked
            return None
    elif event == 'trace_step':
        return 'X-View.Step'
    elif event == 'feedback':
        if action.lower().startswith('analyzer|'):
            return {'EventType': "Intervention",
                    'InterventionType': "Analyzer",
                    'InterventionMessage': action+"|"+body}
        
        elif action.lower() == 'editor error' or action.lower().startswith('syntax|'):
            lines = line_finder.findall(body)
            return {'EventType': "Compile.Error",
                    'CompileMessageType': action, 'CompileMessageData': body,
                    'SourceLocation': lines[0] if lines else ""}
        
        elif action.lower().startswith('complete|'):
            return {'EventType': "Run.Program",
                    'ExecutionResult': "Success",
                    'Score': 1}
        elif action.lower().startswith('runtime|') or action.lower() == 'runtime':
            return {'EventType': "Run.Program",
                    'ExecutionResult': "Error",
                    'ProgramErrorOutput': action+"|"+body}
        elif action.lower() == 'internal error':
            return {'EventType': "Run.Program",
                    'ExecutionResult': "SystemError",
                    'ProgramErrorOutput': action+"|"+body}
        
        return {'EventType': "Intervention", 'InterventionType': "Feedback",
                'InterventionMessage': action+"|"+body}
    elif event == 'engine':
        # NOTE: Don't care about the engine trigger events?
        # TODO: Luke probably cares about this, we may have to jury rig a way
        #       to attach it to the proper feedback result.
        return None
    elif event == 'instructor':
        # NOTE: Don't care about instructors editing assignments
        return None
    elif event == 'trace':
        # NOTE: Don't care about redundant activation of tracer
        return None
    elif event == 'worked_examples':
        # NOTE: Don't care about worked_examples in BlockPy
        return None
    raise UnclassifiedEventType((event, action, body))

def log_blockpy_event(progsnap, record):
    # Skip events without timestamps
        #return (record['event'], record['action'])
    # Gather local variables
    event = record['category'] if 'category' in record else record['event']
    action = record['label'] if 'label' in record else record['action']
    if not record['timestamp'] or record['timestamp'] == 'None':
        return (event, action)
    body = record['body']
    ClientTimestamp = blockpy_timestamp_to_iso8601(record['timestamp'])
    ServerTimestamp = chomp_iso_time_decimal(record['date_created'])
    SubjectID = record['user_id']
    AssignmentID = record['assignment_id']
    CourseID = record.get('course_id', 0)
    # Process event types
    progsnap_event = map_blockpy_event_to_progsnap(event, action, body)
    # Wrap strings with dictionaries
    if progsnap_event == None:
        return (event, action)
        #return (record['event'], record['action'])
    if isinstance(progsnap_event, str):
        progsnap_event = {'EventType': progsnap_event}
    # File edits get code states
    CodeStateID = None
    if progsnap_event['EventType'] == "File.Edit":
        CodeStateID = progsnap.log_code_state(body)
    # And actually log the event
    progsnap.log_event(ClientTimestamp=ClientTimestamp,
                       SubjectID=SubjectID,
                       AssignmentID=AssignmentID,
                       CodeStateID=CodeStateID,
                       ServerTimestamp=ServerTimestamp,
                       CourseID=CourseID,
                       ParentEventID="",
                       **progsnap_event)
                       
    # And done
    return (event, action)

def load_blockpy_events(progsnap, input_filename, target):
    '''
    Open up a submission file downloaded from blockpy and process its events,
    putting all the events into the progsnap instance.
    
    Arguments:
        progsnap (ProgSnap2): The progsnap instance to log events to.
        submissions_filename (str): The file path to the zip file.
    '''
    filesystem = {}
    
    # Open data file appropriately
    temporary_directory = make_directory(TEMPORARY_DIRECTORY)
    if zipfile.is_zipfile(input_filename):
        progsnap.report("Opening ZIP data file")
        data_files = load_zipfile(input_filename, temporary_directory)
    elif tarfile.is_tarfile(input_filename):
        progsnap.report("Opening TAR data file")
        data_files = load_tarfile(input_filename, temporary_directory)
    else:
        progsnap.report("Opening JSON data file")
        data_files = [('log.json', input_filename)]
    for name, path in data_files:
        with open(path) as data_file:
            filesystem[name] = json.load(data_file)
    #pprint(filesystem['log.json'][:10])
    types = Counter()
    for event in filesystem['log.json']:
        EventType = log_blockpy_event(progsnap, event)
        types[EventType] += 1
    #pprint(dict(types.items()))
    

def load_blockpy_logs(input_filename, target="exported/"):
    '''
    Load all the logs from the given files.
    
    Arguments:
        input_filename (str): The file path to the zipped file
        target (str): The directory to store all the generated files in.
    '''
    progsnap = ProgSnap2()
    load_blockpy_events(progsnap, input_filename, target)
    progsnap.export(target)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert event logs from BlockPy into the progsnap2 format.')
    parser.add_argument('input', type=str,
                        help='The dumped database zip.')
    parser.add_argument('--target', dest='target',
                        default="exported/",
                        help='The filename or directory to save this in.')
    parser.add_argument('--unzipped', dest='unzipped',
                        default=False, action='store_true',
                        help='Create an unzipped directory instead of a zipped file.')

    args = parser.parse_args()
    
    
    load_blockpy_logs(args.input, args.target)
    
