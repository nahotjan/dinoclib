import io
import csv
import codecs
import typing
import logging
import pathlib

from py7zr import SevenZipFile, Bad7zFile

def _root_folder_name(volume_id, snapshot_id):
  if snapshot_id == "{00000000-0000-0000-0000-000000000000}":
    return volume_id
  else:
    return f'{volume_id} (vss {snapshot_id})'

def _write_file(
  file_path: pathlib.Path,
  content: io.BytesIO
) -> bool:

  # Write file
  try:
    # Check if already exists if yes warning
    if file_path.is_file():
      logging.warning('File %s already exists', file_path)
      return False

    # Create parent directory
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(file_path, 'wb') as file_d:
      file_d.write(content.getbuffer())
  
  except (OSError, Exception) as err:
    logging.warning('Can\'t write file %s\n%s', file_path, err)
    return False
  
  return True

def _parse_getthis(
  getthis_content: io.BytesIO
) -> dict:

  result = {}

  getthis = csv.DictReader(codecs.getreader('utf-8-sig')(getthis_content))

  for row in getthis:
    result[row['SampleName'].replace('\\', '/')] = pathlib.Path(_root_folder_name(row['VolumeID'], row['SnapshotID']), row['FullName'].replace('\\', '/')[1:])

  return result

def _rename_volumes(
  destination_path: pathlib.Path
) -> None:

  # Get drive letter of VolumeID from volstats reports
  for volstats_file in destination_path.glob('**/volstats.csv'):

     with open(volstats_file, 'r', newline='', encoding='utf-8-sig') as volstats_fd:
      
      volstats = csv.DictReader(volstats_fd)

      for row in volstats:
        if row.get('MountPoint', ''):
          for dir in destination_path.glob(f'{row["VolumeID"]}*'):
            dir.rename(
              dir.parent.joinpath(
                dir.name.replace(row['VolumeID'], row['MountPoint'][0])
              )
            ) 

def extract( 
  archive_path: typing.Union[pathlib.Path, io.BytesIO],
  destination_path: pathlib.Path,
  default_password: str = "",
  rename_volumes: bool = True,
  archive_name: str = ""
) -> bool:
  """
  Extract all artifacts from a DFIR-Orc archive.
  The function rebuild the file tree and rename files with their true name.
  Using this function allow parsers to work like if running on dumped drive.

  An output exemple could be:

  <destination_path>
     ├─ orc_outputs
     |  ├─ commands
     |  └─ logs
     ├─ C
     |  ├─ Program Files
     |  ├─ Users
     |  ...
     |  └─ Windows
     ├─ C (vss XXXXX)
     |  ├─ ...
     |  └─ ...
     └─ D
        ├─ ...
        └─ ...

  Note: Due to the the intensive IO ressources needed, it is prefered to not use network
  folder for both archive_path and destination_path values.

  TODO: Handle p7b archives
  TODO: Handle password protected
  TODO: Check if we use timestomping when writng the file

  Args:
    archive_path: DFIR-Orc archive file (7z).
    destination_path: Destination to extract the file.
    default_password: The password to use if the archive is protected.
    rename_volumes: Indicates if the function should rename the volumes.
    archive_name: The name of the current archive parsed. Mainly used by the recursive called
                  when facing a nexted 7z archive.

  Returns:
    Output format as a string.
  """
  cmd_ouptuts_path = destination_path.joinpath('orc_outputs', 'commands')
  logs_path = destination_path.joinpath('orc_outputs', 'logs')
  script_logs_path = destination_path.joinpath('non_extracted.log')
  getthis_mapping = {}

  archive_name = archive_path.name if isinstance(archive_path, pathlib.Path) else archive_name

  # Open archive
  #TODO: Handle p7b archives
  #TODO: Handle password protected
  try:
    archive = SevenZipFile(archive_path)
    
    if archive.needs_password():
      archive.close()
      archive = SevenZipFile(archive_path, password=default_password)
  except Bad7zFile:
    logging.warning('%s is not a valid 7z file', archive_name)
    return False

  # Read all the content
  files = archive.readall()

  script_logs_path.parent.mkdir(parents=True, exist_ok=True)
  script_log = open(script_logs_path, 'a')

  # Check if there is a GetThis.csv
  if 'GetThis.csv' in files.keys():
    getthis_mapping = _parse_getthis(files['GetThis.csv'])

  for filename, file_content in files.items():

    final_path = ""

    if filename in getthis_mapping.keys():
      final_path = destination_path.joinpath(getthis_mapping[filename])

    elif filename.endswith('.log'):
      final_path = logs_path.joinpath(filename)

    elif filename in ['GetThis.csv', 'Statistics.json']:
      final_path = logs_path.joinpath(archive_name[:-3] + '_' + filename)

    elif filename.endswith('.7z'):
      extract(file_content, destination_path, default_password, False, archive_name=filename)
      continue

    else:
      final_path = cmd_ouptuts_path.joinpath(filename)

    # Write file 
    if not _write_file(final_path, file_content):
      script_log.write(f'{archive_name},{filename},{final_path}\n')
      pass

  archive.close()
  script_log.close()

  # Rename volumes with mapped letter
  if rename_volumes:
    _rename_volumes(destination_path)

  return True
