from __future__ import print_function
import pickle
import os
import logging
import pdb
from hashlib import md5
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/drive.file',
          'https://www.googleapis.com/auth/drive.appdata']

# Includes these directories in the backup
DIRS = {
    'Documents': 'C:\\Users\\Charles\\Documents',
    'Music': 'C:\\Users\\Charles\\Music',
    'Pictures': 'C:\\Users\\Charles\\Pictures',
	'Videos': 'C:\\Users\\Charles\\Videos',
	'Beat_Saber_Levels': 'C:\\Program Files (x86)\\Steam\\steamapps\\common\\Beat Saber\\Beat Saber_Data\\CustomLevels'
}

IGNORED_EXTENSIONS = [
    'ini',
    'lnk'
]

def main():
    service = login()
    for dirname in DIRS.keys():
        dir = DIRS.get(dirname)
        folder = os.path.abspath(dir) # make sure folder is absolute
        folder_ids = {}
        # Walk the entire folder tree and upload each file
        for foldername, subfolders, filenames in os.walk(folder):
            logging.debug("Moved onto new directory")
            logging.debug([foldername, subfolders, filenames])
            request = None
            if (dir == foldername):
                request = service.files().list(
                    pageSize=1,
                    q="name='{0}' and starred=true".format(dirname),
                    fields="nextPageToken, files(id, name, parents)"
                ).execute()
                folder_ids[dir] = request.get('files')[0].get('id')
                logging.debug('Folder ID for ' + dirname)
                logging.debug(folder_ids.get(dir))
            files_metadata = list_files(service, folder_ids.get(foldername))
            for file in filenames:
                location = foldername + '\\' + file
                metadata = next((item for item in files_metadata if item['name'] == file), {})
                if not check_file(location, metadata.get('md5Checksum')):
                    upload_file(service, location, folder_ids.get(foldername), metadata.get('id'))
                try:
                    files_metadata.remove(metadata)
                except:
                    pass
            drive_folders = get_folders(service, folder_ids.get(foldername))
            for folder in subfolders:
                remote_folder = find_folder(drive_folders, folder)
                new_id = foldername + '\\' + folder
                if (remote_folder):
                    folder_ids[new_id] = remote_folder
                else:
                    folder_ids[new_id] = create_folder(service, new_id, folder_ids)
                try:
                    drive_folders.remove({'id': remote_folder, 'name': folder})
                except:
                    pass
            cleanup_deleted_files(service, drive_folders + files_metadata)

def paginate(service, query, fields):
    results = []
    files = service.files()
    request = files.list(q=query, fields='nextPageToken, ' + fields)

    while request is not None:
        files_doc = request.execute()
        results = results + files_doc.get('files')
        request = files.list_next(request, files_doc)
    
    return results

def list_files(service, parent):
    query = "not trashed=true and mimeType!='application/vnd.google-apps.folder' and '{0}' in parents".format(parent)
    fields = "files(id, name, md5Checksum)"
    results = paginate(service, query, fields)
    logging.debug('file list dictionary')
    logging.debug(results)
    return results

def check_file(file_location, remote_hash):
    logging.info('Results for {0}:'.format(file_location))
    extension = file_location.split('.')[1] if len(file_location.split('.')) > 1 else ''
    if extension in IGNORED_EXTENSIONS:
        logging.info('Skipping file with invalid extension.')
        return True
    m = md5()
    with open(file_location, "rb") as f:
        local_hash = None
        if remote_hash:
            while True:
                chunk = f.read(8192)
                if len(chunk):
                    m.update(chunk)
                else:
                    break
            local_hash = m.digest().hex()
            logging.debug('local hash:  ' + (local_hash or ''))
            logging.debug('remote hash: ' + (remote_hash or ''))
        if (not local_hash):
            logging.info('Could not find remote file.')
        elif (local_hash == remote_hash):
            logging.info('Hash match!')
        else:
            logging.info('No hash match!')
        return local_hash and local_hash == remote_hash
    
def find_folder(list, fname):
    for folder in list:
        if (folder.get('name') == fname):
            return folder.get('id')

def get_folders(service, parent):
    query = "not trashed=true and mimeType='application/vnd.google-apps.folder' and '{0}' in parents".format(parent)
    fields = 'files(id, name, md5Checksum)'
    drive_folders = paginate(service, query, fields)
    logging.debug('Searching for drive folders')
    logging.debug(drive_folders)
    return drive_folders

def create_folder(service, location, folder_ids):
    name = location.split('\\')[-1].replace('\'', '\\\'')
    directory = location[:location.rindex('\\')]
    folder_body = {
        'name': name,
        'parents': [folder_ids.get(directory)],
        'mimeType': 'application/vnd.google-apps.folder'
    }
    logging.debug('Creating new folder: ' + name)
    logging.debug(folder_body)
    request = service.files().create(
        body=folder_body,
        fields='id'
    ).execute()
    return request.get('id')

def upload_file(service, location, parent, id):
    name = location.split('\\')[-1].replace('\'', '\\\'')
    media = MediaFileUpload(location, chunksize=2048*2048, resumable=True)
    metadata = { 'name': name, 'parents': [parent] }
    if id is not None:
        logging.info("Updating old Drive file...")
        metadata = { 'name': name }
        service.files().update(
            fileId=id,
            body=metadata,
            media_body=media
        ).execute()
    else:
        logging.info("Creating {0} with {1} in parents...".format(location, parent))
        metadata = { 'name': name, 'parents': [parent] }
        service.files().create(
            body=metadata,
            media_body=media
        ).execute()

def cleanup_deleted_files(service, files):
    # Run cleanup with less files if over limit
    if (len(files) > 1000):
        cleanup_deleted_files(service, files[1000:])
        files = files[:1000]
    # Remove old files that were deleted locally
    batch = service.new_batch_http_request()
    for file in files:
        logging.debug("Deleting file/folder: " + file.get('name'))
        batch.add(service.files().delete(
            fileId=file.get('id'),
            fields="files(id)"
        ))
    batch.execute()

def login():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds)

if __name__ == '__main__':
    logging.basicConfig(filename="backup.log", filemode="w", encoding="utf-8", level=logging.DEBUG)
    try:
        main()
    except Exception as e:
        logging.exception(e)
