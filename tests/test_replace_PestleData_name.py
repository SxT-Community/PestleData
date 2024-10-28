import sys, logging
from pathlib import Path

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


response = input('This script may modify the contents and/or names of files in the current directory. Press type "y" to continue: ')
if response != 'y': 
    logger.info('...run cancelled.')
    sys.exit(0)

logger.info('\n\n...started!')


def insensitive_replace(content:str,find:str, replace:str):
    while find.lower() in content.lower():
        pos1 = content.lower().find(find.lower())
        pos2 = pos1 + len(find)
        content = content[:pos1] + replace + content[pos2:]
    return content
 

def replace_file_content_and_names(topfolder:Path, find:str, replace:str = 'PestleData'):

    updates = {'file content':0, 'file name':0, 'folder name':0}

    # recursively find all files in a directory:
    logger.info(f'Finding files in: {str(topfolder)}')
    files = [r for r in list(topfolder.rglob("*.*")) if '/.git' not in str(r)]

    # apply replacements to all file contents
    for file in files:

        # skip any files that are prefixed with '_'
        if file.name.startswith('_') or file.name in['_.DS_Store']: continue
        
        # read the file contents
        with open(file, 'r') as f:
            content = f.read()

        # with all replacements made, write the contents back to the file
        if find.lower() in content.lower():
            logger.info(f'Updating content of file: {file.name}')
            updates['file content'] += 1
            with open(file, 'w') as f:
                f.write(insensitive_replace(content, find, replace))


    # apply replacements to all filenames
    for file in files:

        # skip any files that are prefixed with '_'
        if file.name.startswith('_'): continue
        
        # rename the file
        if find.lower() in file.name.lower():
            logger.info(f'Updating name of file: {file.name}')
            updates['file name'] += 1
            file.rename(file.parent / insensitive_replace(file.name, find, replace))
     

    # apply replacements to all foldernames
    files = [str(r) for r in list(topfolder.rglob("*.*")) if '/.git' not in str(r)]
    files.sort(key=len, reverse=True) # sort by len (for folder names)
    folders = list(set([Path(f).parent for f in files]))

    for folder in folders:
        
        # rename the folder
        if find.lower() in folder.name.lower():
            logger.info(f'Updating name of file: {folder.name}')
            updates['folder name'] += 1
            folder.rename(insensitive_replace(folder.name, find, replace))
    
    logger.info(f'Updates: {updates}')


# actually run the function: 
if len(sys.argv)==2:
    find = sys.argv[1]
    replace_file_content_and_names(Path(__file__).parent.parent, find)
else:
    logger.error("ValueError: Please provide a 'Find' string as an argument.")

logger.info('...finished!\n')