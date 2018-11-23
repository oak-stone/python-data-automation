
#------------------------------------------------
# BUILT FOR PYTHON 3.x
#------------------------------------------------
#import librarys

#data analysis
import pandas as PD
import numpy

#FTP access
from ftplib import FTP, error_perm, FTP_TLS

#current directories etc
import glob, os, sys

#database connect
import psycopg2 as pg
from sqlalchemy import create_engine
#------------------------------------------------
#import itertools, sys
#spinner = itertools.cycle(['-', '/', '|', '\\'])
#sys.stdout.write(next(spinner))  # write the next character
#sys.stdout.flush()                # flush stdout buffer (actual character display)
#sys.stdout.write('\r\b')            # erase the last written char
filesFound = []


#creating the file types we might be looking for
class FileType:
    def __init__(self, givenExtension, includeInSearch):
        self.extension = givenExtension
        self.search = includeInSearch

csv = FileType(".csv", True)
xml = FileType(".xml", False)


#an optional progress bar to use within the code (otherwise it might seem that the script has chrashed without visual feedback)
def progressBar(current, total, full_progbar_length):
     frac = current/total
     filled_progbar = round(frac * full_progbar_length)
     sys.stdout.flush()  #to prevent the print function freezing from time to time
     print('\r\033[0;32;40m', '#'*filled_progbar + '-'*(full_progbar_length-filled_progbar), '[{:>7.2%}]'.format(frac), end ="\033[0;37;40m")     #special characters \r, \033 and :>7.2% are used to overwrite the previous print, make the bar green and have the % at the end the same length at all times, respectively


#defining fuction to download the appropriate file(s)
def download_file(file):
    print("\nDownloading  %s" %file, end = " | ")
    with open(os.getcwd() + "/files/" + file, 'wb') as file_handle: #using while to automatically close the file as well, even if the write failes
        ftps.retrbinary("RETR %s" %file, file_handle.write)
    print("\033[0;32;40m finished\033[0;37;40m")


def find_files(ftps, dirpath):
    #progressBarCount = 0    #reset count after entering a new directory
    prev_dir = ftps.pwd()
    try:
        ftps.cwd(dirpath)
    except error_perm as e:
        print(str(e))
        return # ignore ones we cannot enter
    fileListing = ftps.nlst()   #get the folders and files in current directory
    dirLength = len(fileListing)    #see how many files are in the directory to set our progress bar end point
    print("\r Looking through: " + str(ftps.pwd()), end = "") #just a visual echo of the current working ftp directory
    for file in fileListing:
        #progressBarCount += 1
        #progressBar(progressBarCount, dirLength, 20)
        if not(file.startswith('_')):
            if(file.endswith(xml.extension)):
                if (xml.search == True):
                    filesFound.append(file)
                    #print("\n Found: " + file)
                    #download_file(file)
                else:
                    continue
            if(file.endswith(csv.extension)):
                if (csv.search == True):
                    filesFound.append(file)
                    #print("\n Found: " + file)
                    #download_file(file)
                else:
                    continue
            else:
                find_files(ftps, file)
        else:
            continue
    if(ftps.pwd() != "/"):
        ftps.cwd(prev_dir)
    else:
        print("\n\033[0;32;40mFinished. \n\033[0;37;40mFound a total of: " + str(len(filesFound)) + " matching files.")
        print("Downloaded the following files: " + str(filesFound))


#connect to postgresql database and write the downloaded files to tables
def connect_to_database(database_path):
    try:
        engine = create_engine(database_path)
    except sqlalchemy.all_errors as e:
        print("\033[0;31;40mCould not connect to database.")
        print(str(e))
    else:
        write_to_database(engine)
    finally:
        engine.dispose()

def write_to_database(engine):
    path = (os.getcwd() + "/files")
    os.chdir(path)
    fileList = os.listdir(os.curdir)
    #print("Current listing of files: %s" %fileList)
    if(len(fileList) > 0):
        for file in fileList:
            if(file.endswith('.csv') and file.startswith('')):
                df = PD.read_csv(file, encoding = "latin1")
                for columnName in df.columns:
                      if(('opened' in columnName) or ('closed' in columnName)):
                          df[columnName] = PD.to_datetime(df[columnName])
                      elif('percentage' in columnName):
                          df[columnName] = df[columnName].str.extract('(\d+)')
                          df[columnName] = PD.to_numeric(df[columnName], downcast='float')
                      else:
                          continue
                print("\nAdding %s to database" %file, end = " | ")
                #df.to_sql('sla', engine, if_exists="append")
                print(" success")
                print("Removing %s from current directory" %file, end = " | ")
                os.remove(file)
                print(" removed")
                fileList = os.listdir(os.curdir)
            elif(file.endswith('.csv')):
                df = PD.read_csv(file, encoding = "latin1")
                for columnName in df.columns:
                      if(('opened' in columnName) or ('closed' in columnName)):
                          df[columnName] = PD.to_datetime(df[columnName])
                      elif('percentage' in columnName):
                          df[columnName] = df[columnName].str.extract('(\d+)')
                          df[columnName] = PD.to_numeric(df[columnName], downcast='float')
                      else:
                          continue
                print("\nAdding %s to database" %file, end = " | ")
                #df.to_sql('inc', engine, if_exists="append")
                print("\033[0;32;40m success\033[0;37;40")
                print("Removing %s from current directory" %file, end = " | ")
                os.remove(file)
                print("\033[0;32;40m removed\033[0;37;40")
                fileList = os.listdir(os.curdir)
            else:
                print("\033[0;31;40mThere aren't any .csv files in %s\033[0;37;40m" %path)
        print("\033[0;32;40mAll files successfully added to database and removed from local directory!\033[0;37;40m")
    else:
        print("\033[0;31;40mThere are no files in %s to add to database. Check your script again.\033[0;37;40m" %path)


#defining fuction to connect to ftp
def establish_ftp_connection(host, user, password):
    try:
        ftps = FTP_TLS(host)    #trying to connect to host address
        ftps.login(user,password) and ftps.prot_p()
        print("Connected to %s " %host)

    except ftplib.all_errors as e:  #ready to handle exception
        print("\033[0;31;40mCould not connect. Error code: " + str(e) + "\033[0;37;40")
        ftps.close()

    else:
        print("\033[0;37;40mLooking for files in FTP directories...")
        find_files(ftps, ftps.pwd())    #look for files in ftp
        ask_for_approval = input("Should the listed files be imported to database? Y/N ")
        if(ask_for_approval == "Y" or ask_for_approval == "y"):
            connect_to_database("")
        elif(ask_for_approval == "N" or ask_for_approval == "n"):
            print("The files have not been imported to the database.")
        else:
            print("Invalid input. Closing the script.")
    finally:
        ftps.close()


if __name__ == ("__main__"):
    establish_ftp_connection('', '', '')
