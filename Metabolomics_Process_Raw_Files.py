from Metabolomics_File_System import FileSystem
from Metabolomics_Database_SetUp import MetabDBSetUp
import os
import glob
import sys

class ProcessRawFile:

    def __init__(self, datafile, filesystem, database):
        self.fs = filesystem
        self.db = database
        self.datafile = datafile

    def run(self):
        # make and set folder for outfiles
        os.chdir(self.fs.out_dir)
        if not os.path.isdir(self.fs.out_dir + "\\" + self.datafile):
            os.makedirs(self.datafile)
        self.outfiles_dir = self.fs.out_dir + "\\" + self.datafile

        # get and insert data
        self.insert_qc_run_data()
        self.run_msconvert()
        self.create_xml()
        self.run_mzmine()
        self.insert_csv()
        print("Inserted Data for " + self.datafile)

    def run_msconvert(self):
        '''Creates .mzXML files in OutFiles'''

        os.chdir(self.fs.sw_dir + "\ProteoWizard")

        # run positive
        command = 'msconvert ' + '"' + self.fs.in_dir + '\\' + self.datafile + '.raw' + '"' \
                        + ' --filter ' + '"peakPicking true 1-"' + ' --filter ' + '"polarity positive"' \
                        + ' --mzXML -o ' + '"' + self.outfiles_dir + '"' + ' --outfile ' + '"' + self.datafile \
                        + '"' + '_pos'
        os.system(command)

        # run negative
        command = 'msconvert ' + '"' + self.fs.in_dir + '\\' + self.datafile + '.raw' + '"' \
                  + ' --filter ' + '"peakPicking true 1-"' + ' --filter ' + '"polarity negative"' \
                  + ' --mzXML -o ' + '"' + self.outfiles_dir + '"' + ' --outfile ' + '"' + self.datafile \
                  + '"' + '_neg'
        os.system(command)

    def create_xml(self):
        # replace in new_xml
        # create a new xml and write list to it
        pos_file = self.outfiles_dir + "\\" + self.datafile + "_pos.mzXML"
        neg_file = self.outfiles_dir + "\\" + self.datafile + "_neg.mzXML"
        neg_db = self.fs.main_dir + "\\" + self.fs.neg_db
        pos_db = self.fs.main_dir + "\\" + self.fs.pos_db
        batch = self.outfiles_dir + "\\" + self.datafile + ".mzmine"
        outfile_xml = self.outfiles_dir + "\\" + self.datafile + ".xml"
        output_file = "OutFiles" +  "\\" + self.datafile + "\\" + self.datafile + ".csv"

        new_xml = []
        os.chdir(self.fs.main_dir)
        with open(self.fs.xml_template, 'r') as infile:
            for line in infile:
                new_line = line.strip()
                new_line = new_line.replace('POSINPUTFILE', pos_file)
                new_line = new_line.replace('NEGINPUTFILE', neg_file)
                new_line = new_line.replace('POSDATABASEFILE', pos_db)
                new_line = new_line.replace('NEGDATABASEFILE', neg_db)
                new_line = new_line.replace('DIRECTORY', self.fs.main_dir)
                new_line = new_line.replace('OUTPUT', output_file)
                new_line = new_line.replace('SAMPLEBATCHNAME', batch)
                new_xml.append(new_line)


        with open(outfile_xml, 'w') as outfile:
            for line in new_xml:
                outfile.write(line + "\n")
                #print(line)

    def run_mzmine(self):
        os.chdir(self.fs.sw_dir + "\\" + "MZmine-2.32")
        command = 'startMZmine_Windows.bat ' + '"' + self.outfiles_dir + "\\"+ self.datafile + '.xml' + '"'
        os.system(command)

    def insert_csv(self):
        # insert metrics in order of table 1 - 9
        # TEST: expected from csv V table values
        run_id = self.db.get_run_id(self.datafile)
        with open(self.outfiles_dir + "\\"+ self.datafile + '.csv', 'r') as incsv:
            for line in incsv:
                in_data = line.strip().split("|")
                if in_data[0][0] != 'r': # skip first line
                    sql = "SELECT component_id FROM sample_component WHERE component_name = " + "'" + in_data[0] + "'"
                    try:
                        self.db.cursor.execute(sql)
                        comp_id = self.db.cursor.fetchone()
                    except Exception as e:
                        print(e)

                    if in_data[1] == 'ESTIMATED':
                        # insert positive mode
                        for i in range(1, 10):
                            ins_sql = "INSERT INTO measurement VALUES( " + "'" + str(i) + "', '" + str(comp_id[0]) \
                                    + "', '" + str(run_id) + "', '" + str(in_data[i + 1]) + "')"
                            try:
                                self.db.cursor.execute(ins_sql)
                                self.db.db.commit()
                            except Exception as e:
                                print(e)
                                print(i)
                    elif in_data[1] == 'UNKNOWN':
                        # insert negative mode
                        for i in range(1, 10):
                            ins_sql = "INSERT INTO measurement VALUES( " + "'" + str(i) + "', '" + str(comp_id[0]) \
                                    + "', '" + str(run_id) + "', '" + str(in_data[i + 11]) + "')"
                            try:
                                self.db.cursor.execute(ins_sql)
                                self.db.db.commit()
                            except Exception as e:
                                print(e)
                                print(i + 10)

    def get_run_date_time(self):
        return self.datafile[-12:]

    def insert_qc_run_data(self):

        run_date = self.get_run_date_time()

        # CONVERT('2014-02-28 08:14:57', DATETIME)
        sql = "INSERT INTO metab_qc_run VALUES(NULL,'" + self.datafile + "', CONVERT('" + str(run_date) + "', DATETIME))"
        try:
            # add a check here or remove run_id to avoid accidental duplicates
            self.db.cursor.execute(sql)
        except Exception as e:
            print(e)

        self.db.db.commit()
        return True


if __name__ == "__main__":

    user = "root"
    password = "raja2417"
    database = "metabqc"
    db = MetabDBSetUp(user, password, database)
    fs = FileSystem(sys.argv[1])
    raw_files = glob.glob(fs.in_dir + '/QC_Metabolomics_*.raw')
    print(len(raw_files))

    #LOOP Through all Raws in Folder

    for file in raw_files:
        id = file[35:-4]
        if id == 'QC_Metabolomics_180608162749_180612103257':
            print("Found")
            raw = ProcessRawFile(id, fs, db)
            raw.run()
        else:
            print("Skippy...!")


    # Handles duel dates eg. QC_Metabolomics_180608162749_180612103257
    # by using second date, uses full name in OutFiles folder