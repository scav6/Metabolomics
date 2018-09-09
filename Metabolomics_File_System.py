import os

class FileSystem:
    # a struct for the file system directories
    def __init__(self, file_directory):
        self.main_dir = os.getcwd()
        self.in_dir = file_directory

        self.sw_dir = self.main_dir + "\Software"
        self.xml_template = "SSQC_xml_template_v2.xml"
        self.neg_db = "Negative_QC_db_v1.csv"
        self.pos_db = "Positive_QC_db_v1.csv"

        if not os.path.isdir(self.main_dir + "\OutFiles"):
            os.makedirs(self.main_dir + "\OutFiles")

        self.out_dir = self.main_dir + "\OutFiles"