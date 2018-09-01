# create a database in MYSQL before running
# refer to Metabolomics QC ER Diagram fro design

import MySQLdb

class MetabDBSetUp:
    '''
	Database access module
	When run as a script sets up Metabolomics DB
	'''
    # CONSTRUCTOR connects to database as user with pword
    def __init__(self, user, pword, database):
        self.username = user
        self.password = pword
        self.database = database
        try:
            self.db = MySQLdb.connect("localhost", self.username, self.password, self.database)
            self.cursor = self.db.cursor()
        except:
            print("Database load error ")
    
    # DESCRIPTION FUNCTIONS details about the database
    def describe(self, tablename):
        try:
            sql = "DESCRIBE " + str(tablename)
            self.cursor.execute(sql)
            result =  self.cursor.fetchall()
            for line in result:
                print(line)
        except Exception as e:
            print(e)
            
    def show_tables(self):
        sql = "SHOW TABLES"
        
        try:
            self.cursor.execute(sql)
            result =  self.cursor.fetchall()
            return result
        except Exception as e:
            print(e)
    
    # CREATE TABLES
    def create_all_tables(self):
        self.create_table_metric_type()
        self.create_table_sample_component()
        self.create_table_qc_run()
        self.create_table_metric()
        self.create_table_measurement()
        
    def create_table_metric_type(self):
        sql = "CREATE TABLE IF NOT EXISTS metric_type ("              
		"type CHAR(1) NOT NULL,"                
		"type_description TEXT,"                
		"PRIMARY KEY(type)) "
                
        try:
            self.cursor.execute(sql)
        except Exception as e:
            print(e)
        
            
    def create_table_sample_component(self):
        sql = "CREATE TABLE IF NOT EXISTS sample_component ("                
		"component_id INT AUTO_INCREMENT NOT NULL,"                
		"component_name TINYTEXT,"                
		"component_description TEXT,"                
		"PRIMARY KEY(component_id)) "
                
        try:
            self.cursor.execute(sql)
        except Exception as e:
            print(e)
            
    def create_table_qc_run(self):
        # no id to avoid accidental duplicates ??
        sql = "CREATE TABLE IF NOT EXISTS metab_qc_run ("                
		"run_id INT AUTO_INCREMENT NOT NULL,"                
		"data_file TINYTEXT,"                
		"date_time DATETIME,"                
		"PRIMARY KEY(run_id)) "
                
        try:
            self.cursor.execute(sql)
        except Exception as e:
            print(e)
            
    def create_table_metric(self):
        sql = "CREATE TABLE IF NOT EXISTS metric ("                
		"metric_id INT AUTO_INCREMENT NOT NULL,"                
		"metric_name TINYTEXT,"                
		"metric_symbol TINYTEXT,"                
		"metric_description TEXT,"                
		"out_type CHAR(1) NOT NULL,"                
		"PRIMARY KEY(metric_id),"                
		"FOREIGN KEY (out_type) REFERENCES metric_type(type))"
                
        try:
            self.cursor.execute(sql)
        except Exception as e:
            print(e)
            
    def create_table_measurement(self):
        # watch FLOAT for value data type
        
        sql = "CREATE TABLE IF NOT EXISTS measurement ("                
		"metric_id INT NOT NULL,"                
		"component_id INT NOT NULL,"                
		"run_id INT NOT NULL,"                
		"value FLOAT(18, 3),"                
		"PRIMARY KEY(metric_id, component_id, run_id),"                
		"FOREIGN KEY (metric_id) REFERENCES metric(metric_id),"                
		"FOREIGN KEY (component_id) REFERENCES sample_component(component_id),"                
		"FOREIGN KEY (run_id) REFERENCES metab_qc_run(run_id))"
                
        try:
            self.cursor.execute(sql)
        except Exception as e:
            print(e)
    
    # DROP TABLES
    def drop_table(self, tablename):
        sql = "DROP TABLE " + tablename
        
        try:
            self.cursor.execute(sql)
        except Exception as e:
            print(e)
            
    def drop_all_tables(self):
        tables = ['measurement', 'metab_qc_run', 'sample_component', 'metric', 'metric_type']
        for table in tables:
            self.drop_table(table)
            
    
    # INSERTS
    def insert_all(self):
        self.insert_metric_type()
        self.insert_components() 
        self.insert_metrics()
        
    def insert_metric_type(self):
        sql1 = "INSERT INTO metric_type VALUES ('F', 'Flag descriptor with a value of 1 or 0 for True or False')"
        sql2 = "INSERT INTO metric_type VALUES ('M', 'A Metric value')"
        
        try:
            self.cursor.execute(sql1)
            self.cursor.execute(sql2)
            self.db.commit()
        except Exception as e:
            print(e)
            
    def insert_components(self):
        
        with open('components.txt', 'r') as infile:
            for line in infile:
                component = line.strip()
                sql = "INSERT INTO sample_component(component_id, component_name) VALUES (NULL, " + "'"
				+ str(component) + "'" + ")"  
                
                try:
                    self.cursor.execute(sql)
                    print("INSERT " + component)
                except Exception as e:
                    print(e)
                
                self.db.commit()
        
    def insert_metrics(self):
        with open('metrics.txt', 'r') as infile:
            for line in infile:
                in_data = line.strip().split(':')
                if in_data[0] == 'F':
                    sql = "INSERT INTO metric VALUES (NULL," + "'" + in_data[1].strip() + "'," + "'"
					+ in_data[2].strip() + "'," + "'" + in_data[3].strip() + "'," + "'" + in_data[0].strip() + "')"
                    try:
                        self.cursor.execute(sql)
                        print("INSERT ",in_data)
                    except Exception as e:
                        print(e)
                elif in_data[0] == 'M':
                    sql = "INSERT INTO metric(metric_id,metric_name,out_type) VALUES (NULL,"
					+  "'" + in_data[1].strip() + "','" + in_data[0].strip() + "')"
                    try:
                        self.cursor.execute(sql)
                        print("INSERT ",in_data)
                    except Exception as e:
                        print(e)
                        
                self.db.commit()
            
    # SELECT
    def select_and_display_all(self, tablename):
        sql = "SELECT * FROM " + tablename
        
        try:
            self.cursor.execute(sql)
            result =  self.cursor.fetchall()
            for line in result:
                print(line)
        except Exception as e:
            print(e)
            
    

if __name__ == "__main__":
    user = "root"
    password = "raja2417"
    database = "metabqc"
	db = MetabDBSetUp(user, password, database)
    #db.drop_all_tables()
    db.create_all_tables()
    db.insert_all()
    





