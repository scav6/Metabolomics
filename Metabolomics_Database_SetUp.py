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
        except Exception as e:
            print(e)
            print("Database load error ")

    def set_up(self):
        self.drop_all_tables()
        self.create_all_tables()
        self.insert_all()
        self.select_and_display_all('metric')
        self.select_and_display_all('sample_component')

    
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
        self.create_table_sample_component()
        self.create_table_qc_run()
        self.create_table_metric()
        self.create_table_measurement()

    def create_table_sample_component(self):
        sql = "CREATE TABLE IF NOT EXISTS sample_component (" \
		"component_id INT AUTO_INCREMENT NOT NULL," \
		"component_name TINYTEXT NOT NULL," \
		"component_description TEXT," \
        "component_mode CHAR(1) NOT NULL," \
        "exp_mass_charge FLOAT(11, 7) NOT NULL," \
        "exp_rt FLOAT(5, 2) NOT NULL," \
        "PRIMARY KEY(component_id))"
                
        try:
            self.cursor.execute(sql)
        except Exception as e:
            print(e)
            
    def create_table_qc_run(self):
        # no id to avoid accidental duplicates ??
        sql = "CREATE TABLE IF NOT EXISTS metab_qc_run (" \
		"run_id INT AUTO_INCREMENT NOT NULL," \
		"data_file TINYTEXT," \
		"date_time DATETIME," \
		"PRIMARY KEY(run_id)) "
                
        try:
            self.cursor.execute(sql)
        except Exception as e:
            print(e)
            
    def create_table_metric(self):
        sql = "CREATE TABLE IF NOT EXISTS metric (" \
		"metric_id INT AUTO_INCREMENT NOT NULL," \
		"metric_name TINYTEXT," \
		"metric_description TEXT,"  \
		"PRIMARY KEY(metric_id))"
                
        try:
            self.cursor.execute(sql)
        except Exception as e:
            print(e)
            
    def create_table_measurement(self):
        # watch FLOAT for value for range, powers in area metric
        
        sql = "CREATE TABLE IF NOT EXISTS measurement (" \
		"metric_id INT NOT NULL," \
		"component_id INT NOT NULL," \
		"run_id INT NOT NULL," \
		"value FLOAT(36, 18)," \
		"PRIMARY KEY(metric_id, component_id, run_id)," \
		"FOREIGN KEY (metric_id) REFERENCES metric(metric_id)," \
		"FOREIGN KEY (component_id) REFERENCES sample_component(component_id)," \
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
        tables = ['measurement', 'metab_qc_run', 'sample_component', 'metric']
        for table in tables:
            self.drop_table(table)

    # INSERTS
    def insert_all(self):
        self.insert_components() 
        self.insert_metrics()

    def insert_components(self):
        
        with open('Negative_QC_db_v1.csv', 'r') as infile:
            for line in infile:
                component = line.strip().split("|")
                if component[0] != 'mz':
                    sql = "INSERT INTO sample_component(component_id, component_name, component_mode, exp_mass_charge, exp_rt) " \
                          "VALUES (NULL, " + "'" + component[2].strip() + "'," + "'N'," + "'" + component[0].strip() + "'," + "'" + component[1].strip() + "'" + ")"

                    try:
                        self.cursor.execute(sql)
                    except Exception as e:
                        print(e)

            self.db.commit()

        with open('Positive_QC_db_v1.csv', 'r') as infile:
            for line in infile:
                component = line.strip().split("|")
                if component[0] != 'mz':
                    sql = "INSERT INTO sample_component(component_id, component_name, component_mode, exp_mass_charge, exp_rt) " \
                          "VALUES (NULL, " + "'" + component[2].strip() + "'," + "'P'," + "'" + component[0].strip() + "'," + "'" + component[1].strip() + "'" + ")"

                    try:
                        self.cursor.execute(sql)
                    except Exception as e:
                        print(e)

            self.db.commit()

    def insert_metrics(self):
        with open('mzmine_metrics.txt', 'r') as infile:
            for line in infile:
                in_data = line.strip().split(':')
                sql = "INSERT INTO metric VALUES (NULL," + "'" + in_data[0].strip() + "'," + "'" \
                + in_data[1].strip() + "'" + ")"

                try:
                    self.cursor.execute(sql)
                except Exception as e:
                    print(e)
            self.db.commit()
            
    # SELECT
    def select_and_display_all(self, tablename):
        print(tablename.upper())
        sql = "SELECT * FROM " + tablename
        
        try:
            self.cursor.execute(sql)
            result =  self.cursor.fetchall()
            for line in result:
                print(line)
            print("\n")
        except Exception as e:
            print(e)

    def get_run_id(self, datafile):
        sql = "SELECT run_id FROM metab_qc_run WHERE data_file = " + "'" + datafile + "'"
        try:
            self.cursor.execute(sql)
            run_id = self.cursor.fetchone()
            return run_id[0]
        except Exception as e:
            print(e)
            return False

    def get_metric_id(self, name):
        sql = "SELECT metric_id FROM metric WHERE metric_name = " + "'" + name + "'"
        try:
            self.cursor.execute(sql)
            metric_id = self.cursor.fetchone()
            return metric_id[0]
        except Exception as e:
            print(e)
            return False


if __name__ == "__main__":
    user = "root"
    password = "password"
    database = "metabqc"
    db = MetabDBSetUp(user, password, database)
    db.set_up()




