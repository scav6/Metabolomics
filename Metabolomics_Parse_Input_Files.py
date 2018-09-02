from Metabolomics_Database_SetUp import MetabDBSetUp


class MetabParseFile:
	
	'''
	This module will parse the data from one Metabolomics text file
	and insert it into the database.
	'''
    
    def __init__(self, filename, db):
        self.filename = filename
        self.db = db
        run_data = self.get_run_data()
        self.data_file = run_data[0]
        self.run_date = run_data[1]
        self.components = {}
        self.components_pass = {}
        self.components_fail = {}
        self.metric_names = []
        
    def enter_data(self):
        self.convert_to_mysql_datetime_string()
        self.insert_qc_run_data()
        self.run_id = self.get_run_id()
        self.get_components()
        self.get_metric_names()
        self.get_measurements_values()
        self.insert_measurement_values()
        self.insert_measurement_flags()
        
    def get_run_data(self):    
        with open(self.filename, 'r') as infile:
            for line in infile:
                in_data = line.strip().split(';')

                if in_data[0].strip() == 'Data File:':
                    data_file = in_data[1].strip()
                elif in_data[0].strip() == 'Acquisition Date:':
                    run_date = in_data[1].strip()

            return data_file, run_date
        
    def get_symbols(self, symbols):
        symbols_list = symbols.strip().split(" ")
        new_list = symbols_list[1:]
        return new_list
    
    def convert_to_mysql_datetime_string(self):
        # https://dev.mysql.com/doc/refman/8.0/en/date-and-time-literals.html
        # YY-MM-DD HH:MM:SS
        # 2014-02-28 08:14:57
        # YYMMDDHHMMSS
        # check again with batch input
        meridian = self.run_date[-2:]
        month = self.run_date[:2]
        day = self.run_date[3:-15]
        year = self.run_date[6:-12]
        hh = self.run_date[9:-9]
        mm = self.run_date[12:-6]
        ss = self.run_date[15:-3]

        if meridian.upper() == 'PM' and hh != '12':
            hh = str(int(hh) + 12)

        self.run_date = year + month + day + hh + mm + ss


    def insert_qc_run_data(self):
        # CONVERT('2014-02-28 08:14:57', DATETIME)
        sql = "INSERT INTO metab_qc_run VALUES(NULL,'" + self.data_file + "', CONVERT('" + self.run_date + "', DATETIME))"
        try:
            # add a check here or remove run_id to avoid accidental duplicates
            self.db.cursor.execute(sql)
        except Exception as e:
            print(e)

        self.db.db.commit()
        
    def get_run_id(self):
        # only checking data file not date
        sql = "SELECT run_id FROM metab_qc_run WHERE data_file = '" + self.data_file + "'"
        try:
            self.db.cursor.execute(sql)
            run_id =  self.db.cursor.fetchall()
        except Exception as e:
            print(e)
            
        return run_id[0]
    
    def get_components(self):
        # creates a component dict each with a list of metrics values
        with open('components.txt', 'r') as infile:
            for line in infile:
                if line.strip() == 'Glucose':
                    continue
                self.components[line.strip()] = []
                self.components_pass[line.strip()] = []
                self.components_fail[line.strip()] = []
                
    def get_metric_names(self):
        with open('metrics.txt', 'r') as infile:
            for line in infile:
                new_line = line.strip().split(':')
                if new_line[0] == 'M':
                    self.metric_names.append(new_line[1])
                
        
    def get_measurements_values(self):
        # list order : ERT, RT, width, area, area_p,height,signal_noise
        with open(self.filename, 'r') as infile:
            for line in infile:
                in_data = line.strip().split(';')

                component = in_data[0].strip()
                if component in self.components:
                    for i in range(1,8):
                        self.components[component].append(in_data[i])
                        
                    try:
                        self.components_pass[component] = self.get_symbols(in_data[8])
                    except IndexError:
                        print("NO PASS DATA")
                        
                    try:
                        self.components_fail[component] = self.get_symbols(in_data[9])
                    except IndexError:
                        print("NO FAIL DATA")


    def insert_measurement_values(self):
        # loops through each component
        for key, value in self.components.items():
                
            sql = "SELECT component_id FROM sample_component WHERE component_name = '" + key + "'"
            try:
                self.db.cursor.execute(sql)
                comp_id =  self.db.cursor.fetchall()
                
            except Exception as e:
                print(e)
                
            # loops through each metric name and value and inserts
            for i in range(len(self.metric_names)):
                get_sql = "SELECT metric_id FROM metric WHERE metric_name = '" + self.metric_names[i] + "'"
                try:
                    self.db.cursor.execute(get_sql)
                    metric_id =  self.db.cursor.fetchall()
                except Exception as e:
                    print(e)
                    
                ins_sql = "INSERT INTO measurement VALUES('" + str(metric_id[0][0]) + "','" + str(comp_id[0][0]) + "','"                + str(self.run_id[0]) + "','" + self.components[key][i].strip() + "')"
                try:
                    self.db.cursor.execute(ins_sql)
                except Exception as e:
                    print(e)
                    
                self.db.db.commit()
            
            # store component_id for flag insert
            self.components[key] = [comp_id[0][0]]
                
    def insert_measurement_flags(self):
		# set up: flag arg for pass/fail
		
        # passed metrics
        for component, flags in self.components_pass.items():
            for symbol in flags:
                sql = "SELECT metric_id FROM metric WHERE metric_symbol = '" + symbol.strip() + "'"
                try:
                    self.db.cursor.execute(sql)
                    metric_id =  self.db.cursor.fetchall()
                except Exception as e:
                    print(e)
                    
                ins_sql = "INSERT INTO measurement VALUES('" + str(metric_id[0][0]) + "','" + str(self.components[component][0])                         + "','" + str(self.run_id[0]) + "','1')" 
                try:
                    self.db.cursor.execute(ins_sql)
                except Exception as e:
                    print(e)
                    
            self.db.db.commit()
        
        # failed metrics
        for component, flags in self.components_fail.items():
            for symbol in flags:
                sql = "SELECT metric_id FROM metric WHERE metric_symbol = '" + symbol.strip() + "'"
                try:
                    self.db.cursor.execute(sql)
                    metric_id =  self.db.cursor.fetchall()
                except Exception as e:
                    print(e)
                    
                ins_sql = "INSERT INTO measurement VALUES('" + str(metric_id[0][0]) + "','" + str(self.components[component][0])                         + "','" + str(self.run_id[0]) + "','0')" 
                try:
                    self.db.cursor.execute(ins_sql)
                except Exception as e:
                    print(e)
                    
            self.db.db.commit()
            

   
if __name__ == "__main__":
	user = "root"
	password = "password"
	database = "metabqc"

	db = MetabDBSetUp(user, password, database)
	new_input = MetabParseFile('./Test Files/QC_Metabolomics_180410132208_Metabolomics_QC_ShortReport_v2.0.txt', db)
	new_input.enter_data()






