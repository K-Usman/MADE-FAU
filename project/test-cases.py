from ETL import download_bayernData,download_schleswigData,transform_bayernData,transform_schleswigData,load_bayernData,load_schleswigData,connect
import unittest
import os

class TestETL(unittest.TestCase):

    def test_downloadBayernData(self):
        #Test downloading of different accessible csv files from google drive
        test_cases=["https://drive.google.com/file/d/1DI-zgA0XKZ1KBN7trQSqx8RZVT3rjE3f/view?usp=sharing","https://drive.google.com/file/d/10y330xLTuAZ6ZA-EK-p5ssOkPUgo9L_2/view?usp=sharing","https://drive.google.com/file/d/1tgwLOlKcrfnHCOvmz7aEwyMMPX5EJOY1/view?usp=sharing"]
        for test in test_cases:
            csvFile=download_bayernData(test)
            self.assertTrue(str(type(csvFile)) == "<class 'pandas.core.frame.DataFrame'>")

    def test_downloadSchleswigData(self):
        #Test downloading different xlsx file from url
        test_cases=["https://www.statistik-nord.de/fileadmin/Dokumente/Statistische_Berichte/verkehr_umwelt_und_energie/H_I_1_m_S/H_I_1-m2309_SH.xlsx","https://api.opendata.onisep.fr/downloads/605344579a7d7/605344579a7d7.xlsx","https://www.statistik-nord.de/fileadmin/Dokumente/Statistische_Berichte/verkehr_umwelt_und_energie/H_II_1_S/H_II_1_hj_1_23_SH.xlsx"]
        for test in test_cases:
            csvFile=download_schleswigData(test)
            self.assertTrue(str(type(csvFile)) == "<class 'pandas.core.frame.DataFrame'>")
    
    def test_loadData(self):
        #Test if the sql lite is created or not
        url='../data/test.sqlite'
        connect(url)
        self.assertTrue(os.path.exists(url))

if __name__ == "__main__":
    unittest.main()
