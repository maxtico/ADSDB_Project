from Landing.Persistent import Persistent
from Formatted.Formatted import Formatted
from Trusted.Owid.Owid_trusted import Owid_trusted
from Trusted.Worldometer.Worldometer_trusted import Worldometer_trusted

def main():
    source_folder = "/Users/maxtico/Documents/Master Data Science/ADSDB/ADSDB_Project/Landing/Temporal/"
    destination_folder = "/Users/maxtico/Documents/Master Data Science/ADSDB/ADSDB_Project/Landing/Persistent/"
    filename = "/Users/maxtico/Documents/Master Data Science/ADSDB/ADSDB_Project/Landing/Temporal/owid-covid-data_v1.csv"
    Persistent(filename,source_folder,destination_folder)
    Formatted()
    Owid_trusted()
    Worldometer_trusted()


if __name__ == "__main__":
    main()