from Landing.Persistent import Persistent
from Formatted.Formatted import Formatted
from Trusted.Owid.Owid_trusted import Owid_trusted
from Trusted.Worldometer.Worldometer_trusted import Worldometer_trusted
from Exploitation.Exploitation import Exploitation
import warnings
import os

def main():
    rel_path = os.path.dirname(os.path.abspath(__name__))
    source_folder = rel_path+"/Landing/Temporal/"
    destination_folder = rel_path+"/Landing/Persistent/"
    filename = ["owid-covid-data_v1.csv","owid-covid-data_v2.csv","worldometer_v1.csv", "worldometer_v2.csv"]
    print('\n.....Executing Landing zone.....')
    for file in filename:
        Persistent(file,source_folder,destination_folder)
    print('\n.....Executing Formatted zone.....')
    Formatted(destination_folder)
    print('\n.....Executing Trusted zone.....')
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        print('\n.....Performing quality analysis & treating missing data for Owid dataset.....')
        Owid_trusted(destination_folder)
        print('\n.....Performing quality analysis & treating missing data for Worldometer dataset.....')
        Worldometer_trusted(destination_folder)
    print('\n.....Executing Exploitation zone.....\n')
    Exploitation(destination_folder)

if __name__ == "__main__":
    main()
