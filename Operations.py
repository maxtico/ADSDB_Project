from DataManagementBackbone.Landing.Persistent import Persistent
from DataManagementBackbone.Formatted.Formatted import Formatted
from DataManagementBackbone.Trusted.Owid.Owid_trusted import Owid_trusted
from DataManagementBackbone.Trusted.Worldometer.Worldometer_trusted import Worldometer_trusted
from DataManagementBackbone.Exploitation.Exploitation import Exploitation
from DataAnalysisBackbone.AnalyticalSandbox.Analytical_sandbox import Sandbox
from DataAnalysisBackbone.FeatureGeneration.Feature_generation import Feature_generation
from DataAnalysisBackbone.ModelTraining.Model_training import Model
from DataAnalysisBackbone.Model_deployment.Model_deployment import Deployment
import warnings
import os

def main():
    rel_path = os.path.dirname(os.path.abspath(__name__))
    source_folder = rel_path+"/DataManagementBackbone/Landing/Temporal/"
    destination_folder = rel_path+"/DataManagementBackbone/Landing/Persistent/"
    filename = ["owid-covid-data_v1.csv","owid-covid-data_v2.csv","owid_preprocessed.csv","worldometer_v1.csv", "worldometer_v2.csv","GDP_v2.csv"]
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
    print('\n.....Executing Exploitation zone.....')
    exp_folder = rel_path+"/DataManagementBackbone/Exploitation/"
    Exploitation(destination_folder,exp_folder)
    print('\n.....Executing Analytical Sandbox.....')
    sand_dir = rel_path+"/DataAnalysisBackbone/AnalyticalSandbox/"
    Sandbox(exp_folder,sand_dir)
    print('\n.....Executing Feature Generation.....')
    feat_dir = rel_path+"/DataAnalysisBackbone/FeatureGeneration/"
    Feature_generation(sand_dir,feat_dir)
    print('\n.....Creating Models.....\n')
    model_path = rel_path+'/DataAnalysisBackbone/ModelTraining/Models/'
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        Model(feat_dir,model_path)
    print('\n.....Model deployment.....\n')
    Deployment(model_path)

if __name__ == "__main__":
    main()
