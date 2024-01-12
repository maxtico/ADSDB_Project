from DataManagementBackbone.Landing.Persistent import Persistent
from DataManagementBackbone.Formatted.Formatted import Formatted
from DataManagementBackbone.Trusted.Owid.Owid_trusted import Owid_trusted
from DataManagementBackbone.Trusted.Worldometer.Worldometer_trusted import Worldometer_trusted
from DataManagementBackbone.Exploitation.Exploitation import Exploitation
from DataAnalysisBackbone.AnalyticalSandbox.Analytical_sandbox import Sandbox
from DataAnalysisBackbone.FeatureGeneration.Feature_generation import Feature_generation
from DataAnalysisBackbone.ModelTraining.Model_training import Model
from DataAnalysisBackbone.Model_deployment.Model_deployment import Deployment
from AdvancedTopic.Landing.Persistent_AT import Persistent_AT
from AdvancedTopic.Formatted.Formatted_AT import Formatted_AT
from AdvancedTopic.Trusted.Trusted_AT import Trusted_AT
from AdvancedTopic.Exploitation.Exploitation_AT import Exploitation_AT
from AdvancedTopic.AnalyticalSandbox.AnalyticalSandbox_AT import Sandbox_AT
from AdvancedTopic.FeatureGeneration.FeatureGeneration_AT import Feature_generation_AT
from AdvancedTopic.ModelTraining.ModelTraining_AT import Model_training_AT
from AdvancedTopic.ModelDeployment.ModelDeployment_AT import Deployment_AT
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

    print('\n')
    print('\n.....ADVANCED TOPIC.....\n')
    print('\n')
    filename_AT = ['WBG_Access to electricity_1705049804.csv','WBG_Birth certificates_1705049740.csv','WBG_GDP growth_1705049736.csv','IBAN_CountryCodes_1705050301.csv','WBG_C02 production per capita_1705049761.csv','OEBC_GDP2020_1705050301.csv','WBG_Corruption estimation_1705049782.csv']
    print('\n.....Executing Landing zone.....')
    source_folder_AT = rel_path+"/AdvancedTopic/Landing/Temporal/"
    destination_folder_AT = rel_path+"/AdvancedTopic/Landing/Persistent/"
    for file in filename_AT: 
        Persistent_AT(file,source_folder_AT,destination_folder_AT)
    print('\n.....Executing Formatted zone.....')
    form_dir = rel_path+'/AdvancedTopic/Formatted'
    Formatted_AT(destination_folder_AT,form_dir)
    print('\n.....Executing Trusted zone.....')
    trust_dir=rel_path+'/AdvancedTopic/Trusted'
    Trusted_AT(form_dir,trust_dir)
    print('\n.....Executing Exploitation zone.....')
    exp_dir = rel_path+'/AdvancedTopic/Exploitation'
    Exploitation_AT(trust_dir,destination_folder,exp_dir)
    print('\n.....Executing Analytical Sandbox.....')
    sand_dir=rel_path+'/AdvancedTopic/AnalyticalSandbox'
    Sandbox_AT(exp_dir,sand_dir)
    print('\n.....Executing Feature Generation.....')
    feat_dir=rel_path+'/AdvancedTopic/FeatureGeneration'
    Feature_generation_AT(sand_dir,feat_dir)
    print('\n.....Creating Models.....\n')
    model_dir = rel_path+"/AdvancedTopic/ModelTraining/Models/"
    Model_training_AT(feat_dir,model_dir)
    print('\n.....Model deployment.....\n')
    Deployment_AT(model_dir)

if __name__ == "__main__":
    main()
