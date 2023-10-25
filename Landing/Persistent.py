import shutil
import os
import datetime

def Persistent(filename, source_folder, destination_folder):
    source_path = os.path.join(source_folder, filename)
    destination_path = os.path.join(destination_folder, filename)

    if not os.path.isfile(source_path):
        print(f"The file '{filename}' does not exist in the source folder.")
        return
    user_choice = input(f"Do you want to move '{filename}' to the '{destination_folder}' folder? (0/1): ")
    
    if user_choice == '1':
        # Get the current date in the desired format (e.g., YYYY-MM-DD)
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")

        # Split the filename into its name and extension
        name, extension = os.path.splitext(filename)

        # Construct the new filename with the date
        new_filename = f"{name}_{current_date}{extension}"

        # Build the full destination path with the new filename
        destination_path_with_date = os.path.join(destination_folder, new_filename)

        # Move the file to the destination folder with the new filename
        shutil.move(source_path, destination_path_with_date)
        print(f"'{filename}' has been moved to '{destination_folder}' as '{new_filename}'.")
    else:
        print("Operation canceled by the user.")


# Example usage:
source_folder = "/Users/maxtico/Documents/Master Data Science/ADSDB/ADSDB_Project/"
destination_folder = "./"
filename = "owid-covid-data_v1.csv"
Persistent(filename, source_folder, destination_folder)